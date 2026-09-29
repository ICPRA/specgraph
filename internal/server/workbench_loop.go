// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// Workbench loop handlers (plan v2 §3): PrepareRun -> bind thread -> delivery
// -> acceptance -> completion (gated in managed projects). Mounted as plain
// HTTP on the same mux as the Connect services, using the same identity resolver
// and authorizer. Browser writes additionally require a same-origin request.
//
// Extension note (fork): upstream has no equivalent. RecordCompletion still
// enforces the managed-project acceptance gate. The current Cedar policies gate
// roles, not project membership. Project selection uses X-Specgraph-Project,
// defaulting to the deployment project.

const defaultWorkbenchProject = "specgraph"

type workbenchLoop struct {
	root *postgres.Store
}

func writeLoopJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write workbench response failed", slog.Any("error", err))
	}
}

// resolveWorkbenchStore scopes the root store to an existing request project
// (X-Specgraph-Project header, default specgraph).
func resolveWorkbenchStore(r *http.Request, root *postgres.Store) (*postgres.Store, error) {
	project := r.Header.Get("X-Specgraph-Project")
	if project == "" {
		project = defaultWorkbenchProject
	}
	result, callErr := root.ScopedExisting(r.Context(), project)
	if callErr != nil {
		return result, fmt.Errorf("workbench loop: %w", callErr)
	}
	return result, nil
}

func (l *workbenchLoop) mapError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, storage.ErrDependencyCycle):
		writeLoopJSON(w, http.StatusConflict, map[string]string{"code": "failed_precondition", "error": err.Error()})
	case errors.Is(err, storage.ErrInvalidTestReport):
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": err.Error()})
	case errors.Is(err, storage.ErrTestReportForbidden):
		writeLoopJSON(w, http.StatusForbidden, map[string]string{"code": "forbidden", "error": err.Error()})
	case errors.Is(err, storage.ErrImplementationTestsRequired):
		writeLoopJSON(w, http.StatusConflict, map[string]string{"code": "failed_precondition", "error": err.Error()})
	case errors.Is(err, storage.ErrReviewForbidden), errors.Is(err, storage.ErrReviewSelfReview):
		writeLoopJSON(w, http.StatusForbidden, map[string]string{"code": "permission_denied", "error": err.Error()})
	case errors.Is(err, storage.ErrReviewHumanHold), errors.Is(err, storage.ErrReviewAlreadyDecided):
		writeLoopJSON(w, http.StatusPreconditionRequired, map[string]string{"code": "failed_precondition", "error": err.Error()})
	case errors.Is(err, storage.ErrInvalidReview):
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": err.Error()})
	case errors.Is(err, storage.ErrReviewRequestNotFound):
		writeLoopJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "error": err.Error()})
	case errors.Is(err, storage.ErrInvalidSubdivisionRequest), errors.Is(err, storage.ErrInvalidRunPreparation):
		writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": err.Error()})
	case errors.Is(err, storage.ErrSpecTerminal), errors.Is(err, storage.ErrSummaryNotExecutable), errors.Is(err, storage.ErrDispatchResponsibilityHeld), errors.Is(err, storage.ErrDispatchResolved), errors.Is(err, storage.ErrPreparationCancelled):
		writeLoopJSON(w, http.StatusPreconditionRequired, map[string]string{"code": "failed_precondition", "error": err.Error()})
	case errors.Is(err, storage.ErrManagedCompletionRequiresAcceptance), errors.Is(err, storage.ErrCompletionRequiresRequirementReview), errors.Is(err, storage.ErrSpecNotApproved), errors.Is(err, storage.ErrDependenciesNotReady), errors.Is(err, storage.ErrExecutionDependenciesChanged):
		writeLoopJSON(w, http.StatusPreconditionRequired, map[string]string{"code": "failed_precondition", "error": err.Error()})
	case errors.Is(err, storage.ErrSpecNotFound), errors.Is(err, storage.ErrDecisionNotFound), errors.Is(err, storage.ErrRunBindingNotFound), errors.Is(err, storage.ErrProjectNotFound), errors.Is(err, storage.ErrDeliveryNotFound), errors.Is(err, storage.ErrSubdivisionNotFound):
		writeLoopJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "error": err.Error()})
	case errors.Is(err, storage.ErrSpecAlreadyExists), errors.Is(err, storage.ErrSpecAlreadyClaimed), errors.Is(err, storage.ErrNotClaimOwner), errors.Is(err, storage.ErrAgentNotClaimOwner), errors.Is(err, storage.ErrRunBindingConflict), errors.Is(err, storage.ErrConcurrentModification), errors.Is(err, storage.ErrManualCompletionConflict):
		writeLoopJSON(w, http.StatusConflict, map[string]string{"code": "conflict", "error": err.Error()})
	default:
		slog.LogAttrs(context.Background(), slog.LevelError, "workbench loop operation failed", slog.String("operation", op), slog.Any("error", err))
		writeLoopJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "error": op + " failed"})
	}
}

// RegisterWorkbenchLoop mounts the closed-loop endpoints under /loop/.
func RegisterWorkbenchLoop(mux *http.ServeMux, store *postgres.Store, resolver auth.Resolver, authorizer auth.Authorizer) {
	l := &workbenchLoop{root: store}
	registerWorkbenchReads(mux, store)
	registerWorkbenchMail(mux, store, resolver, authorizer)
	registerMailInspection(mux, store, resolver, authorizer)
	register := func(pattern, procedure string, handler http.HandlerFunc) {
		mux.Handle(pattern, workbenchWriteAuth(resolver, authorizer, procedure, handler))
	}
	register("POST /loop/specs/{slug}/manual-complete", auth.WorkbenchManualCompleteProcedure, l.manualComplete)
	register("POST /loop/specs/{slug}/assign-review", auth.WorkbenchAcceptDeliveryProcedure, l.assignReview)
	register("POST /loop/specs/{slug}/review-source", auth.WorkbenchAcceptDeliveryProcedure, l.reviewSource)
	register("GET /loop/specs/{slug}/review-status", specgraphv1connect.SpecServiceGetSpecProcedure, l.readReviewStatus)
	register("GET /loop/runs/{id}/context", auth.WorkbenchReadRunContextProcedure, l.readRunContext)
	register("GET /loop/runs/{id}/dispatch", auth.WorkbenchDispatchProcedure, l.readDispatch)
	register("POST /loop/runs/{id}/dispatch/authorize", auth.WorkbenchDispatchProcedure, l.authorizeDispatch)
	register("POST /loop/runs/{id}/dispatch/resolve", auth.WorkbenchDispatchProcedure, l.resolveDispatch)
	register("POST /loop/runs/{id}/dispatch/cancel-preparation", auth.WorkbenchDispatchProcedure, l.cancelPreparation)
	register("POST /loop/preparations/abort", auth.WorkbenchDispatchProcedure, l.abortPreparation)
	register("GET /loop/specs/{slug}/dependencies", auth.WorkbenchEditDependencyProcedure, l.readDependencyEditState)
	register("POST /loop/specs/{slug}/dependencies", auth.WorkbenchEditDependencyProcedure, l.addDependency)
	register("POST /loop/specs/{slug}/dependencies/remove", auth.WorkbenchEditDependencyProcedure, l.removeDependency)
	register("GET /loop/subdivisions/{id}", auth.WorkbenchSubdivisionProcedure, l.readSubdivision)
	register("GET /loop/specs/{slug}/change-preview", auth.WorkbenchChangePreviewProcedure, l.readRequirementChangePreview)
	register("GET /loop/specs/{slug}/subdivisions", auth.WorkbenchSubdivisionProcedure, l.listSpecSubdivisions)
	register("POST /loop/specs/{slug}/subdivide", auth.WorkbenchSubdivisionProcedure, l.subdivideSpec)

	resolve := func(w http.ResponseWriter, r *http.Request) (*postgres.Store, bool) {
		ps, err := resolveWorkbenchStore(r, l.root)
		if err != nil {
			l.mapError(w, "scope store", err)
			return nil, false
		}
		return ps, true
	}

	register("POST /loop/runs", auth.WorkbenchPrepareRunProcedure, func(w http.ResponseWriter, r *http.Request) {
		store, ok := resolve(w, r)
		if !ok {
			return
		}
		var req struct {
			TaskSlug       string          `json:"task_slug"`
			Workspace      string          `json:"workspace"`
			IdempotencyKey string          `json:"idempotency_key"`
			DispatchTarget json.RawMessage `json:"dispatch_target"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil || req.TaskSlug == "" || decoder.Decode(new(any)) != io.EOF || (req.DispatchTarget != nil && req.IdempotencyKey == "") {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"error": "task_slug required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		var bindingID string
		var replayed bool
		var err error
		if req.IdempotencyKey != "" {
			identity, _ := auth.IdentityFromContext(ctx)
			bindingID, replayed, err = store.PrepareRunForOperator(ctx, req.TaskSlug, req.Workspace, identity.UserID, req.IdempotencyKey, req.DispatchTarget)
		} else {
			bindingID, err = store.PrepareRun(ctx, req.TaskSlug, req.Workspace)
		}
		if errors.Is(err, storage.ErrInvalidRunPreparation) {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": err.Error()})
			return
		}
		if err != nil {
			l.mapError(w, "prepare run", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, map[string]any{"binding_id": bindingID, "generation": 1, "replayed": replayed})
	})

	register("POST /loop/runs/{id}/bind", auth.WorkbenchBindRunThreadProcedure, func(w http.ResponseWriter, r *http.Request) {
		store, ok := resolve(w, r)
		if !ok {
			return
		}
		var req struct {
			ThreadRef     string          `json:"thread_ref"`
			EnvironmentID json.RawMessage `json:"environment_id"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil || req.ThreadRef == "" || len(req.ThreadRef) > 256 || decoder.Decode(new(any)) != io.EOF {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": "valid thread_ref required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		var err error
		if len(req.EnvironmentID) != 0 {
			var environmentID string
			if json.Unmarshal(req.EnvironmentID, &environmentID) != nil {
				err = storage.ErrInvalidRunBinding
			} else {
				err = store.BindRunThreadInEnvironment(ctx, r.PathValue("id"), environmentID, req.ThreadRef)
			}
		} else {
			// Omitting environment_id retains the legacy environment-less API.
			err = store.BindRunThread(ctx, r.PathValue("id"), req.ThreadRef)
		}
		if errors.Is(err, storage.ErrInvalidRunBinding) {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_argument", "error": err.Error()})
			return
		}
		if err != nil {
			l.mapError(w, "bind run thread", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, map[string]any{"bound": true})
	})

	register("POST /loop/deliveries", auth.WorkbenchSubmitDeliveryProcedure, func(w http.ResponseWriter, r *http.Request) {
		store, ok := resolve(w, r)
		if !ok {
			return
		}
		var req struct {
			RunBindingID      string          `json:"run_binding_id"`
			Snapshot          json.RawMessage `json:"snapshot"`
			ExpectedAttemptID string          `json:"expectedAttemptId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RunBindingID == "" {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"error": "run_binding_id required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		identity, _ := auth.IdentityFromContext(ctx)
		deliveryID, err := store.CreateDelivery(ctx, req.RunBindingID, req.Snapshot, identity.UserID, req.ExpectedAttemptID)
		if err != nil {
			l.mapError(w, "create delivery", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, map[string]any{"delivery_id": deliveryID})
	})

	register("POST /loop/deliveries/{id}/accept", auth.WorkbenchAcceptDeliveryProcedure, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Fingerprint string          `json:"requirements_fingerprint"`
			Verdict     string          `json:"verdict"`
			Conditions  json.RawMessage `json:"conditions"`
		}
		if err := decodeMailRequest(http.MaxBytesReader(w, r.Body, 32<<10), &req); err != nil {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
			return
		}
		if req.Verdict == "" {
			writeLoopJSON(w, http.StatusBadRequest, map[string]string{"error": "verdict required"})
			return
		}
		store, ok := resolve(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		var conditions struct {
			Review string `json:"review"`
		}
		if json.Unmarshal(req.Conditions, &conditions) != nil {
			l.mapError(w, "accept delivery", storage.ErrInvalidReview)
			return
		}
		result, err := store.ReviewDelivery(ctx, r.PathValue("id"), req.Fingerprint, req.Verdict, conditions.Review)
		if err != nil {
			l.mapError(w, "accept delivery", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, result)
	})

	register("POST /loop/runs/{id}/complete", auth.WorkbenchCompleteRunProcedure, func(w http.ResponseWriter, r *http.Request) {
		store, ok := resolve(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		task, err := store.RunBindingTask(ctx, r.PathValue("id"))
		if err != nil {
			l.mapError(w, "complete run", err)
			return
		}
		if err := store.RecordCompletion(ctx, task, r.PathValue("id")); err != nil {
			l.mapError(w, "complete run", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, map[string]any{"completed": task})
	})
}
