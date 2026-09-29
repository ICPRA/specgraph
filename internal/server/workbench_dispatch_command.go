// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteAgentRunCompletion derives the completing run from verified host scope.
func ExecuteAgentRunCompletion(ctx context.Context, root *postgres.Store, project string, body io.Reader) (storage.RunSelfCompletion, error) {
	var req struct {
		Scope storage.MailScope `json:"scope"`
	}
	if decodeMailRequest(body, &req) != nil {
		return storage.RunSelfCompletion{}, connect.NewError(connect.CodeInvalidArgument, errors.New("self completion accepts only host scope"))
	}
	if project == "" {
		var err error
		project, err = root.ProjectForLocalMailScope(ctx, req.Scope)
		if err != nil {
			return storage.RunSelfCompletion{}, fmt.Errorf("workbench dispatch command: %w", err)
		}
	}
	if project == "_server" {
		return storage.RunSelfCompletion{}, storage.ErrProjectNotFound
	}
	scoped, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return storage.RunSelfCompletion{}, fmt.Errorf("workbench dispatch command: %w", err)
	}
	result, callErr := scoped.CompleteOwnRun(ctx, req.Scope)
	if callErr != nil {
		return result, fmt.Errorf("workbench dispatch command: %w", callErr)
	}
	return result, nil
}

// Local forms use the existing dispatch storage owners and wire field names.
func executeDispatchCommand(ctx context.Context, store *postgres.Store, operation, runID, actor string, body json.RawMessage) (json.RawMessage, error) {
	decode := func(value any) error {
		d := json.NewDecoder(bytes.NewReader(body))
		d.DisallowUnknownFields()
		if len(body) > 32<<10 || d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("invalid dispatch request"))
		}
		return nil
	}
	encode := func(value any, err error) (json.RawMessage, error) {
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	switch operation {
	case "complete-run":
		var req struct{}
		if err := decode(&req); err != nil {
			return nil, err
		}
		slug, err := store.RunBindingTask(ctx, runID)
		if err != nil {
			return nil, fmt.Errorf("workbench dispatch command: %w", err)
		}
		return encode(map[string]string{"completed": slug}, store.RecordCompletion(ctx, slug, runID))
	case "prepare-run":
		var req struct {
			TaskSlug  string          `json:"task_slug"`
			Workspace string          `json:"workspace"`
			Key       string          `json:"idempotency_key"`
			Target    json.RawMessage `json:"dispatch_target"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		if req.TaskSlug == "" || (req.Target != nil && req.Key == "") {
			return nil, storage.ErrInvalidRunPreparation
		}
		var id string
		var replayed bool
		var err error
		if req.Key == "" {
			id, err = store.PrepareRun(ctx, req.TaskSlug, req.Workspace)
		} else {
			id, replayed, err = store.PrepareRunForOperator(ctx, req.TaskSlug, req.Workspace, actor, req.Key, req.Target)
		}
		return encode(map[string]any{"binding_id": id, "generation": 1, "replayed": replayed}, err)
	case "bind-run":
		var req struct {
			Thread      string          `json:"thread_ref"`
			Environment json.RawMessage `json:"environment_id"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		if req.Thread == "" || len(req.Thread) > 256 {
			return nil, storage.ErrInvalidRunBinding
		}
		var err error
		if len(req.Environment) == 0 {
			err = store.BindRunThread(ctx, runID, req.Thread)
		} else {
			var environment string
			if json.Unmarshal(req.Environment, &environment) != nil {
				return nil, storage.ErrInvalidRunBinding
			}
			err = store.BindRunThreadInEnvironment(ctx, runID, environment, req.Thread)
		}
		return encode(map[string]bool{"bound": true}, err)
	case "authorize-run":
		var req struct {
			PackageID string          `json:"packageId"`
			Target    json.RawMessage `json:"target"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		result, err := store.AuthorizeRunDispatch(ctx, runID, actor, req.PackageID, req.Target)
		return encode(result, err)
	case "resolve-run":
		var req struct {
			AdmissionID string `json:"admissionId"`
			Kind        string `json:"kind"`
			Note        string `json:"note"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		err := store.ResolveRunDispatch(ctx, runID, req.AdmissionID, actor, req.Kind, req.Note)
		return encode(map[string]any{"recorded": true, "admissionId": req.AdmissionID}, err)
	case "confirm-run-stopped":
		var req struct {
			AdmissionID   string `json:"admissionId"`
			EnvironmentID string `json:"environmentId"`
			ThreadID      string `json:"threadId"`
			Note          string `json:"note"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		if err := store.ConfirmRunStopped(ctx, runID, req.AdmissionID, req.EnvironmentID, req.ThreadID, actor, req.Note); err != nil {
			return nil, fmt.Errorf("workbench dispatch command: %w", err)
		}
		return encode(store.ReadRunDispatch(ctx, runID))
	case "cancel-preparation":
		var req struct {
			PackageID  string                         `json:"packageId"`
			Note       string                         `json:"note"`
			RawHandoff *storage.RawPreparationHandoff `json:"rawHandoff"`
		}
		if err := decode(&req); err != nil {
			return nil, err
		}
		return encode(map[string]bool{"recorded": true}, store.CancelRunPreparation(ctx, runID, req.PackageID, actor, req.Note, req.RawHandoff))
	case "abort-preparation":
		var req storage.PreparationAbortRequest
		if err := decode(&req); err != nil {
			return nil, err
		}
		result, err := store.AbortPreparation(ctx, actor, &req)
		if errors.Is(err, storage.ErrDispatchResponsibilityHeld) && result.RunID != nil {
			data, marshalErr := json.Marshal(map[string]any{"runId": *result.RunID, "idempotencyKey": result.IdempotencyKey})
			if marshalErr != nil {
				return nil, fmt.Errorf("workbench dispatch command: %w", marshalErr)
			}
			return data, fmt.Errorf("workbench dispatch command: %w", err)
		}
		return encode(result, err)
	default:
		return nil, storage.ErrInvalidRunPreparation
	}
}
