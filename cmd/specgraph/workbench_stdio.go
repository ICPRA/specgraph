// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/spf13/cobra"
)

const workbenchReadTimeout = 15 * time.Second
const workbenchRequestLimit = 64 * 1024

type workbenchReadRequest struct {
	ID                 string `json:"id"`
	Resource           string `json:"resource"`
	Project            string `json:"project,omitempty"`
	Slug               string `json:"slug,omitempty"`
	Credential         string `json:"credential,omitempty"`
	TaskSlug           string `json:"taskSlug,omitempty"`
	State              string `json:"state,omitempty"`
	Cursor             string `json:"cursor,omitempty"`
	cursorNumber       int64
	AttemptCursor      string          `json:"attemptCursor,omitempty"`
	EventCursor        string          `json:"eventCursor,omitempty"`
	ThreadID           string          `json:"threadId,omitempty"`
	Query              string          `json:"query,omitempty"`
	Limit              int             `json:"limit,omitempty"`
	IncludeDescendants json.RawMessage `json:"includeDescendants,omitempty"`
}

// This inspection-only entry deliberately does not update key last-used statistics.
// Credential expiry, revocation, deleted-user and effective-role checks still run.
type workbenchReadOnlyTracker struct{}

func (workbenchReadOnlyTracker) Touch(string) {}

type workbenchReadError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type workbenchReadResponse struct {
	ID    string              `json:"id"`
	Data  map[string]any      `json:"data,omitempty"`
	Error *workbenchReadError `json:"error,omitempty"`
}

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "workbench-stdio",
		Short: "Read existing workbench nodes over newline-delimited JSON",
		Args:  cobra.NoArgs,
		// This child capability does not initialize telemetry, drift nudges,
		// Docker, migrations, HTTP handlers, or managed files.
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadGlobalExplicit(globalConfigPath())
			if err != nil {
				return errors.New("workbench-stdio: cannot load existing config; check --config")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), workbenchReadTimeout)
			store, err := postgres.OpenExistingReadOnly(ctx, cfg.Server.Postgres.URL)
			cancel()
			if errors.Is(err, postgres.ErrSchemaVersionMismatch) || workbenchDatabaseUnavailable(err) {
				return runWorkbenchStdio(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(),
					func(context.Context, workbenchReadRequest) (map[string]any, error) { return nil, err })
			}
			if err != nil {
				// Driver/config errors can contain the connection URL or credentials.
				return errors.New("workbench-stdio: cannot open existing PostgreSQL database; check configuration and availability")
			}
			defer store.Close(cmd.Context()) //nolint:errcheck // Concrete postgres.Store.Close closes its pool and always returns nil.
			return runWorkbenchStdio(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(),
				func(ctx context.Context, req workbenchReadRequest) (map[string]any, error) {
					protected := req.Resource == "review-status" || req.Resource == "change-preview" || req.Resource == "node-mark-history" || req.Resource == "mail-identity" || req.Resource == "mail-threads" || req.Resource == "mail-thread" || req.Resource == "operator-identity" || req.Resource == "dependencies" || req.Resource == "run-context" || req.Resource == "run-dispatch" || req.Resource == "delivery" || req.Resource == "knowledge-search" || req.Resource == "knowledge-spec" || req.Resource == "knowledge-decision" || req.Resource == "knowledge-change"
					protected = protected || req.Resource == "mail-directory" || req.Resource == "mail-owner-history" || req.Resource == "summary" || req.Resource == "summary-history" || req.Resource == "completion-hook"
					protected = protected || req.Resource == "program-loop" || req.Resource == "program-loop-history"
					if protected {
						procedure := auth.WorkbenchInspectMailProcedure
						var additionalProcedures []string
						switch req.Resource {
						case "operator-identity":
							procedure = ""
						case "completion-hook":
							procedure = auth.WorkbenchReadCompletionHookProcedure
						case "program-loop", "program-loop-history":
							procedure = auth.WorkbenchReadProgramRunProcedure
						case "change-preview":
							procedure = specgraphv1connect.SpecServiceGetSpecProcedure
							additionalProcedures = []string{specgraphv1connect.DecisionServiceGetDecisionProcedure}
						case "node-mark-history", "review-status", "summary", "summary-history":
							procedure = specgraphv1connect.SpecServiceGetSpecProcedure
						case "dependencies":
							procedure = auth.WorkbenchEditDependencyProcedure
						case "run-context", "knowledge-search", "knowledge-spec", "knowledge-decision", "knowledge-change":
							procedure = auth.WorkbenchReadRunContextProcedure
						case "run-dispatch":
							procedure = auth.WorkbenchDispatchProcedure
						case "delivery":
							procedure = auth.WorkbenchAcceptDeliveryProcedure
						}
						identity, err := resolveWorkbenchOperator(ctx, store, req.Credential, cfg.Auth.Policies.ExtraDirs, procedure, additionalProcedures...)
						if err != nil {
							return nil, err
						}
						if (req.Resource == "mail-directory" || req.Resource == "mail-owner-history") && identity.UserKind != storage.KindHuman {
							return nil, storage.ErrMailForbidden
						}
						if req.Resource == "completion-hook" {
							if identity.UserKind != storage.KindHuman {
								return nil, storage.ErrProgramRunForbidden
							}
							ctx = auth.WithIdentity(ctx, identity)
						}
						if req.Resource == "mail-identity" || req.Resource == "operator-identity" {
							return map[string]any{"identity": map[string]any{"subject": identity.Subject, "display_name": identity.DisplayName, "role": identity.EffectiveRole}}, nil
						}
					}
					if req.Resource == "projects" {
						return server.ReadWorkbenchProjects(ctx, store)
					}
					if req.Project == "_server" {
						return nil, storage.ErrProjectNotFound
					}
					scoped, scopeErr := store.ScopedExisting(ctx, req.Project)
					if scopeErr != nil {
						return nil, scopeErr
					}
					if req.Resource == "dependencies" {
						state, err := scoped.ReadDependencyEditState(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"specVersion": state.SpecVersion, "revision": strconv.FormatInt(state.Revision, 10), "operations": state.Operations}, nil
					}
					if req.Resource == "review-status" {
						status, err := scoped.ReadReviewStatus(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"taskSlug": status.TaskSlug, "reviews": status.Reviews}, nil
					}
					if req.Resource == "summary" {
						state, err := scoped.ReadSummary(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"summary": state}, nil
					}
					if req.Resource == "completion-hook" {
						hook, err := scoped.ReadHumanCompletionProgramHook(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"hook": hook}, nil
					}
					if req.Resource == "program-loop" {
						run, err := scoped.ReadProgramLoop(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"run": run}, nil
					}
					if req.Resource == "program-loop-history" {
						page, err := scoped.ReadProgramLoopHistory(ctx, req.Slug, req.AttemptCursor, req.EventCursor)
						if err != nil {
							return nil, err
						}
						return map[string]any{"history": page}, nil
					}
					if req.Resource == "summary-history" {
						page, err := scoped.ReadSummaryHistory(ctx, req.Slug, req.Query, req.Cursor)
						if err != nil {
							return nil, err
						}
						return map[string]any{"history": page}, nil
					}
					if req.Resource == "change-preview" {
						preview, err := scoped.ReadRequirementChangePreview(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"specSlug": preview.SpecSlug, "scope": preview.Scope, "nodes": preview.Nodes, "relations": preview.Relations}, nil
					}
					if req.Resource == "test-reports" {
						page, err := scoped.ReadTestReports(ctx, req.Slug, req.Cursor)
						if err != nil {
							return nil, err
						}
						return map[string]any{"deliveryId": page.DeliveryID, "reports": page.Reports, "hasMore": page.HasMore, "nextCursor": page.NextCursor}, nil
					}
					if req.Resource == "node-events" {
						page, err := scoped.ReadNodeEvents(ctx, req.Slug, req.Cursor)
						if err != nil {
							return nil, err
						}
						return map[string]any{"taskSlug": page.TaskSlug, "events": page.Events, "hasMore": page.HasMore, "nextCursor": page.NextCursor}, nil
					}
					if req.Resource == "node-mark-history" {
						page, err := scoped.ReadNodeMarkHistory(ctx, req.Slug, req.cursorNumber)
						if err != nil {
							return nil, err
						}
						return map[string]any{"items": page.Items, "hasMore": page.HasMore, "nextCursor": page.NextCursor}, nil
					}
					if req.Resource == "knowledge-search" {
						value, err := scoped.SearchWorkbenchKnowledge(ctx, req.Query)
						if err != nil {
							return nil, err
						}
						return map[string]any{"query": value.Query, "scope": value.Scope, "hasMore": value.HasMore, "items": value.Items}, nil
					}
					if req.Resource == "knowledge-spec" || req.Resource == "knowledge-decision" || req.Resource == "knowledge-change" {
						value, err := scoped.ReadWorkbenchKnowledgeRecord(ctx, strings.TrimPrefix(req.Resource, "knowledge-"), req.Slug)
						if err != nil {
							return nil, err
						}
						result := map[string]any{"kind": value.Kind, "id": value.ID, "slug": value.Slug, "version": value.Version, "record": value.Record}
						if value.SourceRefs != nil {
							result["sourceRefs"] = value.SourceRefs
						}
						return result, nil
					}
					if req.Resource == "run-context" {
						value, err := scoped.ReadRunContext(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"runId": value.RunID, "taskSlug": value.TaskSlug, "packageId": value.PackageID, "body": value.Body, "createdAt": value.CreatedAt}, nil
					}
					if req.Resource == "delivery" {
						value, err := scoped.ReadWorkbenchDelivery(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"id": value.ID, "runBindingId": value.RunBindingID, "submittedBy": value.SubmittedBy, "submittedAt": value.SubmittedAt, "snapshot": value.Snapshot}, nil
					}
					if req.Resource == "run-dispatch" {
						value, err := scoped.ReadRunDispatch(ctx, req.Slug)
						if err != nil {
							return nil, err
						}
						return map[string]any{"admission": value.Admission, "resolution": value.Resolution, "cancellation": value.Cancellation, "stopConfirmation": value.StopConfirmation}, nil
					}
					if req.Resource == "mail-threads" {
						page, err := scoped.InspectMailThreads(ctx, req.TaskSlug, req.State, 50, req.Cursor, string(req.IncludeDescendants) == "true")
						if err != nil {
							return nil, err
						}
						return server.MailInspectionThreadsJSON(page), nil
					}
					if req.Resource == "mail-thread" {
						page, err := scoped.InspectMailThread(ctx, req.ThreadID, 50, req.Cursor)
						if err != nil {
							return nil, err
						}
						return server.MailInspectionThreadJSON(&page), nil
					}
					if req.Resource == "mail-directory" {
						page, err := scoped.MailDirectory(ctx, req.Limit, req.Cursor, "")
						if err != nil {
							return nil, err
						}
						return map[string]any{"contacts": page.Contacts, "next_cursor": page.NextCursor}, nil
					}
					if req.Resource == "mail-owner-history" {
						page, err := scoped.MailOwnerHistory(ctx, "", req.ThreadID, req.Limit, req.Cursor)
						if err != nil {
							return nil, err
						}
						return map[string]any{"events": page.Events, "next_cursor": page.NextCursor}, nil
					}
					if req.Resource == "spec" {
						data, _, err := server.ReadWorkbenchSpec(ctx, scoped, req.Project, req.Slug)
						return data, err
					}
					data, _, err := server.ReadWorkbenchCurrentView(ctx, scoped, req.Project)
					return data, err
				})
		},
	})
}

func runWorkbenchStdio(ctx context.Context, input io.Reader, output io.Writer,
	read func(context.Context, workbenchReadRequest) (map[string]any, error),
) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), workbenchRequestLimit+2)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var req workbenchReadRequest
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&req)
		var trailing any
		validResource := false
		switch req.Resource {
		case "mail-directory", "mail-owner-history":
			if req.Limit == 0 {
				req.Limit = 50
			}
			validResource = strings.TrimSpace(req.Project) != "" && req.Slug == "" && req.TaskSlug == "" && req.State == "" && req.Limit >= 1 && req.Limit <= 100 &&
				((req.Resource == "mail-directory" && req.ThreadID == "") || (req.Resource == "mail-owner-history" && strings.TrimSpace(req.ThreadID) != ""))
		case "projects":
			validResource = req.Project == "" && req.Slug == ""
		case "current-view":
			validResource = strings.TrimSpace(req.Project) != "" && req.Slug == ""
		case "spec":
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.Slug) != ""
		case "mail-identity", "operator-identity":
			validResource = req.Project == "" && req.Slug == "" && req.TaskSlug == "" && req.State == "" && req.Cursor == "" && req.ThreadID == ""
		case "mail-threads":
			if req.State == "" {
				req.State = "open"
			}
			validResource = strings.TrimSpace(req.Project) != "" && req.Slug == "" && req.ThreadID == "" && (req.State == "open" || req.State == "closed" || req.State == "all")
		case "mail-thread":
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.ThreadID) != "" && req.Slug == "" && req.TaskSlug == "" && req.State == ""
		case "node-mark-history", "test-reports", "node-events":
			cursor, cursorErr := strconv.ParseInt(req.Cursor, 10, 64)
			req.cursorNumber = cursor
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.Slug) != "" && req.TaskSlug == "" && req.State == "" && req.ThreadID == "" &&
				(req.Cursor == "" || (cursorErr == nil && cursor > 0 && strconv.FormatInt(cursor, 10) == req.Cursor))
		case "program-loop", "completion-hook", "summary", "review-status", "change-preview", "dependencies", "run-context", "run-dispatch", "delivery", "knowledge-spec", "knowledge-decision", "knowledge-change":
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.Slug) != "" && req.TaskSlug == "" && req.State == "" && req.Cursor == "" && req.ThreadID == ""
		case "program-loop-history":
			cursor, cursorErr := strconv.Atoi(req.AttemptCursor)
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.Slug) != "" && req.TaskSlug == "" && req.State == "" && req.Cursor == "" && req.ThreadID == "" &&
				(req.AttemptCursor == "" || (cursorErr == nil && cursor > 0 && strconv.Itoa(cursor) == req.AttemptCursor)) &&
				(req.EventCursor == "" || (strings.TrimSpace(req.EventCursor) != "" && utf8.RuneCountInString(req.EventCursor) <= 256 && !strings.ContainsRune(req.EventCursor, 0)))
		case "summary-history":
			cursor, cursorErr := strconv.ParseInt(req.Cursor, 10, 64)
			validResource = strings.TrimSpace(req.Project) != "" && strings.TrimSpace(req.Slug) != "" && req.TaskSlug == "" && req.State == "" && req.ThreadID == "" &&
				(req.Query == "dispositions" || req.Query == "acceptances") && (req.Cursor == "" || (cursorErr == nil && cursor > 0 && strconv.FormatInt(cursor, 10) == req.Cursor))
		case "knowledge-search":
			req.Query = strings.TrimSpace(req.Query)
			validResource = strings.TrimSpace(req.Project) != "" && req.Query != "" && utf8.RuneCountInString(req.Query) <= 256 &&
				req.Slug == "" && req.TaskSlug == "" && req.State == "" && req.Cursor == "" && req.ThreadID == ""
		}
		if req.Resource != "knowledge-search" && req.Resource != "summary-history" && req.Query != "" {
			validResource = false
		}
		if req.Resource != "program-loop-history" && (req.AttemptCursor != "" || req.EventCursor != "") {
			validResource = false
		}
		if len(req.IncludeDescendants) != 0 && (req.Resource != "mail-threads" ||
			(string(req.IncludeDescendants) != "true" && string(req.IncludeDescendants) != "false") ||
			(string(req.IncludeDescendants) == "true" && strings.TrimSpace(req.TaskSlug) == "")) {
			validResource = false
		}
		if req.Resource != "mail-directory" && req.Resource != "mail-owner-history" && req.Limit != 0 {
			validResource = false
		}
		if req.Resource == "projects" || req.Resource == "current-view" || req.Resource == "spec" {
			validResource = validResource && req.Credential == "" && req.TaskSlug == "" && req.State == "" && req.Cursor == "" && req.ThreadID == ""
		}
		if decodeErr != nil || decoder.Decode(&trailing) != io.EOF || len(scanner.Bytes()) > workbenchRequestLimit ||
			strings.TrimSpace(req.ID) == "" || !validResource {
			if err := encoder.Encode(workbenchReadResponse{ID: req.ID, Error: &workbenchReadError{
				Code: "invalid_request", Message: "invalid fields for workbench read resource; maximum 64 KiB",
			}}); err != nil {
				return fmt.Errorf("workbench-stdio: write response: %w", err)
			}
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, workbenchReadTimeout)
		data, err := read(readCtx, req)
		cancel()
		response := workbenchReadResponse{ID: req.ID}
		var databaseError *pgconn.PgError
		switch {
		case errors.Is(err, storage.ErrInvalidProgramLoop), errors.Is(err, storage.ErrInvalidProgramRun):
			response.Error = &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
		case errors.Is(err, storage.ErrInvalidCompletionHook):
			response.Error = &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
		case errors.Is(err, storage.ErrCompletionHookNotFound):
			response.Error = &workbenchReadError{Code: "not_found", Message: err.Error()}
		case errors.Is(err, storage.ErrProgramRunForbidden), errors.Is(err, storage.ErrPlanningForbidden):
			response.Error = &workbenchReadError{Code: "forbidden", Message: err.Error()}
		case errors.Is(err, storage.ErrInvalidSummary):
			response.Error = &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
		case errors.Is(err, storage.ErrSummaryForbidden):
			response.Error = &workbenchReadError{Code: "forbidden", Message: err.Error()}
		case errors.Is(err, storage.ErrSummaryConflict):
			response.Error = &workbenchReadError{Code: "conflict", Message: err.Error()}
		case errors.Is(err, storage.ErrSummaryNotAcceptable):
			response.Error = &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
		case errors.Is(err, storage.ErrSummaryAcceptanceNotFound):
			response.Error = &workbenchReadError{Code: "not_found", Message: err.Error()}
		case errors.Is(err, auth.ErrUnauthenticated):
			response.Error = &workbenchReadError{Code: "unauthorized", Message: "valid existing SpecGraph API key (spgr_sk_) or session (spgr_ws_) required"}
		case errors.Is(err, storage.ErrMailForbidden):
			response.Error = &workbenchReadError{Code: "forbidden", Message: "permission for this workbench read is required"}
		case errors.Is(err, storage.ErrMailInvalid):
			response.Error = &workbenchReadError{Code: "invalid_request", Message: "invalid mail inspection request"}
		case errors.Is(err, storage.ErrSpecNotFound), errors.Is(err, storage.ErrDecisionNotFound), errors.Is(err, postgres.ErrWorkbenchChangeNotFound), errors.Is(err, storage.ErrProjectNotFound), errors.Is(err, storage.ErrMailNotFound), errors.Is(err, storage.ErrRunContextNotFound), errors.Is(err, storage.ErrRunBindingNotFound), errors.Is(err, storage.ErrDeliveryNotFound):
			response.Error = &workbenchReadError{Code: "not_found", Message: "project, node or delivery not found"}
		case errors.Is(err, postgres.ErrSchemaVersionMismatch):
			response.Error = &workbenchReadError{Code: "schema_mismatch", Message: "database requires the current initialization schema; no data was changed"}
		case workbenchDatabaseUnavailable(err):
			response.Error = &workbenchReadError{Code: "database_unavailable", Message: "configured PostgreSQL is not accepting connections"}
		case errors.Is(err, context.DeadlineExceeded):
			response.Error = &workbenchReadError{Code: "timeout", Message: "workbench read timed out"}
		case errors.As(err, &databaseError) && (databaseError.Code == "42703" || databaseError.Code == "42P01"):
			response.Error = &workbenchReadError{Code: "schema_mismatch", Message: "database schema does not match this build; no migration was run"}
		case err != nil:
			response.Error = &workbenchReadError{Code: "read_failed", Message: "cannot read workbench data from existing database"}
		default:
			if req.Resource == "current-view" {
				capabilities := data["capabilities"].(map[string]bool) //nolint:errcheck // ReadWorkbenchCurrentView constructs this typed map, not external input.
				for capability := range capabilities {
					capabilities[capability] = capability == "mailInspection" || capability == "manualCompletion" || capability == "dependencyEditing" || capability == "dependencyRemoval" || capability == "deliveryHooks"
				}
				capabilities["createNode"] = true
				capabilities["nodeApproval"] = true
				capabilities["runDispatch"] = true
				capabilities["deliveryReview"] = true
				capabilities["subdivision"] = true
				capabilities["nodeMarks"] = true
				capabilities["nodeEvents"] = true
				capabilities["changePreview"] = true
				capabilities["abandonNode"] = true
				capabilities["independentReview"] = true
				capabilities["testReports"] = true
				data["basis"] = map[string]any{"source": "SpecGraph store (local read-only stdio)", "limitation": "Authorized node and dependency actions use a separate command process; the mail panel is inspection-only."}
				data["readOnlyTransport"] = true
			}
			response.Data = data
		}
		if err := encoder.Encode(response); err != nil {
			return fmt.Errorf("workbench-stdio: write response: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		if writeErr := encoder.Encode(workbenchReadResponse{ID: "", Error: &workbenchReadError{
			Code: "invalid_request", Message: "cannot read request line; maximum 64 KiB",
		}}); writeErr != nil {
			return fmt.Errorf("workbench-stdio: write response: %w", writeErr)
		}
		return fmt.Errorf("workbench-stdio: read input: %w", err)
	}
	return nil
}

// Only refusal and PostgreSQL startup state permit the host's availability handling.
func workbenchDatabaseUnavailable(err error) bool {
	var pgError *pgconn.PgError
	return errors.Is(err, syscall.ECONNREFUSED) || (errors.As(err, &pgError) && pgError.Code == "57P03")
}

// resolveWorkbenchOperator verifies an existing credential without login writes.
// An empty procedure is identity-only; all protected operations pass their policy.
func resolveWorkbenchOperator(ctx context.Context, store *postgres.Store, credential string, policyDirs []string, procedure string, additionalProcedures ...string) (*auth.Identity, error) {
	if !strings.HasPrefix(credential, "spgr_sk_") && !strings.HasPrefix(credential, "spgr_ws_") {
		return nil, auth.ErrUnauthenticated
	}
	authStore := store.ExistingAuth()
	resolver, err := auth.NewIdentityStore(auth.IdentityStoreConfig{Users: authStore, WebAuth: authStore, Tracker: workbenchReadOnlyTracker{}})
	if err != nil {
		return nil, err
	}
	identity, err := resolver.Resolve(ctx, credential)
	if err != nil {
		return nil, err
	}
	if identity.UserID == "" {
		return nil, auth.ErrUnauthenticated
	}
	if procedure == "" && len(additionalProcedures) == 0 {
		return identity, nil
	}
	sources := []auth.PolicySource{auth.NewEmbeddedPolicySource()}
	for _, dir := range policyDirs {
		sources = append(sources, auth.NewDirectoryPolicySource(dir))
	}
	engine, err := auth.NewCedarEngine(ctx, sources, auth.ActionNames())
	if err != nil {
		return nil, err
	}
	for _, procedure := range append([]string{procedure}, additionalProcedures...) {
		decision, err := auth.NewCedarAuthorizer(engine).Authorize(ctx, identity, procedure, nil)
		if err != nil {
			return nil, err
		}
		if !decision.Allowed {
			return nil, storage.ErrMailForbidden
		}
	}
	return identity, nil
}
