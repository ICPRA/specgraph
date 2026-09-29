// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

func decodeMailBody(w http.ResponseWriter, r *http.Request, body any) bool {
	if err := decodeMailRequest(http.MaxBytesReader(w, r.Body, 512<<10), body); err != nil {
		writeMailError(w, err)
		return false
	}
	return true
}

func decodeMailRequest(reader io.Reader, body any) error {
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()
	if err := d.Decode(body); err != nil {
		return storage.ErrMailInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return storage.ErrMailInvalid
	}
	return nil
}

func writeMailError(w http.ResponseWriter, err error) {
	status, code, message := http.StatusInternalServerError, "internal", "mail operation failed"
	switch {
	case errors.Is(err, storage.ErrMailInvalid):
		status, code, message = 400, "invalid_argument", "invalid mail request"
	case errors.Is(err, storage.ErrMailForbidden):
		status, code, message = 403, "permission_denied", "mail delegation or participation unavailable"
	case errors.Is(err, storage.ErrMailNotFound), errors.Is(err, storage.ErrProjectNotFound), errors.Is(err, storage.ErrRunBindingNotFound), errors.Is(err, storage.ErrSpecNotFound):
		status, code, message = 404, "not_found", "mail resource not found"
	case errors.Is(err, storage.ErrMailConflict), errors.Is(err, storage.ErrMailClosed):
		status, code, message = 409, "conflict", "mail request conflicts with stored state"
	default:
		slog.Error("workbench mail operation failed", slog.Any("error", err))
	}
	writeLoopJSON(w, status, map[string]string{"code": code, "error": message})
}

// The HTTP representation is explicit; storage Go field names are not a wire contract.
func mailThreadJSON(t *storage.MailThread) any {
	return struct {
		ID          string     `json:"id"`
		TaskSlug    string     `json:"task_slug"`
		OpenedByRun string     `json:"opened_by_run"`
		OwnerRun    string     `json:"owner_run"`
		Subject     string     `json:"subject"`
		CreatedAt   time.Time  `json:"created_at"`
		ClosedAt    *time.Time `json:"closed_at"`
		ClosedByRun *string    `json:"closed_by_run"`
		ClosureNote *string    `json:"closure_note"`
	}{t.ID, t.TaskSlug, t.OpenedByRun, t.OwnerRun, t.Subject, t.CreatedAt, t.ClosedAt, t.ClosedByRun, t.ClosureNote}
}

func mailMessageJSON(m *storage.MailMessage) any {
	references := m.References
	if references == nil {
		references = []storage.MailReference{}
	}
	return struct {
		ID              string                  `json:"id"`
		ThreadID        string                  `json:"thread_id"`
		SenderRun       string                  `json:"sender_run"`
		IdempotencyKey  string                  `json:"idempotency_key"`
		Body            string                  `json:"body"`
		CreatedAt       time.Time               `json:"created_at"`
		RecipientRunIDs []string                `json:"recipient_run_ids"`
		References      []storage.MailReference `json:"references"`
		HandoffToRun    *string                 `json:"handoff_to_run"`
	}{m.ID, m.ThreadID, m.SenderRun, m.IdempotencyKey, m.Body, m.CreatedAt, m.RecipientRunIDs, references, m.HandoffToRun}
}

func mailReceiptJSON(r *storage.MailReceipt) any {
	if r == nil {
		return nil
	}
	return struct {
		ReadAt         *time.Time `json:"read_at"`
		AcknowledgedAt *time.Time `json:"acknowledged_at"`
	}{r.ReadAt, r.AcknowledgedAt}
}

func mailPageJSON(p storage.MailPage) map[string]any {
	items := make([]any, 0, len(p.Items))
	for i := range p.Items {
		item := &p.Items[i]
		items = append(items, map[string]any{"message": mailMessageJSON(&item.Message), "receipt": mailReceiptJSON(item.Receipt)})
	}
	return map[string]any{"items": items, "next_cursor": p.NextCursor}
}

func registerWorkbenchMail(mux *http.ServeMux, root *postgres.Store, resolver auth.Resolver, authorizer auth.Authorizer) {
	// No default project on this delegation boundary: the bridge must name it.
	resolve := func(w http.ResponseWriter, r *http.Request) (*postgres.Store, bool) {
		if r.Header.Get("X-Specgraph-Project") == "" {
			writeMailError(w, storage.ErrMailInvalid)
			return nil, false
		}
		s, err := resolveWorkbenchStore(r, root)
		if err != nil {
			writeMailError(w, err)
			return nil, false
		}
		return s, true
	}
	register := func(pattern, procedure string, h http.HandlerFunc) {
		bounded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
			defer cancel()
			h(w, r.WithContext(ctx))
		})
		mux.Handle(pattern, workbenchWriteAuth(resolver, authorizer, procedure, bounded))
	}
	for _, route := range []struct{ path, operation string }{{"grants", "grant"}, {"bindings", "bind"}} {
		register("POST /loop/mail/admin/"+route.path, auth.WorkbenchMailManageProcedure, func(w http.ResponseWriter, r *http.Request) {
			result, err := ExecuteWorkbenchMailEnrollment(r.Context(), root, r.Header.Get("X-Specgraph-Project"), route.operation, http.MaxBytesReader(w, r.Body, 512<<10))
			if err != nil {
				writeMailError(w, err)
				return
			}
			writeLoopJSON(w, 200, result)
		})
	}
	for _, kind := range []string{"grants", "bindings"} {
		register("POST /loop/mail/admin/"+kind+"/{id}/revoke", auth.WorkbenchMailManageProcedure, func(w http.ResponseWriter, r *http.Request) {
			var req struct{}
			if !decodeMailBody(w, r, &req) {
				return
			}
			s, ok := resolve(w, r)
			if !ok {
				return
			}
			id, _ := auth.IdentityFromContext(r.Context())
			var result any
			var err error
			if kind == "grants" {
				result, err = s.RevokeMailGrant(r.Context(), r.PathValue("id"), id.UserID)
			} else {
				result, err = s.RevokeMailBinding(r.Context(), r.PathValue("id"), id.UserID)
			}
			if err != nil {
				writeMailError(w, err)
				return
			}
			writeLoopJSON(w, 200, result)
		})
	}
	for _, op := range []string{"send", "inbox", "thread", "read", "ack", "close"} {
		procedure := auth.WorkbenchMailWriteProcedure
		if op == "inbox" || op == "thread" {
			procedure = auth.WorkbenchMailReadProcedure
		}
		register("POST /loop/mail/"+op, procedure, func(w http.ResponseWriter, r *http.Request) {
			result, err := ExecuteWorkbenchMail(r.Context(), root, r.Header.Get("X-Specgraph-Project"), op, http.MaxBytesReader(w, r.Body, 512<<10))
			if err != nil {
				writeMailError(w, err)
				return
			}
			writeLoopJSON(w, 200, result)
		})
	}
}

// ExecuteWorkbenchMailEnrollment is called only after mail.manage authorization.
func ExecuteWorkbenchMailEnrollment(ctx context.Context, root *postgres.Store, project, op string, body io.Reader) (any, error) {
	if project == "" {
		return nil, storage.ErrMailInvalid
	}
	id, ok := auth.IdentityFromContext(ctx)
	if !ok {
		return nil, storage.ErrMailForbidden
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench mail: %w", err)
	}
	switch op {
	case "grant":
		var req struct {
			EnvironmentID string `json:"environment_id"`
			BridgeSubject string `json:"bridge_subject"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		result, callErr := s.GrantMailBridge(ctx, req.EnvironmentID, req.BridgeSubject, id.UserID)
		if callErr != nil {
			return result, fmt.Errorf("workbench mail: %w", callErr)
		}
		return result, nil
	case "bind":
		var req struct {
			Scope   storage.MailScope `json:"scope"`
			GrantID string            `json:"grant_id"`
			RunID   string            `json:"run_id"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		result, callErr := s.ApproveMailBinding(ctx, req.GrantID, req.RunID, req.Scope, id.UserID)
		if callErr != nil {
			return result, fmt.Errorf("workbench mail: %w", callErr)
		}
		return result, nil
	default:
		return nil, storage.ErrMailInvalid
	}
}

// ExecuteWorkbenchMail shares existing scoped mail operations with local callers.
// The caller must authenticate and authorize the operation first.
func ExecuteWorkbenchMail(ctx context.Context, root *postgres.Store, project, op string, body io.Reader) (any, error) {
	return executeWorkbenchMail(ctx, root, project, op, body, false)
}

// ExecuteLocalWorkbenchMail uses verified local scope with the existing mail authorization.
func ExecuteLocalWorkbenchMail(ctx context.Context, root *postgres.Store, project, op string, body io.Reader) (any, error) {
	return executeWorkbenchMail(ctx, root, project, op, body, true)
}

func executeWorkbenchMail(ctx context.Context, root *postgres.Store, project, op string, body io.Reader, local bool) (any, error) {
	id, ok := auth.IdentityFromContext(ctx)
	if !ok || id.Source != "apikey" || id.EffectiveRole != auth.RoleReader {
		return nil, storage.ErrMailForbidden
	}
	// Each operation has its own DTO so irrelevant fields are rejected too.
	var scope storage.MailScope
	var operation func(context.Context, *postgres.Store, storage.MailActor) (any, error)
	switch op {
	case "takeover":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope storage.MailScope `json:"scope"`
			storage.TakeoverMailRequest
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, _ storage.MailActor) (any, error) {
			result, err := s.TakeoverMail(ctx, req.TakeoverMailRequest, &scope)
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return map[string]any{"thread": mailThreadJSON(&result.Thread), "event": result.Event}, err
		}
	case "retired":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope  storage.MailScope `json:"scope"`
			Limit  int               `json:"limit"`
			Cursor string            `json:"cursor"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, _ storage.MailActor) (any, error) {
			page, err := s.RetiredMailThreads(ctx, req.Limit, req.Cursor)
			threads := make([]any, 0, len(page.Threads))
			for i := range page.Threads {
				threads = append(threads, mailThreadJSON(&page.Threads[i]))
			}
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return map[string]any{"threads": threads, "next_cursor": page.NextCursor}, err
		}
	case "owner-history":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope    storage.MailScope `json:"scope"`
			ThreadID string            `json:"mail_thread_id"`
			Limit    int               `json:"limit"`
			Cursor   string            `json:"cursor"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, actor storage.MailActor) (any, error) {
			return s.MailOwnerHistory(ctx, actor.RunID, req.ThreadID, req.Limit, req.Cursor)
		}
	case "owned":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope  storage.MailScope `json:"scope"`
			Limit  int               `json:"limit"`
			Cursor string            `json:"cursor"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, actor storage.MailActor) (any, error) {
			page, err := s.OwnedMailThreads(ctx, actor.RunID, req.Limit, req.Cursor)
			threads := make([]any, 0, len(page.Threads))
			for i := range page.Threads {
				threads = append(threads, mailThreadJSON(&page.Threads[i]))
			}
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return map[string]any{"threads": threads, "next_cursor": page.NextCursor}, err
		}
	case "directory":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope          storage.MailScope `json:"scope"`
			Limit          int               `json:"limit"`
			Cursor         string            `json:"cursor"`
			AssignmentRole string            `json:"assignment_role"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, _ storage.MailActor) (any, error) {
			return s.MailDirectory(ctx, req.Limit, req.Cursor, req.AssignmentRole)
		}
	case "context":
		var req struct {
			Scope storage.MailScope `json:"scope"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(_ context.Context, _ *postgres.Store, actor storage.MailActor) (any, error) { //nolint:unparam // The shared operation/WithMailActor contract requires (any, error); this context projection cannot fail.
			return map[string]string{"project": project, "run_id": actor.RunID, "task_slug": actor.TaskSlug}, nil
		}
	case "send":
		var req struct {
			Scope           storage.MailScope              `json:"scope"`
			MailThreadID    string                         `json:"mail_thread_id"`
			Subject         string                         `json:"subject"`
			Body            string                         `json:"body"`
			RecipientRunIDs []string                       `json:"recipient_run_ids"`
			IdempotencyKey  string                         `json:"idempotency_key"`
			References      []storage.MailReferenceRequest `json:"references"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			task := a.TaskSlug
			if req.MailThreadID != "" {
				page, err := s.ReadMailThread(ctx, a.RunID, req.MailThreadID, 1, "")
				if err != nil {
					return nil, fmt.Errorf("workbench mail: %w", err)
				}
				task = page.Thread.TaskSlug
			}
			result, err := s.SendMail(ctx, a.RunID, &storage.SendMailRequest{ThreadID: req.MailThreadID, TaskSlug: task, Subject: req.Subject, Body: req.Body, RecipientRunIDs: req.RecipientRunIDs, IdempotencyKey: req.IdempotencyKey, References: req.References})
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return map[string]any{"thread": mailThreadJSON(&result.Thread), "message": mailMessageJSON(&result.Message)}, err
		}
	case "handoff":
		if !local {
			return nil, storage.ErrMailInvalid
		}
		var req struct {
			Scope          storage.MailScope `json:"scope"`
			MailThreadID   string            `json:"mail_thread_id"`
			RecipientRunID string            `json:"recipient_run_id"`
			Body           string            `json:"body"`
			IdempotencyKey string            `json:"idempotency_key"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			page, err := s.ReadMailThread(ctx, a.RunID, req.MailThreadID, 1, "")
			if err != nil {
				return nil, fmt.Errorf("workbench mail: %w", err)
			}
			result, err := s.SendMail(ctx, a.RunID, &storage.SendMailRequest{ThreadID: req.MailThreadID, TaskSlug: page.Thread.TaskSlug, Subject: page.Thread.Subject, Body: req.Body, RecipientRunIDs: []string{req.RecipientRunID}, IdempotencyKey: req.IdempotencyKey, HandoffToRun: req.RecipientRunID})
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return map[string]any{"thread": mailThreadJSON(&result.Thread), "message": mailMessageJSON(&result.Message)}, err
		}
	case "inbox":
		var req struct {
			Scope  storage.MailScope `json:"scope"`
			Limit  int               `json:"limit"`
			Cursor string            `json:"cursor"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			page, err := s.Inbox(ctx, a.RunID, req.Limit, req.Cursor)
			return mailPageJSON(page), err
		}
	case "thread":
		var req struct {
			Scope        storage.MailScope `json:"scope"`
			MailThreadID string            `json:"mail_thread_id"`
			Limit        int               `json:"limit"`
			Cursor       string            `json:"cursor"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			page, err := s.ReadMailThread(ctx, a.RunID, req.MailThreadID, req.Limit, req.Cursor)
			result := mailPageJSON(page.MailPage)
			result["thread"] = mailThreadJSON(&page.Thread)
			if err != nil {
				err = fmt.Errorf("workbench mail: %w", err)
			}
			return result, err
		}
	case "read", "ack":
		var req struct {
			Scope     storage.MailScope `json:"scope"`
			MessageID string            `json:"message_id"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			var receipt storage.MailReceipt
			var err error
			if op == "read" {
				receipt, err = s.MarkMailRead(ctx, a.RunID, req.MessageID)
			} else {
				receipt, err = s.AcknowledgeMail(ctx, a.RunID, req.MessageID)
			}
			return mailReceiptJSON(&receipt), err
		}
	case "close":
		var req struct {
			Scope        storage.MailScope `json:"scope"`
			MailThreadID string            `json:"mail_thread_id"`
			Resolution   string            `json:"resolution"`
		}
		if err := decodeMailRequest(body, &req); err != nil {
			return nil, err
		}
		scope = req.Scope
		operation = func(ctx context.Context, s *postgres.Store, a storage.MailActor) (any, error) {
			thread, err := s.CloseMailThread(ctx, a.RunID, req.MailThreadID, req.Resolution)
			return mailThreadJSON(&thread), err
		}
	}
	if operation == nil || project == "" {
		return nil, storage.ErrMailInvalid
	}
	s, scopeErr := root.ScopedExisting(ctx, project)
	if scopeErr != nil {
		return nil, fmt.Errorf("workbench mail: %w", scopeErr)
	}
	var result any
	var err error
	if local {
		if bindingErr := s.EnsureLocalMailBinding(ctx, id.Subject, id.UserID, scope); bindingErr != nil {
			return nil, fmt.Errorf("workbench mail: %w", bindingErr)
		}
	}
	if op == "takeover" {
		// Takeover owns project-before-mailbox locking and PM authorization.
		return operation(ctx, s, storage.MailActor{})
	}
	if op == "retired" {
		err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
			manager, managerErr := s.PlanningManagerRun(txCtx, scope)
			if managerErr != nil {
				return fmt.Errorf("workbench mail: %w", managerErr)
			}
			return s.WithMailActor(txCtx, id.Subject, scope, func(actorCtx context.Context, actor storage.MailActor) error {
				if actor.RunID != manager {
					return storage.ErrMailForbidden
				}
				result, err = operation(actorCtx, s, actor)
				return err
			})
		})
		if err != nil {
			err = fmt.Errorf("workbench mail: %w", err)
		}
		return result, err
	}
	err = s.WithMailActor(ctx, id.Subject, scope, func(ctx context.Context, actor storage.MailActor) error {
		var operationErr error
		result, operationErr = operation(ctx, s, actor)
		return operationErr
	})
	if err != nil {
		err = fmt.Errorf("workbench mail: %w", err)
	}
	return result, err
}
