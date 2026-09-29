// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// This operator view requires the existing workbench.manage policy, not a run grant.
// Inspection never sends mail or changes read, acknowledgement or closure state.
func registerMailInspection(mux *http.ServeMux, root *postgres.Store, resolver auth.Resolver, authorizer auth.Authorizer) {
	for _, pattern := range []string{"GET /loop/mail/threads", "GET /loop/mail/threads/{id}"} {
		mux.Handle(pattern, workbenchWriteAuth(resolver, authorizer, auth.WorkbenchInspectMailProcedure, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
			defer cancel()
			r = r.WithContext(ctx)
			limit := 50
			q := r.URL.Query()
			includeDescendants := false
			if q.Has("include_descendants") {
				value := q.Get("include_descendants")
				if len(q["include_descendants"]) != 1 || (value != "true" && value != "false") || r.PathValue("id") != "" {
					writeMailError(w, storage.ErrMailInvalid)
					return
				}
				includeDescendants = value == "true"
			}
			if q.Has("limit") {
				var err error
				limit, err = strconv.Atoi(q.Get("limit"))
				if err != nil || limit < 1 || limit > 100 {
					writeMailError(w, storage.ErrMailInvalid)
					return
				}
			}
			state := q.Get("state")
			if !q.Has("state") {
				state = "open"
			}
			if r.Header.Get("X-Specgraph-Project") == "" || (state != "open" && state != "closed" && state != "all") {
				writeMailError(w, storage.ErrMailInvalid)
				return
			}
			s, scopeErr := resolveWorkbenchStore(r, root)
			if scopeErr != nil {
				writeMailError(w, scopeErr)
				return
			}
			if id := r.PathValue("id"); id != "" {
				page, err := s.InspectMailThread(ctx, id, limit, q.Get("cursor"))
				if err != nil {
					writeMailError(w, err)
					return
				}
				writeLoopJSON(w, 200, MailInspectionThreadJSON(&page))
				return
			}
			page, err := s.InspectMailThreads(ctx, q.Get("task_slug"), state, limit, q.Get("cursor"), includeDescendants)
			if err != nil {
				writeMailError(w, err)
				return
			}
			writeLoopJSON(w, 200, MailInspectionThreadsJSON(page))
		})))
	}
}

// MailInspectionThreadsJSON preserves the operator thread-list response across transports.
func MailInspectionThreadsJSON(page storage.MailInspectionThreads) map[string]any {
	threads := make([]any, 0, len(page.Threads))
	for i := range page.Threads {
		t := &page.Threads[i]
		threads = append(threads, map[string]any{
			"id": t.ID, "task_slug": t.TaskSlug, "opened_by_run": t.OpenedByRun, "owner_run": t.OwnerRun, "subject": t.Subject,
			"created_at": t.CreatedAt, "closed_at": t.ClosedAt, "closed_by_run": t.ClosedByRun, "closure_note": t.ClosureNote,
			"message_count": t.MessageCount, "pending_ack_count": t.PendingAckCount, "last_message_at": t.LastMessageAt, "participant_run_ids": t.ParticipantRunIDs,
			"node_links": t.NodeLinks,
		})
	}
	return map[string]any{"threads": threads, "next_cursor": page.NextCursor}
}

// MailInspectionThreadJSON preserves messages, references and receipts without mutations.
func MailInspectionThreadJSON(page *storage.MailInspectionPage) map[string]any {
	items := make([]any, 0, len(page.Items))
	for i := range page.Items {
		item := &page.Items[i]
		items = append(items, map[string]any{"message": mailMessageJSON(&item.Message), "receipts": item.Receipts})
	}
	return map[string]any{"thread": mailThreadJSON(&page.Thread), "items": items, "next_cursor": page.NextCursor}
}
