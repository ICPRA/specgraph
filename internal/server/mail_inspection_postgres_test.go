// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestMailInspectionHTTPPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("inspection-http"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "inspection-http-other")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES('inspection-http','task');
	 INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES('inspection-http','inspect-http-a','task'),('inspection-http','inspect-http-b','task'),('inspection-http','inspect-http-c','task')`)
	require.NoError(t, err)
	req := storage.SendMailRequest{TaskSlug: "task", Subject: "Test subject", Body: "source", RecipientRunIDs: []string{"inspect-http-b", "inspect-http-c"}, IdempotencyKey: "first"}
	first, err := s.SendMail(ctx, "inspect-http-a", &req)
	require.NoError(t, err)
	req.ThreadID, req.IdempotencyKey, req.Body = first.Thread.ID, "reply", "reply body"
	req.References = []storage.MailReferenceRequest{{MessageID: first.Message.ID, Kind: "reply"}}
	_, err = s.SendMail(ctx, "inspect-http-a", &req)
	require.NoError(t, err)
	_, err = s.MarkMailRead(ctx, "inspect-http-b", first.Message.ID)
	require.NoError(t, err)
	engine, err := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	require.NoError(t, err)
	resolver := &workbenchAuthResolver{identity: &auth.Identity{UserID: "operator", Subject: "human-session", EffectiveRole: auth.RoleAdmin}}
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, s, resolver, auth.NewCedarAuthorizer(engine))
	get := func(project, path string, want int) map[string]any {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Specgraph-Project", project)
		r.AddCookie(&http.Cookie{Name: "specgraph_session", Value: "session"})
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		require.Equal(t, want, w.Code, w.Body.String())
		var body map[string]any
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		return body
	}
	const snapshot = `SELECT jsonb_build_array(
	 (SELECT jsonb_agg(to_jsonb(t) ORDER BY project_slug,id) FROM mail_threads t),
	 (SELECT jsonb_agg(to_jsonb(m) ORDER BY project_slug,id) FROM mail_messages m),
	 (SELECT jsonb_agg(to_jsonb(r) ORDER BY project_slug,message_id,recipient_run) FROM mail_recipients r),
	 (SELECT jsonb_agg(to_jsonb(r) ORDER BY project_slug,message_id,source_message_id) FROM mail_references r))::text`
	var before, after string
	require.NoError(t, s.Pool().QueryRow(ctx, snapshot).Scan(&before))
	list := get("inspection-http", "/loop/mail/threads", 200)
	threads := list["threads"].([]any)
	require.Len(t, threads, 1)
	thread := threads[0].(map[string]any)
	require.Len(t, thread, 14)
	require.Equal(t, "inspect-http-a", thread["owner_run"])
	require.Equal(t, float64(2), thread["message_count"])
	require.Equal(t, float64(4), thread["pending_ack_count"])
	require.Equal(t, "task", thread["task_slug"])
	require.Len(t, thread["participant_run_ids"], 3)
	require.Equal(t, []any{map[string]any{
		"sender_task_slug": "task", "recipient_task_slug": "task", "message_count": float64(4), "pending_ack_count": float64(4),
	}}, thread["node_links"])
	path := "/loop/mail/threads/" + first.Thread.ID
	detail := get("inspection-http", path+"?limit=1", 200)
	require.Equal(t, first.Message.ID, detail["next_cursor"])
	item := detail["items"].([]any)[0].(map[string]any)
	require.Len(t, item["receipts"], 2)
	receipt := item["receipts"].([]any)[0].(map[string]any)
	require.Equal(t, "inspect-http-b", receipt["recipient_run_id"])
	require.NotNil(t, receipt["read_at"])
	require.Nil(t, receipt["acknowledged_at"])
	detail = get("inspection-http", path+"?limit=1&cursor="+first.Message.ID, 200)
	require.Empty(t, detail["next_cursor"])
	message := detail["items"].([]any)[0].(map[string]any)["message"].(map[string]any)
	reference := message["references"].([]any)[0].(map[string]any)
	require.Equal(t, "source", reference["body"])
	require.Equal(t, "reply", reference["kind"])
	require.Empty(t, get("inspection-http-other", "/loop/mail/threads", 200)["threads"])
	get("inspection-http-other", path, 404)
	get("missing-project", "/loop/mail/threads", 404)
	get("inspection-http", "/loop/mail/threads?cursor="+strings.Repeat("x", 257), 400)
	get("inspection-http", "/loop/mail/threads?task_slug=%00", 400)
	get("inspection-http", path+"?cursor=%00", 400)
	get("inspection-http", "/loop/mail/threads?include_descendants=true", 400)
	get("inspection-http", "/loop/mail/threads?task_slug=task&include_descendants=1", 400)
	get("inspection-http", path+"?include_descendants=false", 400)
	withFalse := get("inspection-http", "/loop/mail/threads?include_descendants=false", 200)
	require.Equal(t, list, withFalse)
	withTrue := get("inspection-http", "/loop/mail/threads?task_slug=task&include_descendants=true", 200)
	require.Equal(t, list, withTrue)
	require.NoError(t, s.Pool().QueryRow(ctx, snapshot).Scan(&after))
	require.Equal(t, before, after, "HTTP inspection changed stored mail")
}

func TestLocalMailDirectoryPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("mail-directory"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.EnsureProject(ctx, "mail-directory-other")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-directory','task'),('mail-directory-other','task');
	 INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES
	 ('directory-knowledge','mail-directory','task','{"dispatch_target":{"assignmentRole":"knowledge"}}'),
	 ('directory-other-package','mail-directory-other','task','{"dispatch_target":{"assignmentRole":"knowledge"}}');
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug,thread_ref,environment_id,state,package_id) VALUES
	 ('dir-a','mail-directory','task','thread-a','env','bound',''),
	 ('dir-b','mail-directory','task','thread-b','env','bound',''),
	 ('dir-c','mail-directory','task','thread-c','env','bound','directory-knowledge'),
	 ('dir-d','mail-directory','task','thread-d','env','bound','directory-knowledge'),
	 ('dir-e','mail-directory','task','thread-e','env','bound','directory-knowledge'),
	 ('dir-f','mail-directory','task','thread-f','env','bound','directory-knowledge'),
	 ('dir-g','mail-directory','task','thread-g','env','bound','directory-knowledge'),
	 ('dir-other','mail-directory-other','task','thread-other','env','bound','directory-other-package')`)
	require.NoError(t, err)
	grant, err := s.GrantMailBridge(ctx, "env", "apikey:directory", "admin")
	require.NoError(t, err)
	revokedGrant, err := s.GrantMailBridge(ctx, "env", "apikey:revoked-directory", "admin")
	require.NoError(t, err)
	scope := storage.MailScope{EnvironmentID: "env", ThreadID: "thread-a", ProviderSessionID: "session-a", ProviderInstanceID: "codex"}
	for _, suffix := range []string{"b", "c", "d", "e", "f"} {
		contactScope := scope
		contactScope.ThreadID, contactScope.ProviderSessionID = "thread-"+suffix, "session-"+suffix
		grantID := grant.ID
		if suffix == "e" {
			grantID = revokedGrant.ID
		}
		binding, err := s.ApproveMailBinding(ctx, grantID, "dir-"+suffix, contactScope, "admin")
		require.NoError(t, err)
		if suffix == "d" {
			_, err = s.RevokeMailBinding(ctx, binding.ID, "admin")
			require.NoError(t, err)
		}
	}
	_, err = s.RevokeMailGrant(ctx, revokedGrant.ID, "admin")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='prepared' WHERE id='dir-f'`)
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "mail-directory-other")
	require.NoError(t, err)
	otherGrant, err := other.GrantMailBridge(ctx, "env", "apikey:directory", "admin")
	require.NoError(t, err)
	otherScope := scope
	otherScope.ThreadID, otherScope.ProviderSessionID = "thread-other", "session-other"
	_, err = other.ApproveMailBinding(ctx, otherGrant.ID, "dir-other", otherScope, "admin")
	require.NoError(t, err)
	actorContext := auth.WithIdentity(ctx, &auth.Identity{UserID: "bridge", Subject: "apikey:directory", Source: "apikey", EffectiveRole: auth.RoleReader})
	directory := func(limit int, cursor, role string) storage.MailDirectoryPage {
		t.Helper()
		body, err := json.Marshal(map[string]any{"scope": scope, "limit": limit, "cursor": cursor, "assignment_role": role})
		require.NoError(t, err)
		result, err := ExecuteLocalWorkbenchMail(actorContext, s, "mail-directory", "directory", strings.NewReader(string(body)))
		require.NoError(t, err)
		return result.(storage.MailDirectoryPage)
	}
	page := directory(50, "", "")
	require.Len(t, page.Contacts, 3)
	require.Equal(t, "dir-a", page.Contacts[0].RunID)
	require.Equal(t, "dir-b", page.Contacts[1].RunID)
	require.Equal(t, "dir-c", page.Contacts[2].RunID)
	require.Nil(t, page.Contacts[0].AssignmentRole)
	require.Nil(t, page.Contacts[1].AssignmentRole)
	require.Equal(t, "knowledge", *page.Contacts[2].AssignmentRole)
	require.Equal(t, "task", page.Contacts[2].TaskSlug)
	first := directory(1, "", "")
	require.Equal(t, "dir-a", first.NextCursor)
	second := directory(1, first.NextCursor, "")
	require.Equal(t, "dir-b", second.Contacts[0].RunID)
	require.Equal(t, "dir-b", second.NextCursor)
	last := directory(1, second.NextCursor, "")
	require.Equal(t, "dir-c", last.Contacts[0].RunID)
	require.Empty(t, last.NextCursor)
	filtered := directory(1, "", "knowledge")
	require.Len(t, filtered.Contacts, 1)
	require.Equal(t, "dir-c", filtered.Contacts[0].RunID, "filter by recorded role before applying the limit")
	require.Empty(t, filtered.NextCursor)
	require.Empty(t, directory(1, "", "manager").Contacts)
	var unregistered int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM mail_bindings WHERE project_slug='mail-directory' AND run_id='dir-g'`).Scan(&unregistered))
	require.Zero(t, unregistered, "directory must not enroll other contacts")
	request, err := json.Marshal(map[string]any{"scope": scope, "limit": 50})
	require.NoError(t, err)
	_, err = ExecuteLocalWorkbenchMail(actorContext, s, "mail-directory-other", "directory", strings.NewReader(string(request)))
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = ExecuteWorkbenchMail(actorContext, s, "mail-directory", "directory", strings.NewReader(string(request)))
	require.ErrorIs(t, err, storage.ErrMailInvalid, "the directory is not an HTTP mailbox operation")
	_, err = s.MailDirectory(ctx, 0, "", "")
	require.ErrorIs(t, err, storage.ErrMailInvalid)
	_, err = s.RevokeMailGrant(ctx, grant.ID, "admin")
	require.NoError(t, err)
	_, err = ExecuteLocalWorkbenchMail(actorContext, s, "mail-directory", "directory", strings.NewReader(string(request)))
	require.ErrorIs(t, err, storage.ErrMailForbidden, "local caller enrollment must preserve revocation")
}

func TestLocalMailHandoffPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("mail-handoff"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-handoff','task');
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug,thread_ref,environment_id,state) VALUES
	 ('handoff-a','mail-handoff','task','thread-a','env','bound'),
	 ('handoff-b','mail-handoff','task','thread-b','env','bound'),
	 ('handoff-c','mail-handoff','task','thread-c','env','bound'),
	 ('handoff-unregistered','mail-handoff','task','thread-unregistered','env','bound')`)
	require.NoError(t, err)
	grant, err := s.GrantMailBridge(ctx, "env", "apikey:handoff", "admin")
	require.NoError(t, err)
	scopes := map[string]storage.MailScope{}
	for _, actor := range []string{"a", "b", "c"} {
		scope := storage.MailScope{EnvironmentID: "env", ThreadID: "thread-" + actor, ProviderSessionID: "session-" + actor, ProviderInstanceID: "codex"}
		scopes[actor] = scope
		_, err := s.ApproveMailBinding(ctx, grant.ID, "handoff-"+actor, scope, "admin")
		require.NoError(t, err)
	}
	actorCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: "bridge", Subject: "apikey:handoff", Source: "apikey", EffectiveRole: auth.RoleReader})
	base := storage.SendMailRequest{TaskSlug: "task", Subject: "Original subject", Body: "Original raw body", RecipientRunIDs: []string{"handoff-b"}, IdempotencyKey: "original"}
	first, err := s.SendMail(ctx, "handoff-a", &base)
	require.NoError(t, err)
	require.Equal(t, "handoff-a", first.Thread.OwnerRun)
	require.Nil(t, first.Message.HandoffToRun)
	_, err = s.AcknowledgeMail(ctx, "handoff-b", first.Message.ID)
	require.NoError(t, err)
	owned := func(actor string, limit int, cursor string) map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"scope": scopes[actor], "limit": limit, "cursor": cursor})
		require.NoError(t, err)
		result, err := ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "owned", strings.NewReader(string(body)))
		require.NoError(t, err)
		return result.(map[string]any)
	}
	ownedBefore, err := s.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Len(t, owned("a", 50, "")["threads"], 1, "fully acknowledged open thread remains discoverable by owner")
	require.Empty(t, owned("b", 50, "")["threads"], "recipient is not owner")
	ownedAfter, err := s.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Equal(t, ownedBefore, ownedAfter, "owned discovery changes no mail or receipts")
	_, err = ExecuteWorkbenchMail(actorCtx, s, "mail-handoff", "owned", strings.NewReader(`{}`))
	require.ErrorIs(t, err, storage.ErrMailInvalid, "owned discovery is IPC only")
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "owned", strings.NewReader(`{"owner_run":"handoff-b","limit":50}`))
	require.ErrorIs(t, err, storage.ErrMailInvalid, "caller cannot select another owner")
	request := func(actor, recipient, key string) string {
		body, err := json.Marshal(map[string]any{"scope": scopes[actor], "mail_thread_id": first.Thread.ID, "recipient_run_id": recipient, "body": "  Exact handoff reason\n", "idempotency_key": key})
		require.NoError(t, err)
		return string(body)
	}
	_, err = ExecuteWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", "handoff-b", "handoff")))
	require.ErrorIs(t, err, storage.ErrMailInvalid, "handoff is IPC only")
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "send", strings.NewReader(`{"handoff_to_run":"handoff-b"}`))
	require.ErrorIs(t, err, storage.ErrMailInvalid, "send cannot smuggle transfer metadata")
	for _, recipient := range []string{"handoff-a", "handoff-unregistered", "missing"} {
		_, err := ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", recipient, "invalid-"+recipient)))
		require.Error(t, err)
	}
	for _, update := range []string{"state='prepared'", "thread_ref='changed'", "environment_id='other-env'"} {
		_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET `+update+` WHERE project_slug='mail-handoff' AND id='handoff-c'`)
		require.NoError(t, err)
		_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", "handoff-c", "ineligible")))
		require.ErrorIs(t, err, storage.ErrMailForbidden)
		_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='bound',thread_ref='thread-c',environment_id='env' WHERE project_slug='mail-handoff' AND id='handoff-c'`)
		require.NoError(t, err)
	}
	_, err = s.Pool().Exec(ctx, `UPDATE mail_bindings SET revoked_at=now(),revoked_by='fixture' WHERE project_slug='mail-handoff' AND run_id='handoff-c'`)
	require.NoError(t, err)
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", "handoff-c", "revoked")))
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = s.Pool().Exec(ctx, `UPDATE mail_bindings SET revoked_at=NULL,revoked_by=NULL WHERE project_slug='mail-handoff' AND run_id='handoff-c'`)
	require.NoError(t, err)
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("b", "handoff-c", "not-owner")))
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	before, err := s.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Len(t, before.Items, 1, "failed handoffs must not leave mail")
	handoff := base
	handoff.ThreadID, handoff.IdempotencyKey, handoff.HandoffToRun = first.Thread.ID, "rolled-back", "handoff-b"
	rollback := errors.New("rollback fixture")
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, err := s.SendMail(txCtx, "handoff-a", &handoff)
		require.NoError(t, err)
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	after, err := s.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Equal(t, before, after, "rollback preserves owner, messages and receipts")
	handoff.HandoffToRun, handoff.IdempotencyKey = "", "same-key"
	ordinary, err := s.SendMail(ctx, "handoff-a", &handoff)
	require.NoError(t, err)
	handoff.HandoffToRun = "handoff-b"
	_, err = s.SendMail(ctx, "handoff-a", &handoff)
	require.ErrorIs(t, err, storage.ErrMailConflict, "ordinary send key cannot acquire handoff intent")
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", "handoff-b", "a-to-b")))
	require.NoError(t, err)
	require.Empty(t, owned("a", 50, "")["threads"])
	require.Len(t, owned("b", 50, "")["threads"], 1, "handoff transfers discovery with ownership")
	page, err := s.ReadMailThread(ctx, "handoff-b", first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Equal(t, "handoff-a", page.Thread.OpenedByRun)
	require.Equal(t, "handoff-b", page.Thread.OwnerRun)
	require.Equal(t, "Original subject", page.Thread.Subject)
	require.Len(t, page.Items, 3)
	require.Equal(t, first.Message, page.Items[0].Message)
	require.NotNil(t, page.Items[0].Receipt.AcknowledgedAt)
	require.Equal(t, ordinary.Message, page.Items[1].Message)
	require.Equal(t, "handoff-b", *page.Items[2].Message.HandoffToRun)
	require.Equal(t, "  Exact handoff reason\n", page.Items[2].Message.Body)
	require.Equal(t, "handoff-a", page.Items[2].Message.SenderRun)
	_, err = s.Pool().Exec(ctx, `UPDATE mail_threads SET closed_at=now(),closed_by_run='handoff-a',closure_note='invalid former owner' WHERE project_slug='mail-handoff' AND id=$1`, first.Thread.ID)
	require.ErrorContains(t, err, "mail_threads_check", "database must reject closing by former owner")
	_, err = s.CloseMailThread(ctx, "handoff-a", first.Thread.ID, "not owner")
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("b", "handoff-c", "b-to-c")))
	require.NoError(t, err)
	successorPage, err := s.ReadMailThread(ctx, "handoff-c", first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Len(t, successorPage.Items, 4)
	require.Equal(t, first.Message, successorPage.Items[0].Message)
	require.Nil(t, successorPage.Items[0].Receipt, "handoff must not copy old recipient receipts")
	successorInbox, err := s.Inbox(ctx, "handoff-c", 50, "")
	require.NoError(t, err)
	require.Len(t, successorInbox.Items, 1, "only the actual handoff is newly delivered")
	replayed, err := ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("a", "handoff-b", "a-to-b")))
	require.NoError(t, err)
	encoded, err := json.Marshal(replayed)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"owner_run":"handoff-c"`)
	require.Contains(t, string(encoded), `"handoff_to_run":"handoff-b"`)
	require.Contains(t, string(encoded), page.Items[2].Message.ID)
	_, err = s.CloseMailThread(ctx, "handoff-b", first.Thread.ID, "former owner")
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	closed, err := s.CloseMailThread(ctx, "handoff-c", first.Thread.ID, "resolved")
	require.NoError(t, err)
	require.Equal(t, "handoff-c", *closed.ClosedByRun)
	require.Empty(t, owned("c", 50, "")["threads"], "closed threads are excluded")
	_, err = ExecuteLocalWorkbenchMail(actorCtx, s, "mail-handoff", "handoff", strings.NewReader(request("c", "handoff-b", "closed")))
	require.ErrorIs(t, err, storage.ErrMailClosed)
	final, err := s.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Len(t, final.Items, 4, "replay and close add no messages")
	require.Equal(t, "handoff-c", final.Thread.OwnerRun)
	require.Equal(t, "handoff-b", *final.Items[2].Message.HandoffToRun)
	require.NotNil(t, final.Items[0].Receipts[0].AcknowledgedAt)

	base.IdempotencyKey = "race-thread"
	race, err := s.SendMail(ctx, "handoff-a", &base)
	require.NoError(t, err)
	handoff.ThreadID, handoff.IdempotencyKey = race.Thread.ID, "race-handoff"
	var handoffErr, closeErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, handoffErr = s.SendMail(ctx, "handoff-a", &handoff) }()
	go func() {
		defer wg.Done()
		_, closeErr = s.CloseMailThread(ctx, "handoff-a", race.Thread.ID, "race close")
	}()
	wg.Wait()
	if handoffErr == nil {
		require.ErrorIs(t, closeErr, storage.ErrMailForbidden)
	} else {
		require.ErrorIs(t, handoffErr, storage.ErrMailClosed)
		require.NoError(t, closeErr)
	}
	base.IdempotencyKey = "owned-older"
	older, err := s.SendMail(ctx, "handoff-a", &base)
	require.NoError(t, err)
	base.IdempotencyKey = "owned-newer"
	newer, err := s.SendMail(ctx, "handoff-a", &base)
	require.NoError(t, err)
	firstOwned, err := s.OwnedMailThreads(ctx, "handoff-a", 1, "")
	require.NoError(t, err)
	require.Equal(t, []storage.MailThread{newer.Thread}, firstOwned.Threads)
	require.Equal(t, newer.Thread.ID, firstOwned.NextCursor)
	lastOwned, err := s.OwnedMailThreads(ctx, "handoff-a", 1, firstOwned.NextCursor)
	require.NoError(t, err)
	require.Equal(t, []storage.MailThread{older.Thread}, lastOwned.Threads)
	require.Empty(t, lastOwned.NextCursor)
	_, err = s.EnsureProject(ctx, "mail-handoff-other")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-handoff-other','task');
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug) VALUES ('foreign-owner','mail-handoff-other','task')`)
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "mail-handoff-other")
	require.NoError(t, err)
	foreignOwned, err := other.OwnedMailThreads(ctx, "foreign-owner", 50, "")
	require.NoError(t, err)
	require.Empty(t, foreignOwned.Threads, "another project cannot see owned threads")
	_, err = other.OwnedMailThreads(ctx, "handoff-a", 50, "")
	require.ErrorIs(t, err, storage.ErrRunBindingNotFound)
}

func TestLocalMailTakeoverPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("mail-takeover"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-takeover','task'),('mail-takeover','successor-task');
	 INSERT INTO context_packages(id,project_slug,task_spec_slug,body) VALUES ('takeover-pm','mail-takeover','task','{"dispatch_target":{"assignmentRole":"manager"}}');
	 INSERT INTO run_bindings(id,project_slug,task_spec_slug,thread_ref,environment_id,state,package_id) VALUES
	 ('takeover-a','mail-takeover','task','thread-a','env','bound',''),
	 ('takeover-b','mail-takeover','successor-task','thread-b','env','bound',''),
	 ('takeover-c','mail-takeover','task','thread-c','env','bound',''),
	 ('takeover-pm','mail-takeover','task','thread-pm','env','bound','takeover-pm'),
	 ('takeover-unregistered','mail-takeover','task','thread-unregistered','env','bound','')`)
	require.NoError(t, err)
	grant, err := s.GrantMailBridge(ctx, "env", "apikey:takeover", "admin")
	require.NoError(t, err)
	scopes := map[string]storage.MailScope{}
	for _, actor := range []string{"a", "b", "c", "pm"} {
		scope := storage.MailScope{EnvironmentID: "env", ThreadID: "thread-" + actor, ProviderSessionID: "session-" + actor, ProviderInstanceID: "codex"}
		scopes[actor] = scope
		_, err := s.ApproveMailBinding(ctx, grant.ID, "takeover-"+actor, scope, "admin")
		require.NoError(t, err)
	}
	agentCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: "actual-bridge-user", Subject: "apikey:takeover", Source: "apikey", UserKind: storage.KindServiceAccount, EffectiveRole: auth.RoleReader})
	humanCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: "actual-human", Subject: "session:human", Source: "session", UserKind: storage.KindHuman, EffectiveRole: auth.RoleAdmin})
	base := storage.SendMailRequest{TaskSlug: "task", Subject: "Preserved subject", Body: "Original evidence", RecipientRunIDs: []string{"takeover-c"}, IdempotencyKey: "first"}
	private := base
	private.IdempotencyKey = "private-source"
	source, err := s.SendMail(ctx, "takeover-a", &private)
	require.NoError(t, err)
	base.References = []storage.MailReferenceRequest{{MessageID: source.Message.ID, Kind: "context"}}
	first, err := s.SendMail(ctx, "takeover-a", &base)
	require.NoError(t, err)
	_, err = s.AcknowledgeMail(ctx, "takeover-c", first.Message.ID)
	require.NoError(t, err)
	call := func(actor, op string, fields map[string]any) (any, error) {
		t.Helper()
		fields["scope"] = scopes[actor]
		body, err := json.Marshal(fields)
		require.NoError(t, err)
		return ExecuteLocalWorkbenchMail(agentCtx, s, "mail-takeover", op, strings.NewReader(string(body)))
	}
	req := storage.TakeoverMailRequest{ThreadID: first.Thread.ID, RecipientRunID: "takeover-b", Body: "  Actual PM takeover reason\n", IdempotencyKey: "pm-key"}
	_, err = s.TakeoverMail(agentCtx, req, nil)
	require.ErrorIs(t, err, storage.ErrMailForbidden, "service account cannot impersonate human")
	bScope, pmScope := scopes["b"], scopes["pm"]
	_, err = s.TakeoverMail(agentCtx, req, &bScope)
	require.Error(t, err)
	_, err = s.TakeoverMail(agentCtx, req, &pmScope)
	require.ErrorIs(t, err, storage.ErrMailConflict, "bound owner cannot be seized")
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='handed_off' WHERE project_slug='mail-takeover' AND id='takeover-a'`)
	require.NoError(t, err)
	_, err = call("a", "owned", map[string]any{"limit": 50})
	require.ErrorIs(t, err, storage.ErrMailForbidden, "takeover must not reactivate retired mailbox")
	_, err = call("b", "retired", map[string]any{"limit": 50})
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	discovery, err := call("pm", "retired", map[string]any{"limit": 1})
	require.NoError(t, err)
	require.Len(t, discovery.(map[string]any)["threads"], 1)
	_, err = call("pm", "thread", map[string]any{"mail_thread_id": first.Thread.ID, "limit": 50})
	require.ErrorIs(t, err, storage.ErrMailForbidden, "PM discovery is not history membership")
	bad := req
	bad.RecipientRunID = "takeover-unregistered"
	_, err = s.TakeoverMail(humanCtx, bad, nil)
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	before, err := s.InspectMailThreads(ctx, "", "all", 50, "", false)
	require.NoError(t, err)
	rollback := errors.New("rollback takeover fixture")
	err = s.RunInTransaction(humanCtx, func(txCtx context.Context) error {
		_, err := s.TakeoverMail(txCtx, req, nil)
		require.NoError(t, err)
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	rolledBack, err := s.InspectMailThreads(ctx, "", "all", 50, "", false)
	require.NoError(t, err)
	require.Equal(t, before, rolledBack)
	noHistory, err := s.MailOwnerHistory(ctx, "", first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Empty(t, noHistory.Events, "rollback leaves neither new owner nor audit event")
	wire, err := call("pm", "takeover", map[string]any{"mail_thread_id": req.ThreadID, "recipient_run_id": req.RecipientRunID, "body": req.Body, "idempotency_key": req.IdempotencyKey})
	require.NoError(t, err)
	taken, err := s.TakeoverMail(agentCtx, req, &pmScope)
	require.NoError(t, err)
	require.Equal(t, taken.Event, wire.(map[string]any)["event"])
	changed := req
	changed.Body = "changed reason"
	_, err = s.TakeoverMail(agentCtx, changed, &pmScope)
	require.ErrorIs(t, err, storage.ErrMailConflict)
	require.Equal(t, "takeover-a", taken.Thread.OpenedByRun)
	require.Equal(t, "takeover-b", taken.Thread.OwnerRun)
	require.Equal(t, "actual-bridge-user", taken.Event.ActorUserID)
	require.Equal(t, "takeover-pm", *taken.Event.ActorRunID)
	require.Equal(t, "agent", taken.Event.ActorKind)
	require.Equal(t, req.Body, taken.Event.Reason)
	page, err := s.ReadMailThread(ctx, "takeover-b", first.Thread.ID, 50, "")
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, first.Message, page.Items[0].Message)
	require.Nil(t, page.Items[0].Receipt)
	inbox, err := s.Inbox(ctx, "takeover-b", 50, "")
	require.NoError(t, err)
	require.Empty(t, inbox.Items, "takeover creates no fake notification")
	owned, err := s.OwnedMailThreads(ctx, "takeover-b", 50, "")
	require.NoError(t, err)
	require.Len(t, owned.Threads, 1)
	after, err := s.InspectMailThreads(ctx, "", "all", 50, "", false)
	require.NoError(t, err)
	require.Equal(t, before.Threads[0].MessageCount, after.Threads[0].MessageCount)
	require.Equal(t, before.Threads[0].PendingAckCount, after.Threads[0].PendingAckCount)
	require.Equal(t, before.Threads[0].NodeLinks, after.Threads[0].NodeLinks)
	require.Contains(t, after.Threads[0].ParticipantRunIDs, "takeover-b")
	require.NotContains(t, after.Threads[0].ParticipantRunIDs, "takeover-pm", "acting PM is not a new member")
	filtered, err := s.InspectMailThreads(ctx, "successor-task", "open", 50, "", false)
	require.NoError(t, err)
	require.Len(t, filtered.Threads, 1)
	handoff := base
	handoff.ThreadID, handoff.IdempotencyKey, handoff.HandoffToRun = first.Thread.ID, "b-to-c", "takeover-c"
	handoff.References = nil
	_, err = s.SendMail(ctx, "takeover-b", &handoff)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-takeover','owner-root');
	 INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES ('mail-takeover','owner-root','successor-task','COMPOSES')`)
	require.NoError(t, err)
	ownerSubmap, err := s.InspectMailThreads(ctx, "owner-root", "open", 50, "", true)
	require.NoError(t, err)
	require.Len(t, ownerSubmap.Threads, 1, "historical takeover recipient participates in descendant scope after losing ownership")
	require.Equal(t, first.Thread.ID, ownerSubmap.Threads[0].ID)
	replayed, err := s.TakeoverMail(agentCtx, req, &pmScope)
	require.NoError(t, err)
	require.Equal(t, taken.Event, replayed.Event)
	require.Equal(t, "takeover-c", replayed.Thread.OwnerRun, "replay cannot rewind subsequent handoff")
	_, err = s.ReadMailThread(ctx, "takeover-b", first.Thread.ID, 50, "")
	require.NoError(t, err, "historical takeover target remains member")
	_, err = s.CloseMailThread(ctx, "takeover-b", first.Thread.ID, "former owner")
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	handoff.IdempotencyKey = "former-owner-transfer"
	_, err = s.SendMail(ctx, "takeover-b", &handoff)
	require.ErrorIs(t, err, storage.ErrMailForbidden, "historical membership is not transfer authority")
	reply := handoff
	reply.HandoffToRun, reply.IdempotencyKey = "", "former-owner-reply"
	_, err = s.SendMail(ctx, "takeover-b", &reply)
	require.NoError(t, err, "valid historical member can reply")
	forward := base
	forward.IdempotencyKey, forward.References = "b-forward", []storage.MailReferenceRequest{{MessageID: first.Message.ID, Kind: "forward"}}
	_, err = s.SendMail(ctx, "takeover-b", &forward)
	require.NoError(t, err, "former target may still cite its legitimately readable history")
	forward.IdempotencyKey, forward.References = "b-forward-nested-source", []storage.MailReferenceRequest{{MessageID: source.Message.ID, Kind: "forward"}}
	_, err = s.SendMail(ctx, "takeover-b", &forward)
	require.NoError(t, err, "historical takeover membership permits re-sharing an explicitly received source quote")
	_, err = s.ReadMailThread(ctx, "takeover-b", source.Thread.ID, 50, "")
	require.ErrorIs(t, err, storage.ErrMailForbidden, "a source quote does not grant its whole original thread")
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='completed' WHERE project_slug='mail-takeover' AND id='takeover-c'`)
	require.NoError(t, err)
	humanReq := req
	humanReq.Body, humanReq.IdempotencyKey = "Human designates a successor", "human-key"
	body, err := json.Marshal(humanReq)
	require.NoError(t, err)
	raw, err := ExecuteWorkbenchCommand(humanCtx, s, "takeover-mail", "mail-takeover", "", "untrusted actor argument", body)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"actor_user_id":"actual-human"`)
	require.Contains(t, string(raw), `"actor_run_id":null`)
	_, err = call("c", "thread", map[string]any{"mail_thread_id": first.Thread.ID, "limit": 50})
	require.ErrorIs(t, err, storage.ErrMailForbidden, "retained membership never reactivates a completed run")
	history, err := s.MailOwnerHistory(ctx, "takeover-b", first.Thread.ID, 1, "")
	require.NoError(t, err)
	require.Len(t, history.Events, 1)
	require.Equal(t, "human", history.Events[0].ActorKind)
	require.NotEmpty(t, history.NextCursor)
	history, err = s.MailOwnerHistory(ctx, "takeover-b", first.Thread.ID, 1, history.NextCursor)
	require.NoError(t, err)
	require.Equal(t, taken.Event, history.Events[0])
	require.Empty(t, history.NextCursor)
	_, err = s.MailOwnerHistory(ctx, "takeover-pm", first.Thread.ID, 50, "")
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	_, err = s.CloseMailThread(ctx, "takeover-b", first.Thread.ID, "resolved")
	require.NoError(t, err)
	humanReq.IdempotencyKey = "closed"
	_, err = s.TakeoverMail(humanCtx, humanReq, nil)
	require.ErrorIs(t, err, storage.ErrMailClosed)
	_, err = s.EnsureProject(ctx, "mail-takeover-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "mail-takeover-other")
	require.NoError(t, err)
	_, err = other.TakeoverMail(humanCtx, req, nil)
	require.ErrorIs(t, err, storage.ErrMailNotFound)
	_, err = other.MailOwnerHistory(ctx, "", first.Thread.ID, 50, "")
	require.ErrorIs(t, err, storage.ErrMailNotFound)
	retiredOther, err := other.RetiredMailThreads(ctx, 50, "")
	require.NoError(t, err)
	require.Empty(t, retiredOther.Threads)
}
