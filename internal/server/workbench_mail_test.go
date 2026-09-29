// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

type mailCheckResolver struct {
	auth.Resolver
	identities map[string]*auth.Identity
}

func TestMailReferenceWire(t *testing.T) {
	for _, field := range []string{"body", "sender_run_id", "subject"} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"references":[{"message_id":"source","kind":"forward","`+field+`":"forged"}]}`))
		w := httptest.NewRecorder()
		var body struct {
			References []storage.MailReferenceRequest `json:"references"`
		}
		if decodeMailBody(w, r, &body) || w.Code != http.StatusBadRequest {
			t.Fatalf("accepted caller-authored source field %s", field)
		}
	}
	message := storage.MailMessage{}
	data, setupErr := json.Marshal(mailMessageJSON(&message))
	if message.References != nil {
		t.Fatal("wire projection mutated the stored message references")
	}
	if setupErr != nil || !strings.Contains(string(data), `"references":[]`) {
		t.Fatalf("empty references wire shape: %s %v", data, setupErr)
	}
	data, setupErr = json.Marshal(mailMessageJSON(&storage.MailMessage{References: []storage.MailReference{{MessageID: "source", Kind: "reply", ThreadID: "thread", TaskSlug: "task", SenderRunID: "sender", Subject: "subject", Body: "raw source"}}}))
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	var references []map[string]json.RawMessage
	if err := json.Unmarshal(result["references"], &references); err != nil || len(references) != 1 {
		t.Fatalf("reference JSON: %s %v", data, err)
	}
	for _, key := range []string{"message_id", "kind", "thread_id", "task_slug", "sender_run_id", "subject", "body", "created_at"} {
		if _, ok := references[0][key]; !ok {
			t.Errorf("missing source field %s", key)
		}
	}
}

func (r mailCheckResolver) Resolve(_ context.Context, token string) (*auth.Identity, error) {
	id, ok := r.identities[token]
	if !ok {
		return nil, auth.ErrUnauthenticated
	}
	return id, nil
}

type mailCheckForbid struct{}

func (mailCheckForbid) Name() string { return "mail-check-forbid" }
func (mailCheckForbid) Load(context.Context) ([]auth.PolicyDocument, error) {
	return []auth.PolicyDocument{{Source: "mail-check-forbid.cedar", Text: `forbid(principal, action == SpecGraph::Action::"mail.write", resource);`}}, nil
}

func TestWorkbenchMailAPI(t *testing.T) {
	configPath := os.Getenv("SPECGRAPH_MAIL_CHECK_CONFIG")
	if configPath == "" {
		t.Skip("explicit isolated local PostgreSQL check only")
	}
	ctx := context.Background()
	cfg, setupErr := config.LoadGlobalExplicit(configPath)
	if setupErr != nil {
		t.Fatal("cannot load explicit database configuration")
	}
	u, setupErr := url.Parse(cfg.Server.Postgres.URL)
	if setupErr != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("expected PostgreSQL URL")
	}
	q := u.Query()
	q.Del("database")
	q.Del("dbname")
	u.RawQuery = q.Encode()
	u.Path = "/postgres"
	u.RawPath = ""
	admin, setupErr := pgx.Connect(ctx, u.String())
	if setupErr != nil {
		t.Fatal("cannot connect to maintenance database")
	}
	t.Cleanup(func() { _ = admin.Close(ctx) })
	const database = "specgraph_workbench_mail_api_check"
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("refusing preexisting isolated check database")
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE specgraph_workbench_mail_api_check`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, `DROP DATABASE specgraph_workbench_mail_api_check`); err != nil {
			t.Error(err)
			return
		}
		if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, database).Scan(&exists); err != nil || exists {
			t.Error("isolated database cleanup verification failed")
		}
	})
	u.Path = "/" + database
	parsed, setupErr := pgx.ParseConfig(u.String())
	if setupErr != nil || parsed.Database != database {
		t.Fatal("isolated target rewrite failed")
	}
	s, setupErr := postgres.New(ctx, u.String(), postgres.WithProject("mail-check"))
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })
	_, setupErr = s.Pool().Exec(ctx, `INSERT INTO projects(slug) VALUES('other-check');
		INSERT INTO specs(project_slug,slug) VALUES('mail-check','task-a'),('mail-check','task-b'),('other-check','task-a');
		INSERT INTO run_bindings(id,project_slug,task_spec_slug,thread_ref,state) VALUES
		('run-a','mail-check','task-a','thread-a','bound'),('run-b','mail-check','task-b','thread-b','bound'),
		('run-unbound','mail-check','task-a','thread-unbound','prepared'),('run-other','other-check','task-a','thread-other','bound')`)
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	ids := map[string]*auth.Identity{
		"admin":    {UserID: "human-admin", Subject: "oidc:human", Source: "oidc", Role: auth.RoleAdmin, EffectiveRole: auth.RoleAdmin},
		"bridge":   {UserID: "bridge-user", Subject: "apikey:bridge-key", Source: "apikey", Role: auth.RoleReader, EffectiveRole: auth.RoleReader},
		"ordinary": {UserID: "ordinary", Subject: "apikey:ordinary-key", Source: "apikey", EffectiveRole: auth.RoleReader},
		"oidc":     {UserID: "bridge-user", Subject: "apikey:bridge-key", Source: "oidc", EffectiveRole: auth.RoleReader},
		"writer":   {UserID: "bridge-user", Subject: "apikey:bridge-key", Source: "apikey", EffectiveRole: auth.RoleWriter},
	}
	resolver := mailCheckResolver{identities: ids}
	engine, setupErr := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	authorizer := auth.NewCedarAuthorizer(engine)
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, s, resolver, authorizer)
	crossSite := httptest.NewRequest("POST", "http://workbench.local/loop/mail/send", strings.NewReader(`{}`))
	crossSite.Header.Set("Authorization", "Bearer bridge")
	crossSite.Header.Set("Sec-Fetch-Site", "cross-site")
	crossSiteResult := httptest.NewRecorder()
	mux.ServeHTTP(crossSiteResult, crossSite)
	if crossSiteResult.Code != http.StatusForbidden {
		t.Fatal("cross-origin mailbox write admitted")
	}
	request := func(handler http.Handler, token, project, path, body string, want int) map[string]any {
		t.Helper()
		r := httptest.NewRequest("POST", "http://workbench.local"+path, strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("X-Specgraph-Project", project)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s as %s: got %d want %d: %s", path, token, w.Code, want, w.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	call := func(token, path string, body any, want int) map[string]any {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return request(mux, token, "mail-check", path, string(b), want)
	}
	scopeA := storage.MailScope{EnvironmentID: "env-1", ThreadID: "thread-a", ProviderSessionID: "session-a", ProviderInstanceID: "provider-1"}
	scopeB := storage.MailScope{EnvironmentID: "env-1", ThreadID: "thread-b", ProviderSessionID: "session-b", ProviderInstanceID: "provider-1"}
	inbox := map[string]any{"scope": scopeA, "limit": 10}
	call("bridge", "/loop/mail/inbox", inbox, 403)
	call("bridge", "/loop/mail/admin/grants", map[string]any{"environment_id": "env-1", "bridge_subject": "apikey:bridge-key"}, 403)
	grantReq := map[string]any{"environment_id": "env-1", "bridge_subject": "apikey:bridge-key"}
	grant := call("admin", "/loop/mail/admin/grants", grantReq, 200)
	if grant["granted_by"] != "human-admin" {
		t.Fatal("grant did not use authenticated user")
	}
	if call("admin", "/loop/mail/admin/grants", grantReq, 200)["id"] != grant["id"] {
		t.Fatal("active grant not idempotent")
	}
	bindA := map[string]any{"grant_id": grant["id"], "run_id": "run-a", "scope": scopeA}
	binding := call("admin", "/loop/mail/admin/bindings", bindA, 200)
	if binding["approved_by"] != "human-admin" {
		t.Fatal("binding did not use authenticated user")
	}
	if call("admin", "/loop/mail/admin/bindings", bindA, 200)["id"] != binding["id"] {
		t.Fatal("active binding not idempotent")
	}
	call("admin", "/loop/mail/admin/bindings", map[string]any{"grant_id": grant["id"], "run_id": "run-b", "scope": scopeB}, 200)
	badScope := scopeA
	badScope.ProviderSessionID = "changed"
	call("admin", "/loop/mail/admin/bindings", map[string]any{"grant_id": grant["id"], "run_id": "run-a", "scope": badScope}, 409)
	badScope = scopeA
	badScope.ThreadID = "thread-unbound"
	call("admin", "/loop/mail/admin/bindings", map[string]any{"grant_id": grant["id"], "run_id": "run-unbound", "scope": badScope}, 409)
	call("ordinary", "/loop/mail/admin/bindings", bindA, 403)
	call("bridge", "/loop/mail/inbox", inbox, 200)
	for _, token := range []string{"ordinary", "oidc", "writer", "admin"} {
		call(token, "/loop/mail/inbox", inbox, 403)
	}
	call("", "/loop/mail/inbox", inbox, 401)
	call("revoked-key", "/loop/mail/inbox", inbox, 401)
	for _, field := range []string{"environment", "thread", "session", "instance"} {
		scope := scopeA
		switch field {
		case "environment":
			scope.EnvironmentID = "old"
		case "thread":
			scope.ThreadID = "old"
		case "session":
			scope.ProviderSessionID = "old"
		case "instance":
			scope.ProviderInstanceID = "old"
		}
		call("bridge", "/loop/mail/inbox", map[string]any{"scope": scope, "limit": 10}, 403)
	}
	raw, _ := json.Marshal(inbox)
	request(mux, "bridge", "other-check", "/loop/mail/inbox", string(raw), 403)
	request(mux, "bridge", "missing-check", "/loop/mail/inbox", string(raw), 404)
	request(mux, "bridge", "", "/loop/mail/inbox", string(raw), 400)
	for _, body := range []string{`{`, string(raw) + ` {}`, `{"scope":{},"limit":10,"task_slug":"forged"}`, `{"scope":{},"limit":10,"sender_run":"run-b"}`} {
		request(mux, "bridge", "mail-check", "/loop/mail/inbox", body, 400)
	}
	request(mux, "bridge", "mail-check", "/loop/mail/send", `{"body":"`+strings.Repeat("x", 513<<10)+`"}`, 400)
	for _, field := range []string{"sender_run", "task_slug"} {
		call("bridge", "/loop/mail/send", map[string]any{"scope": scopeA, field: "forged"}, 400)
	}
	send := map[string]any{"scope": scopeA, "subject": "interface", "body": "request", "recipient_run_ids": []string{"run-b"}, "idempotency_key": "send-1"}
	sent := call("bridge", "/loop/mail/send", send, 200)
	thread := sent["thread"].(map[string]any)
	message := sent["message"].(map[string]any)
	if thread["task_slug"] != "task-a" || message["sender_run"] != "run-a" {
		t.Fatal("sender/task not derived from binding")
	}
	if call("bridge", "/loop/mail/send", send, 200)["message"].(map[string]any)["id"] != message["id"] {
		t.Fatal("send not idempotent")
	}
	page := call("bridge", "/loop/mail/inbox", map[string]any{"scope": scopeB, "limit": 10}, 200)
	if len(page["items"].([]any)) != 1 {
		t.Fatal("delivery absent")
	}
	receipt := call("bridge", "/loop/mail/read", map[string]any{"scope": scopeB, "message_id": message["id"]}, 200)
	if receipt["read_at"] == nil || receipt["acknowledged_at"] != nil {
		t.Fatal("read implied acknowledgement")
	}
	receipt = call("bridge", "/loop/mail/ack", map[string]any{"scope": scopeB, "message_id": message["id"]}, 200)
	if receipt["acknowledged_at"] == nil {
		t.Fatal("ack missing")
	}
	page = call("bridge", "/loop/mail/inbox", map[string]any{"scope": scopeB, "limit": 10}, 200)
	if len(page["items"].([]any)) != 0 {
		t.Fatal("ack still in inbox")
	}
	reply := call("bridge", "/loop/mail/send", map[string]any{"scope": scopeB, "mail_thread_id": thread["id"], "subject": "interface", "body": "reply from another task", "recipient_run_ids": []string{"run-a"}, "idempotency_key": "reply-1"}, 200)
	if reply["thread"].(map[string]any)["task_slug"] != "task-a" {
		t.Fatal("cross-node reply changed thread task")
	}
	page = call("bridge", "/loop/mail/thread", map[string]any{"scope": scopeB, "mail_thread_id": thread["id"], "limit": 10}, 200)
	if len(page["items"].([]any)) != 2 {
		t.Fatal("thread missing messages")
	}
	escaped := call("bridge", "/loop/mail/send", map[string]any{"scope": scopeA, "subject": "escaped", "body": "x" + strings.Repeat("\x01", 65535), "recipient_run_ids": []string{"run-b"}, "idempotency_key": "escaped-1"}, 200)
	if len(escaped["message"].(map[string]any)["body"].(string)) != 65536 {
		t.Fatal("escaped valid raw body truncated")
	}
	call("bridge", "/loop/mail/close", map[string]any{"scope": scopeB, "mail_thread_id": thread["id"], "resolution": "not opener"}, 403)
	closed := call("bridge", "/loop/mail/close", map[string]any{"scope": scopeA, "mail_thread_id": thread["id"], "resolution": "interface agreed"}, 200)
	if closed["closed_at"] == nil || closed["closed_by_run"] != "run-a" {
		t.Fatal("closure missing")
	}
	call("bridge", "/loop/mail/send", send, 200)
	send["mail_thread_id"] = thread["id"]
	send["idempotency_key"] = "closed-send"
	call("bridge", "/loop/mail/send", send, 409)
	delete(send, "mail_thread_id")
	send["recipient_run_ids"] = []string{"run-other"}
	send["idempotency_key"] = "wrong-recipient"
	call("bridge", "/loop/mail/send", send, 404)
	denyEngine, setupErr := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource(), mailCheckForbid{}}, auth.ActionNames())
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	denyMux := http.NewServeMux()
	RegisterWorkbenchLoop(denyMux, s, resolver, auth.NewCedarAuthorizer(denyEngine))
	request(denyMux, "bridge", "mail-check", "/loop/mail/send", `{}`, 403)
	decision, setupErr := authorizer.Authorize(ctx, ids["bridge"], "/specgraph.v1.SpecService/CreateSpec", nil)
	if setupErr != nil || decision.Allowed {
		t.Fatal("reader acquired spec.write")
	}
	// A current bound run is required on every operation, not just enrollment.
	if _, err := s.Pool().Exec(ctx, `UPDATE run_bindings SET state='completed' WHERE id='run-a'`); err != nil {
		t.Fatal(err)
	}
	call("bridge", "/loop/mail/inbox", inbox, 403)
	if _, err := s.Pool().Exec(ctx, `UPDATE run_bindings SET state='bound' WHERE id='run-a'`); err != nil {
		t.Fatal(err)
	}
	// Verify both revocations wait for the authorization+operation transaction.
	for _, kind := range []string{"binding", "grant"} {
		locked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- s.WithMailActor(ctx, "apikey:bridge-key", scopeA, func(context.Context, storage.MailActor) error { close(locked); <-release; return nil })
		}()
		select {
		case <-locked:
		case err := <-done:
			t.Fatalf("lock acquisition: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("lock acquisition timeout")
		}
		revoked := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var revokeErr error
			if kind == "binding" {
				_, revokeErr = s.RevokeMailBinding(waitCtx, binding["id"].(string), "human-admin")
			} else {
				_, revokeErr = s.RevokeMailGrant(waitCtx, grant["id"].(string), "human-admin")
			}
			revoked <- revokeErr
		}()
		blocked := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err := s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE mail_%')`).Scan(&blocked); err != nil {
				close(release)
				<-done
				<-revoked
				t.Fatal(err)
			}
			if blocked {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		close(release)
		operationErr := <-done
		setupErr = <-revoked
		if !blocked || setupErr != nil || operationErr != nil {
			t.Fatalf("revocation did not serialize: %v / %v", setupErr, operationErr)
		}
		call("bridge", "/loop/mail/inbox", inbox, 403)
		if kind == "binding" {
			call("admin", "/loop/mail/admin/bindings/"+binding["id"].(string)+"/revoke", map[string]any{}, 200)
			newBinding := call("admin", "/loop/mail/admin/bindings", bindA, 200)
			if newBinding["id"] == binding["id"] {
				t.Fatal("revoked binding reactivated")
			}
		}
	}
	call("admin", "/loop/mail/admin/grants/"+grant["id"].(string)+"/revoke", map[string]any{}, 200)
	call("bridge", "/loop/mail/inbox", inbox, 403)
	newGrant := call("admin", "/loop/mail/admin/grants", grantReq, 200)
	if newGrant["id"] == grant["id"] {
		t.Fatal("revoked grant reactivated")
	}
	call("bridge", "/loop/mail/inbox", inbox, 403)
	var grants, bindings int
	if err := s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM mail_grants),(SELECT count(*) FROM mail_bindings)`).Scan(&grants, &bindings); err != nil || grants != 2 || bindings != 3 {
		t.Fatal("audit history was lost")
	}
}
