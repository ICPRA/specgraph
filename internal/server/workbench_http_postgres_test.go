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

// The resolver supplies synthetic test identities; the actual middleware,
// Cedar policies, handlers, migrations and PostgreSQL writes are exercised.
// A dedicated database is created only if absent and dropped on every exit.
func TestWorkbenchHTTPPostgres(t *testing.T) {
	path := os.Getenv("SPECGRAPH_HTTP_TEST_CONFIG")
	if path == "" {
		t.Skip("SPECGRAPH_HTTP_TEST_CONFIG is not set")
	}
	cfg, setupErr := config.LoadGlobalExplicit(path)
	if setupErr != nil || cfg.Server.Postgres.URL == "" {
		t.Fatal("explicit test database configuration is required")
	}
	connection, setupErr := pgx.ParseConfig(cfg.Server.Postgres.URL)
	if setupErr != nil {
		t.Fatal("cannot parse test database configuration")
	}
	if connection.Host != "127.0.0.1" && connection.Host != "localhost" && connection.Host != "::1" {
		t.Fatal("only a loopback test database is allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, setupErr := pgx.ConnectConfig(ctx, connection)
	if setupErr != nil {
		t.Fatal("cannot connect to the local database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := admin.Close(cleanup); err != nil {
			t.Errorf("close control connection: %v", err)
		}
	}()
	const database = "specgraph_workbench_http_check"
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, database).Scan(&exists); err != nil || exists {
		t.Fatalf("refusing to reuse an existing or unverified test database: exists=%v err=%v", exists, err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE specgraph_workbench_http_check`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP DATABASE specgraph_workbench_http_check`); err != nil {
			t.Errorf("drop owned test database: %v", err)
			return
		}
		if err := admin.QueryRow(cleanup, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, database).Scan(&exists); err != nil || exists {
			t.Errorf("test database cleanup: exists=%v err=%v", exists, err)
		}
	}()
	testURL, setupErr := url.Parse(cfg.Server.Postgres.URL)
	if setupErr != nil || (testURL.Scheme != "postgres" && testURL.Scheme != "postgresql") {
		t.Fatal("the integration test requires a PostgreSQL URL")
	}
	testURL.Path, testURL.RawPath = "/"+database, ""
	query := testURL.Query()
	query.Del("database")
	query.Del("dbname")
	testURL.RawQuery = query.Encode()
	testConnection, setupErr := pgx.ParseConfig(testURL.String())
	if setupErr != nil || testConnection.Database != database || testConnection.Host != connection.Host {
		t.Fatal("isolated database configuration did not resolve exactly")
	}
	store, setupErr := postgres.New(ctx, testURL.String(), postgres.WithProject("alpha"))
	if setupErr != nil {
		t.Fatal("could not initialize the isolated test store")
	}
	defer store.Close(ctx)
	fixture, setupErr := pgx.ConnectConfig(ctx, testConnection)
	if setupErr != nil {
		t.Fatal("could not connect to the isolated fixture database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := fixture.Close(cleanup); err != nil {
			t.Errorf("close fixture connection: %v", err)
		}
	}()
	if _, err := store.EnsureProject(ctx, "beta"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Exec(ctx, `INSERT INTO run_bindings(id, project_slug, task_spec_slug) VALUES ('run-fixture', 'alpha', 'task-fixture')`); err != nil {
		t.Fatal(err)
	}
	engine, setupErr := auth.NewCedarEngine(ctx, []auth.PolicySource{auth.NewEmbeddedPolicySource()}, auth.ActionNames())
	if setupErr != nil {
		t.Fatal(setupErr)
	}
	resolver := &workbenchAuthResolver{}
	mux := http.NewServeMux()
	RegisterWorkbenchLoop(mux, store, resolver, auth.NewCedarAuthorizer(engine))
	request := func(project, route, body string, role auth.Role, want int) map[string]any {
		t.Helper()
		resolver.identity = &auth.Identity{UserID: "server-" + string(role), Subject: "fixture", EffectiveRole: role}
		r := httptest.NewRequest(http.MethodPost, route, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer fixture-only")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Specgraph-Project", project)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: status=%d body=%s", role, route, w.Code, w.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	if _, err := store.CreateSpec(ctx, "unapproved", "Unapproved fixture", "medium", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	request("alpha", "/loop/runs", `{"task_slug":"unapproved","workspace":"fixture"}`, auth.RoleWriter, 428)
	request("alpha", "/loop/runs", `{"task_slug":"missing","workspace":"fixture"}`, auth.RoleWriter, 404)
	var preparations int
	if err := fixture.QueryRow(ctx, `SELECT (SELECT count(*) FROM context_packages) + (SELECT count(*) FROM run_bindings WHERE id <> 'run-fixture')`).Scan(&preparations); err != nil || preparations != 0 {
		t.Fatalf("failed bundle created execution state: count=%d err=%v", preparations, err)
	}
	if claim, err := store.GetActiveClaim(ctx, "unapproved"); err != nil || claim != nil {
		t.Fatalf("failed bundle claimed task: claim=%v err=%v", claim, err)
	}
	if _, err := fixture.Exec(ctx, `UPDATE specs SET stage='approved' WHERE project_slug='alpha' AND slug='unapproved'`); err != nil {
		t.Fatal(err)
	}
	prepared := request("alpha", "/loop/runs", `{"task_slug":"unapproved","workspace":"fixture"}`, auth.RoleWriter, 200)
	if id, ok := prepared["binding_id"].(string); !ok || id == "" {
		t.Fatalf("approved task did not return a binding: %v", prepared)
	}
	if err := fixture.QueryRow(ctx, `SELECT count(*) FROM context_packages WHERE body ? 'bundle' AND NOT body ? 'bundle_error'`).Scan(&preparations); err != nil || preparations != 1 {
		t.Fatalf("approved task requires a real bundle: count=%d err=%v", preparations, err)
	}
	request("alpha", "/loop/runs/run-fixture/bind", `{"thread_ref":"real-reference-fixture"}`, auth.RoleReader, 403)
	request("alpha", "/loop/runs/run-fixture/bind", `{"thread_ref":"real-reference-fixture"}`, auth.RoleWriter, 200)
	request("alpha", "/loop/runs/run-fixture/bind", `{"thread_ref":"another-reference"}`, auth.RoleWriter, 409)
	request("missing", "/loop/runs/run-fixture/bind", `{"thread_ref":"real-reference-fixture"}`, auth.RoleWriter, 404)
	request("beta", "/loop/deliveries", `{"run_binding_id":"run-fixture","snapshot":{}}`, auth.RoleWriter, 404)
	response := request("alpha", "/loop/deliveries", `{"run_binding_id":"run-fixture","snapshot":{},"submitted_by":"forged-user"}`, auth.RoleWriter, 200)
	delivery, ok := response["delivery_id"].(string)
	if !ok || delivery == "" {
		t.Fatal("no persisted delivery ID returned")
	}
	var actor string
	if err := fixture.QueryRow(ctx, `SELECT submitted_by FROM deliveries WHERE id=$1`, delivery).Scan(&actor); err != nil || actor != "server-writer" {
		t.Fatalf("delivery attribution: actor=%q err=%v", actor, err)
	}
	acceptRoute := "/loop/deliveries/" + delivery + "/accept"
	request("alpha", acceptRoute, `{"verdict":"accepted","approver":"forged-user"}`, auth.RoleWriter, 403)
	request("beta", acceptRoute, `{"verdict":"accepted"}`, auth.RoleAdmin, 404)
	request("alpha", acceptRoute, `{}`, auth.RoleAdmin, 400)
	request("alpha", acceptRoute, `{"verdict":"accepted","approver":"forged-user"}`, auth.RoleAdmin, 200)
	if err := fixture.QueryRow(ctx, `SELECT approver FROM acceptances WHERE delivery_id=$1`, delivery).Scan(&actor); err != nil || actor != "server-admin" {
		t.Fatalf("acceptance attribution: actor=%q err=%v", actor, err)
	}
	response = request("alpha", acceptRoute, `{"verdict":"rejected","approver":"forged-user"}`, auth.RoleAdmin, 200)
	if response["recorded"] != true || response["accepted"] != false || response["verdict"] != "rejected" {
		t.Fatalf("rejection response must not claim acceptance: %v", response)
	}
	var count int
	if err := fixture.QueryRow(ctx, `SELECT count(*) FROM projects WHERE slug='missing'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed write created a project: count=%d err=%v", count, err)
	}
	if err := fixture.QueryRow(ctx, `SELECT count(*) FROM acceptances`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("rejected requests wrote acceptance rows: count=%d err=%v", count, err)
	}
}
