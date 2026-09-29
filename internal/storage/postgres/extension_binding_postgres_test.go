// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/storage"
)

// This opt-in check executes the real SQL against a connection-private temporary
// table. It neither creates a project nor writes persistent application rows.
func TestBindRunThreadPostgres(t *testing.T) {
	path := os.Getenv("SPECGRAPH_BIND_TEST_CONFIG")
	if path == "" {
		t.Skip("SPECGRAPH_BIND_TEST_CONFIG is not set")
	}
	cfg, configErr := config.LoadGlobalExplicit(path)
	if configErr != nil {
		t.Fatal("cannot read explicit test connection configuration")
	}
	if cfg.Server.Postgres.URL == "" {
		t.Fatal("explicit database URL required")
	}
	connection, parseErr := pgx.ParseConfig(cfg.Server.Postgres.URL)
	if parseErr != nil {
		t.Fatal("cannot parse test connection configuration")
	}
	if connection.Host != "127.0.0.1" && connection.Host != "localhost" && connection.Host != "::1" {
		t.Fatal("this test only permits an explicitly configured loopback database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, connectErr := pgx.ConnectConfig(ctx, connection)
	if connectErr != nil {
		t.Fatal("could not connect to the loopback database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := conn.Close(cleanup); err != nil {
			t.Errorf("close test connection: %v", err)
		}
	}()
	tx, beginErr := conn.Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := tx.Rollback(cleanup); err != nil {
			t.Errorf("rollback test transaction: %v", err)
			return
		}
		var removed bool
		if err := conn.QueryRow(cleanup, `SELECT to_regclass('pg_temp.run_bindings') IS NULL AND to_regclass('pg_temp.deliveries') IS NULL AND to_regclass('pg_temp.acceptances') IS NULL AND to_regclass('pg_temp.specs') IS NULL`).Scan(&removed); err != nil || !removed {
			t.Errorf("temporary table cleanup: removed=%v err=%v", removed, err)
		}
	}()
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE run_bindings (LIKE public.run_bindings INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE specs (LIKE public.specs INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE projects (LIKE public.projects INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP;
		INSERT INTO pg_temp.projects(slug) VALUES ('alpha'),('beta')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE deliveries (LIKE public.deliveries INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE acceptances (LIKE public.acceptances INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	var isolated bool
	if err := tx.QueryRow(ctx, `SELECT 'run_bindings'::regclass::oid = 'pg_temp.run_bindings'::regclass::oid AND 'deliveries'::regclass::oid = 'pg_temp.deliveries'::regclass::oid AND 'acceptances'::regclass::oid = 'pg_temp.acceptances'::regclass::oid`).Scan(&isolated); err != nil || !isolated {
		t.Fatal("unqualified binding queries are not isolated to the temporary table")
	}
	initial := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	if _, err := tx.Exec(ctx, `INSERT INTO pg_temp.specs (slug, project_slug, intent) VALUES ('task-test', 'alpha', 'private fixture')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pg_temp.run_bindings (id, project_slug, task_spec_slug, state, updated_at)
		VALUES ('binding-test', 'alpha', 'task-test', 'prepared', $1), ('cancelled-test', 'alpha', 'task-test', 'cancelled', $1)`, initial); err != nil {
		t.Fatal(err)
	}
	boundAt := initial.Add(time.Hour)
	store := &Store{project: "alpha", nowFunc: func() time.Time { return boundAt }}
	txCtx := txToContext(ctx, tx)
	if err := store.BindRunThread(txCtx, "binding-test", "thread-one"); err != nil {
		t.Fatal(err)
	}
	assertBinding := func(wantState string) {
		t.Helper()
		var reference, state string
		var updated time.Time
		if err := tx.QueryRow(ctx, `SELECT thread_ref, state, updated_at FROM pg_temp.run_bindings WHERE id = 'binding-test'`).Scan(&reference, &state, &updated); err != nil {
			t.Fatal(err)
		}
		if reference != "thread-one" || state != wantState || !updated.Equal(boundAt) {
			t.Fatalf("binding changed unexpectedly: %q %q %v", reference, state, updated)
		}
	}
	assertBinding("bound")
	store.nowFunc = func() time.Time { return boundAt.Add(time.Hour) }
	if err := store.BindRunThread(txCtx, "binding-test", "thread-one"); err != nil {
		t.Fatal(err)
	}
	assertBinding("bound")
	if _, err := tx.Exec(ctx, `UPDATE pg_temp.run_bindings SET state = 'completed' WHERE id = 'binding-test'`); err != nil {
		t.Fatal(err)
	}
	if err := store.BindRunThread(txCtx, "binding-test", "thread-one"); err != nil {
		t.Fatal(err)
	}
	assertBinding("completed")
	if err := store.BindRunThread(txCtx, "binding-test", "thread-two"); !errors.Is(err, storage.ErrRunBindingConflict) {
		t.Fatalf("replacement must conflict: %v", err)
	}
	assertBinding("completed")
	if err := store.BindRunThread(txCtx, "cancelled-test", "thread-one"); !errors.Is(err, storage.ErrRunBindingConflict) {
		t.Fatalf("cancelled empty binding must conflict: %v", err)
	}
	if err := store.BindRunThread(txCtx, "missing-test", "thread-one"); !errors.Is(err, storage.ErrRunBindingNotFound) {
		t.Fatalf("missing binding: %v", err)
	}
	store.project = "beta"
	if err := store.BindRunThread(txCtx, "binding-test", "thread-one"); !errors.Is(err, storage.ErrRunBindingNotFound) {
		t.Fatalf("wrong project must not bind: %v", err)
	}
	assertBinding("completed")

	if _, err := store.CreateDelivery(txCtx, "binding-test", json.RawMessage(`{"fixture":true}`), "writer-fixture"); !errors.Is(err, storage.ErrRunBindingNotFound) {
		t.Fatalf("cross-project delivery must not be created: %v", err)
	}
	store.project = "alpha"
	deliveryID, deliveryErr := store.CreateDelivery(txCtx, "binding-test", json.RawMessage(`{"fixture":true}`), "writer-fixture")
	if deliveryErr != nil {
		t.Fatal(deliveryErr)
	}
	store.project = "beta"
	if err := store.InsertAcceptance(txCtx, deliveryID, "fixture", "accepted", "admin-fixture", nil); !errors.Is(err, storage.ErrDeliveryNotFound) {
		t.Fatalf("cross-project acceptance must not be created: %v", err)
	}
	// Counterexamples to the old gate: historical mixed-project rows must not
	// validate an alpha task. These rows exist only in this private test table.
	if _, err := tx.Exec(ctx, `INSERT INTO pg_temp.acceptances (id, project_slug, delivery_id, verdict) VALUES ('foreign-acceptance', 'beta', $1, 'accepted')`, deliveryID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO pg_temp.deliveries (id, project_slug, run_binding_id, snapshot) VALUES ('foreign-delivery', 'beta', 'binding-test', '{}')`); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAcceptance(txCtx, "foreign-delivery", "fixture", "accepted", "admin-fixture", nil); !errors.Is(err, storage.ErrDeliveryNotFound) {
		t.Fatalf("mixed-project delivery must not be accepted: %v", err)
	}
	store.project = "alpha"
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "binding-test"); err != nil || accepted {
		t.Fatalf("foreign acceptance must not satisfy the gate: accepted=%v err=%v", accepted, err)
	}
	if err := store.InsertAcceptance(txCtx, deliveryID, "fixture", "accepted", "admin-fixture", nil); err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "binding-test"); err != nil || !accepted {
		t.Fatalf("same-project acceptance: accepted=%v err=%v", accepted, err)
	}
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "cancelled-test"); err != nil || accepted {
		t.Fatalf("another run cannot inherit acceptance: accepted=%v err=%v", accepted, err)
	}
	deliveryID, deliveryErr = store.CreateDelivery(txCtx, "binding-test", json.RawMessage(`{"candidate":2}`), "writer-fixture")
	if deliveryErr != nil {
		t.Fatal(deliveryErr)
	}
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "binding-test"); err != nil || accepted {
		t.Fatalf("new candidate requires its own acceptance: accepted=%v err=%v", accepted, err)
	}
	if err := store.InsertAcceptance(txCtx, deliveryID, "fixture", "accepted", "admin-fixture", nil); err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "binding-test"); err != nil || !accepted {
		t.Fatalf("current candidate acceptance: accepted=%v err=%v", accepted, err)
	}
	store.nowFunc = func() time.Time { return boundAt.Add(2 * time.Hour) }
	if err := store.InsertAcceptance(txCtx, deliveryID, "fixture", "rejected", "admin-fixture", nil); err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.hasAcceptedDelivery(txCtx, "task-test", "binding-test"); err != nil || accepted {
		t.Fatalf("an old acceptance must not override a later rejection: accepted=%v err=%v", accepted, err)
	}
}
