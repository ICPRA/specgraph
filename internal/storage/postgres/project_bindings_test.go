// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/storage"
)

// This opt-in check executes the real SQL against connection-private temporary
// tables. It neither creates a project nor writes persistent application rows.
func TestProjectBindingsPostgres(t *testing.T) {
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
		if err := conn.QueryRow(cleanup, `SELECT to_regclass('pg_temp.project_bindings') IS NULL AND to_regclass('pg_temp.projects') IS NULL`).Scan(&removed); err != nil || !removed {
			t.Errorf("temporary table cleanup: removed=%v err=%v", removed, err)
		}
	}()
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE projects (LIKE public.projects INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP;
		INSERT INTO pg_temp.projects(slug) VALUES ('alpha'),('beta')`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE project_bindings (LIKE public.project_bindings INCLUDING ALL) ON COMMIT DROP`); err != nil {
		t.Fatal("the configured database does not have the current project_bindings table; apply the baseline schema first:", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path TO pg_temp`); err != nil {
		t.Fatal(err)
	}
	var isolated bool
	if err := tx.QueryRow(ctx, `SELECT 'project_bindings'::regclass::oid = 'pg_temp.project_bindings'::regclass::oid AND 'projects'::regclass::oid = 'pg_temp.projects'::regclass::oid`).Scan(&isolated); err != nil || !isolated {
		t.Fatal("unqualified binding queries are not isolated to the temporary tables")
	}
	initial := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	store := &Store{project: "alpha", nowFunc: func() time.Time { return initial }}
	txCtx := txToContext(ctx, tx)

	// A project without any recorded binding has no active binding.
	if active, err := store.ActiveProjectBinding(txCtx, "alpha"); err != nil || active != nil {
		t.Fatalf("unbound project must have no active binding: %v %v", active, err)
	}

	// Bind records the confirmed target.
	bound, err := store.BindProject(txCtx, "alpha", "env-one", "native-one", "/ws/one", "First confirmed dispatch", "operator")
	if err != nil {
		t.Fatal(err)
	}
	if bound.ID == "" || bound.ProjectSlug != "alpha" || bound.EnvironmentID != "env-one" || bound.NativeProjectID != "native-one" ||
		bound.WorkspaceRoot == nil || *bound.WorkspaceRoot != "/ws/one" || bound.Reason != "First confirmed dispatch" ||
		bound.Actor != "operator" || !bound.CreatedAt.Equal(initial) || bound.RevokedAt != nil {
		t.Fatalf("binding record changed: %+v", bound)
	}
	active, err := store.ActiveProjectBinding(txCtx, "alpha")
	if err != nil || active == nil || active.ID != bound.ID {
		t.Fatalf("active binding lookup: %v %v", active, err)
	}

	// Rebind revokes the previous binding in the same transaction and keeps history.
	reboundAt := initial.Add(time.Hour)
	store.nowFunc = func() time.Time { return reboundAt }
	rebound, err := store.BindProject(txCtx, "alpha", "env-two", "native-two", "", "Rebind after project move", "operator")
	if err != nil {
		t.Fatal(err)
	}
	if rebound.WorkspaceRoot != nil || rebound.EnvironmentID != "env-two" || rebound.NativeProjectID != "native-two" || rebound.RevokedAt != nil {
		t.Fatalf("rebound record: %+v", rebound)
	}
	history, err := store.ProjectBindingHistory(txCtx, "alpha", 0)
	if err != nil {
		t.Fatal(err)
	}
	if history.HasMore || history.NextCursor != "" || len(history.Items) != 2 {
		t.Fatalf("history after rebind: %+v", history)
	}
	if history.Items[0].ID != rebound.ID || history.Items[1].ID != bound.ID {
		t.Fatal("history must be newest first")
	}
	if history.Items[1].RevokedAt == nil || !history.Items[1].RevokedAt.Equal(reboundAt) {
		t.Fatalf("rebind must revoke the previous binding: %+v", history.Items[1])
	}
	if active, err := store.ActiveProjectBinding(txCtx, "alpha"); err != nil || active == nil || active.ID != rebound.ID {
		t.Fatalf("rebind must replace the active binding: %v %v", active, err)
	}

	// The partial unique index permits at most one active binding per project.
	// The expected violation runs inside a savepoint so the surrounding test
	// transaction survives it.
	if _, err := tx.Exec(ctx, `SAVEPOINT project_binding_uniqueness`); err != nil {
		t.Fatal(err)
	}
	_, uniqueErr := tx.Exec(ctx, `INSERT INTO pg_temp.project_bindings(project_slug,environment_id,native_project_id,reason,actor)
		VALUES ('alpha','env-x','native-x','raw insert','operator')`)
	var pgErr *pgconn.PgError
	if !errors.As(uniqueErr, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("a second active binding must violate the partial unique index: %v", uniqueErr)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT project_binding_uniqueness`); err != nil {
		t.Fatal(err)
	}

	// History pages by decimal identity cursor.
	before, parseErr := strconv.ParseInt(rebound.ID, 10, 64)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	older, err := store.ProjectBindingHistory(txCtx, "alpha", before)
	if err != nil || len(older.Items) != 1 || older.Items[0].ID != bound.ID || older.HasMore {
		t.Fatalf("cursor paging: %+v %v", older, err)
	}

	// Revoke ends the active binding without deleting history.
	revokedAt := reboundAt.Add(time.Hour)
	store.nowFunc = func() time.Time { return revokedAt }
	revoked, err := store.RevokeProjectBinding(txCtx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if revoked.ID != rebound.ID || revoked.RevokedAt == nil || !revoked.RevokedAt.Equal(revokedAt) {
		t.Fatalf("revocation record: %+v", revoked)
	}
	if active, err := store.ActiveProjectBinding(txCtx, "alpha"); err != nil || active != nil {
		t.Fatalf("revoked project must have no active binding: %v %v", active, err)
	}
	if history, err := store.ProjectBindingHistory(txCtx, "alpha", 0); err != nil || len(history.Items) != 2 {
		t.Fatalf("revocation must preserve history: %+v %v", history, err)
	}
	if _, err := store.RevokeProjectBinding(txCtx, "alpha"); !errors.Is(err, storage.ErrProjectBindingNotFound) {
		t.Fatalf("second revoke must report the absent active binding: %v", err)
	}
	if _, err := store.RevokeProjectBinding(txCtx, "beta"); !errors.Is(err, storage.ErrProjectBindingNotFound) {
		t.Fatalf("revoking an unbound project must report the absent active binding: %v", err)
	}

	// Unknown projects and malformed requests are rejected.
	if _, err := store.BindProject(txCtx, "missing", "env", "native", "", "reason", "operator"); !errors.Is(err, storage.ErrProjectNotFound) {
		t.Fatalf("binding an unknown project: %v", err)
	}
	if _, err := store.ProjectBindingHistory(txCtx, "missing", 0); !errors.Is(err, storage.ErrProjectNotFound) {
		t.Fatalf("history of an unknown project: %v", err)
	}
	for _, bad := range [][6]string{
		{"", "env", "native", "", "reason", "operator"},
		{"alpha", " ", "native", "", "reason", "operator"},
		{"alpha", "env", "", "", "reason", "operator"},
		{"alpha", "env", "native", "", "", "operator"},
		{"alpha", "env", "native", "", strings.Repeat("r", 4001), "operator"},
		{"alpha", "env", "native", "", "reason", ""},
		{"alpha", "env", "native", strings.Repeat("w", 4097), "reason", "operator"},
	} {
		if _, err := store.BindProject(txCtx, bad[0], bad[1], bad[2], bad[3], bad[4], bad[5]); !errors.Is(err, storage.ErrInvalidProjectBinding) {
			t.Fatalf("malformed binding accepted: %q %v", bad, err)
		}
	}

	// Dispatch targets are checked against the active binding once one exists.
	target := func(environment, project string) json.RawMessage {
		body, err := json.Marshal(map[string]string{"environmentId": environment, "projectId": project})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	if err := store.checkDispatchTargetBinding(txCtx, target("env-three", "native-three")); err != nil {
		t.Fatalf("an unbound project must not constrain dispatch targets: %v", err)
	}
	if _, err := store.BindProject(txCtx, "alpha", "env-two", "native-two", "", "Confirmed again after unbind", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := store.checkDispatchTargetBinding(txCtx, target("env-two", "native-two")); err != nil {
		t.Fatalf("matching dispatch target: %v", err)
	}
	if err := store.checkDispatchTargetBinding(txCtx, target("env-other", "native-two")); !errors.Is(err, storage.ErrProjectBindingMismatch) {
		t.Fatalf("foreign environment must be rejected: %v", err)
	}
	if err := store.checkDispatchTargetBinding(txCtx, target("env-two", "native-other")); !errors.Is(err, storage.ErrProjectBindingMismatch) {
		t.Fatalf("foreign native project must be rejected: %v", err)
	}
	beta := &Store{project: "beta", nowFunc: func() time.Time { return revokedAt }}
	if err := beta.checkDispatchTargetBinding(txCtx, target("env-other", "native-other")); err != nil {
		t.Fatalf("an unbound project must not inherit another project's binding: %v", err)
	}
	if _, err := beta.BindProject(txCtx, "beta", "env-beta", "native-beta", "", "Beta binding", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := beta.checkDispatchTargetBinding(txCtx, target("env-two", "native-two")); !errors.Is(err, storage.ErrProjectBindingMismatch) {
		t.Fatalf("beta must be constrained by its own binding: %v", err)
	}
}
