// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/storage"
)

// Use private session tables, not an outer transaction: PrepareRun must own
// the transaction so this check detects a separately committed claim.
func TestPrepareRunPostgresAtomicity(t *testing.T) {
	path := os.Getenv("SPECGRAPH_PREPARE_TEST_CONFIG")
	if path == "" {
		t.Skip("SPECGRAPH_PREPARE_TEST_CONFIG is not set")
	}
	cfg, configErr := config.LoadGlobalExplicit(path)
	if configErr != nil || cfg.Server.Postgres.URL == "" {
		t.Fatal("explicit test connection configuration required")
	}
	poolCfg, parseErr := pgxpool.ParseConfig(cfg.Server.Postgres.URL)
	if parseErr != nil {
		t.Fatal("cannot parse test connection configuration")
	}
	if host := poolCfg.ConnConfig.Host; host != "127.0.0.1" && host != "localhost" && host != "::1" {
		t.Fatal("this test only permits an explicitly configured loopback database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	observer, connectErr := pgx.ConnectConfig(ctx, poolCfg.ConnConfig.Copy())
	if connectErr != nil {
		t.Fatal("could not connect to the loopback database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := observer.Close(cleanup); err != nil {
			t.Errorf("close observer: %v", err)
		}
	}()
	poolCfg.MaxConns = 1
	poolCfg.MinConns = 0
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		for _, table := range []string{"projects", "dependency_changes", "specs", "claims", "edges", "conversation_logs", "decisions", "context_packages", "run_bindings"} {
			name := pgx.Identifier{table}.Sanitize()
			if _, err := conn.Exec(ctx, `CREATE TEMP TABLE `+name+` (LIKE public.`+name+` INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT PRESERVE ROWS`); err != nil {
				return fmt.Errorf("create private %s: %w", table, err)
			}
		}
		// Missing fixture tables must fail, never resolve to application tables.
		_, err := conn.Exec(ctx, `SET search_path TO pg_temp`)
		return err
	}
	pool, poolErr := pgxpool.NewWithConfig(ctx, poolCfg)
	if poolErr != nil {
		t.Fatal("could not create isolated test pool")
	}
	var namespace uint32
	defer func() {
		pool.Close()
		if namespace == 0 {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		// Closing the client socket does not wait for server-side session teardown.
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var count int
			if err := observer.QueryRow(cleanup, `SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace = $1`, namespace).Scan(&count); err != nil {
				t.Errorf("private table cleanup query: %v", err)
				return
			}
			if count == 0 {
				return
			}
			select {
			case <-cleanup.Done():
				t.Errorf("private table cleanup: remaining=%d err=%v", count, cleanup.Err())
				return
			case <-ticker.C:
			}
		}
	}()
	if err := pool.QueryRow(ctx, `SELECT pg_my_temp_schema()`).Scan(&namespace); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"projects", "dependency_changes", "specs", "claims", "edges", "conversation_logs", "decisions", "context_packages", "run_bindings"} {
		var isolated bool
		if err := pool.QueryRow(ctx, `SELECT $1::regclass::oid = $2::regclass::oid`, table, "pg_temp."+table).Scan(&isolated); err != nil || !isolated {
			t.Fatalf("table %s is not isolated: %v", table, err)
		}
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE pg_temp.run_bindings ADD CHECK (workspace <> 'reject-fixture')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE pg_temp.context_packages ADD CHECK (body->>'workspace' <> 'reject-package')`); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	store := &Store{pool: pool, project: "prepare-fixture", nowFunc: func() time.Time { return now }}
	if _, err := pool.Exec(ctx, `INSERT INTO pg_temp.projects(slug) VALUES ('prepare-fixture')`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, workspace, errorPart string
		otherOwner                 bool
		unapproved                 bool
	}{
		{name: "package failure", workspace: "reject-package", errorPart: "prepare run package"},
		{name: "binding failure", workspace: "reject-fixture", errorPart: "prepare run binding"},
		{name: "success", workspace: "fixture-workspace"},
		{name: "other owner", otherOwner: true, errorPart: "prepare run claim"},
		{name: "unapproved", unapproved: true, errorPart: "prepare run bundle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `TRUNCATE pg_temp.specs, pg_temp.claims, pg_temp.edges, pg_temp.context_packages, pg_temp.run_bindings`); err != nil {
				t.Fatal(err)
			}
			stage := "approved"
			if tc.unapproved {
				stage = "spark"
			}
			if _, err := pool.Exec(ctx, `INSERT INTO pg_temp.specs (slug, project_slug, intent, stage, version) VALUES ('task-fixture', 'prepare-fixture', 'test only', $1, 7)`, stage); err != nil {
				t.Fatal(err)
			}
			if tc.otherOwner {
				if _, err := store.ClaimSpec(ctx, "task-fixture", "another-owner", time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			binding, err := store.PrepareRun(ctx, "task-fixture", tc.workspace)
			if tc.errorPart == "" {
				if err != nil || binding == "" {
					t.Fatalf("prepare success: binding=%q err=%v", binding, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.errorPart) || binding != "" {
				t.Fatalf("expected %s and empty binding: binding=%q err=%v", tc.errorPart, binding, err)
			}
			if tc.otherOwner && !errors.Is(err, storage.ErrSpecAlreadyClaimed) {
				t.Fatalf("other owner must be refused: %v", err)
			}
			if tc.unapproved && !errors.Is(err, storage.ErrSpecNotApproved) {
				t.Fatalf("unapproved stage must be refused: %v", err)
			}
			var claims, edges, packages, bindings int
			if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM pg_temp.claims), (SELECT count(*) FROM pg_temp.edges), (SELECT count(*) FROM pg_temp.context_packages), (SELECT count(*) FROM pg_temp.run_bindings)`).Scan(&claims, &edges, &packages, &bindings); err != nil {
				t.Fatal(err)
			}
			wantClaims, wantWrites := 0, 0
			if tc.errorPart == "" {
				wantClaims, wantWrites = 1, 1
			} else if tc.otherOwner {
				wantClaims = 1
			}
			if claims != wantClaims || edges != wantClaims || packages != wantWrites || bindings != wantWrites {
				t.Fatalf("claims/edges/packages/bindings = %d/%d/%d/%d; want %d/%d/%d/%d", claims, edges, packages, bindings, wantClaims, wantClaims, wantWrites, wantWrites)
			}
			if wantClaims == 1 {
				var owner string
				var expires time.Time
				if err := pool.QueryRow(ctx, `SELECT agent, lease_expires FROM pg_temp.claims`).Scan(&owner, &expires); err != nil {
					t.Fatal(err)
				}
				wantOwner, wantLease := binding, 30*time.Minute
				if tc.otherOwner {
					wantOwner, wantLease = "another-owner", time.Hour
				}
				if owner != wantOwner || !expires.Equal(now.Add(wantLease)) {
					t.Fatalf("unexpected claim: owner=%s expires=%v", owner, expires)
				}
			}
			if wantWrites == 1 {
				var matches bool
				if err := pool.QueryRow(ctx, `SELECT rb.state = 'prepared' AND rb.generation = 1 AND rb.workspace = $2
					AND cp.body->>'task_slug' = rb.task_spec_slug AND cp.body->>'workspace' = rb.workspace
					AND (cp.body->>'spec_version')::integer = 7
					AND cp.body->'spec_version' = cp.body->'bundle'->'Spec'->'Version'
					AND cp.body->'bundle'->'Claim'->>'Agent' = rb.id
					FROM pg_temp.run_bindings rb JOIN pg_temp.context_packages cp ON cp.id = rb.package_id AND cp.project_slug = rb.project_slug WHERE rb.id = $1`, binding, tc.workspace).Scan(&matches); err != nil || !matches {
					t.Fatalf("package/binding contract: matches=%v err=%v", matches, err)
				}
			}
		})
	}
}
