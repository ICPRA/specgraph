// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestPreparedContextVersionAndClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var clockCalls atomic.Int32
	bundleRead, resume := make(chan struct{}), make(chan struct{})
	store := newStore(t, postgres.WithProject("context-version"), postgres.WithClock(func() time.Time {
		if clockCalls.Add(-1) == 0 {
			close(bundleRead)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
		return time.Now()
	}))
	spec, err := store.CreateSpec(ctx, "context-task", "Context fixture", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	spec, err = store.UpdateSpec(ctx, spec.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	probe, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer probe.Close(ctx)
	// The second clock read is GenerateBundle's GetActiveClaim, after claim insertion.
	clockCalls.Store(2)
	type result struct {
		id  string
		err error
	}
	prepared := make(chan result, 1)
	go func() {
		id, err := store.PrepareRun(ctx, spec.Slug, "context-workspace")
		prepared <- result{id, err}
	}()
	select {
	case <-bundleRead:
	case <-ctx.Done():
		t.Fatal("preparation did not reach bundle claim read")
	}
	var version int32
	lockErr := probe.QueryRow(ctx, `SELECT version FROM specs WHERE project_slug = 'context-version' AND slug = $1 FOR UPDATE NOWAIT`, spec.Slug).Scan(&version)
	close(resume)
	r := <-prepared
	require.NoError(t, r.err)
	var postgresErr *pgconn.PgError
	require.ErrorAs(t, lockErr, &postgresErr)
	require.Equal(t, "55P03", postgresErr.Code)
	var body []byte
	require.NoError(t, probe.QueryRow(ctx, `SELECT cp.body FROM context_packages cp JOIN run_bindings rb ON rb.package_id = cp.id AND rb.project_slug = cp.project_slug WHERE rb.id = $1 AND rb.project_slug = 'context-version'`, r.id).Scan(&body))
	var pkg struct {
		TaskSlug    string         `json:"task_slug"`
		Workspace   string         `json:"workspace"`
		GeneratedAt time.Time      `json:"generated_at"`
		SpecVersion int32          `json:"spec_version"`
		Bundle      storage.Bundle `json:"bundle"`
	}
	require.NoError(t, json.Unmarshal(body, &pkg))
	require.Equal(t, spec.Slug, pkg.TaskSlug)
	require.Equal(t, "context-workspace", pkg.Workspace)
	require.False(t, pkg.GeneratedAt.IsZero())
	require.Equal(t, spec.Version, pkg.SpecVersion)
	require.Equal(t, pkg.SpecVersion, pkg.Bundle.Spec.Version)
	require.Equal(t, r.id, pkg.Bundle.Claim.Agent)
	changedIntent := "Changed after preparation"
	_, err = store.UpdateSpec(ctx, spec.Slug, &changedIntent, nil, nil, nil, nil)
	require.NoError(t, err)
	// Old missing headers and missing packages remain unknown, not current version.
	_, err = probe.Exec(ctx, `INSERT INTO context_packages (id, project_slug, task_spec_slug, body) VALUES ('legacy-context-package', 'context-version', $1, '{}');`, spec.Slug)
	require.NoError(t, err)
	_, err = probe.Exec(ctx, `INSERT INTO run_bindings (id, project_slug, task_spec_slug, package_id) VALUES ('legacy-context-run', 'context-version', $1, 'legacy-context-package'), ('missing-context-run', 'context-version', $1, '')`, spec.Slug)
	require.NoError(t, err)
	metadata, err := store.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 3)
	for _, run := range metadata.Runs {
		if run.ID == r.id {
			require.NotNil(t, run.SpecVersion)
			require.Equal(t, spec.Version, *run.SpecVersion)
		} else {
			require.Nil(t, run.SpecVersion)
		}
	}
}

func TestPrepareRunStageEligibility(t *testing.T) {
	ctx := context.Background()
	store := newStore(t, postgres.WithProject("context-stages"))
	for _, stage := range []string{"spark", "shape", "specify", "decompose", "approved", "in_progress", "done"} {
		t.Run(stage, func(t *testing.T) {
			spec, err := store.CreateSpec(ctx, "stage-"+stage, "Stage fixture", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			spec, err = store.UpdateSpec(ctx, spec.Slug, nil, &stage, nil, nil, nil)
			require.NoError(t, err)
			id, err := store.PrepareRun(ctx, spec.Slug, "fixture")
			if stage == "approved" || stage == "in_progress" {
				require.NoError(t, err)
				require.NotEmpty(t, id)
				return
			}
			require.ErrorIs(t, err, storage.ErrSpecNotApproved)
			require.Empty(t, id)
			claim, err := store.GetActiveClaim(ctx, spec.Slug)
			require.NoError(t, err)
			require.Nil(t, claim)
		})
	}
	metadata, err := store.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 2)
	probe, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer probe.Close(ctx)
	var packages int
	require.NoError(t, probe.QueryRow(ctx, `SELECT count(*) FROM context_packages WHERE project_slug = 'context-stages'`).Scan(&packages))
	require.Equal(t, 2, packages)
}
