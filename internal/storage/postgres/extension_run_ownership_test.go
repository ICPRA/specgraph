// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestPreparedRunOwnsItsClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var pauseClaimCheck atomic.Bool
	claimCheck := make(chan struct{})
	resume := make(chan struct{})
	store := newStore(t, postgres.WithProject("run-ownership"), postgres.WithClock(func() time.Time {
		if pauseClaimCheck.CompareAndSwap(true, false) {
			close(claimCheck)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
		return time.Now()
	}))
	require.NoError(t, store.SetProjectManaged(ctx, "run-ownership", false))
	spec, err := store.CreateSpec(ctx, "owned-run", "Ownership fixture", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	spec, err = store.UpdateSpec(ctx, spec.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	type result struct {
		id  string
		err error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			id, err := store.PrepareRun(ctx, spec.Slug, "fixture")
			results <- result{id, err}
		}()
	}
	close(start)
	var winner string
	for range 2 {
		r := <-results
		if r.err == nil {
			require.Empty(t, winner, "only one preparation may own the task")
			winner = r.id
		} else {
			require.ErrorIs(t, r.err, storage.ErrSpecAlreadyClaimed)
			require.Empty(t, r.id)
		}
	}
	require.NotEmpty(t, winner)
	claim, err := store.GetActiveClaim(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, winner, claim.Agent)
	metadata, err := store.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 1)

	// Model a released lease and replacement execution; a late old completion
	// must not consume the replacement's claim or complete its task.
	require.NoError(t, store.UnclaimSpec(ctx, spec.Slug, winner))
	replacement, err := store.PrepareRun(ctx, spec.Slug, "replacement")
	require.NoError(t, err)
	require.NotEqual(t, winner, replacement)
	require.ErrorIs(t, store.RecordCompletion(ctx, spec.Slug, winner), storage.ErrAgentNotClaimOwner)
	claim, err = store.GetActiveClaim(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, replacement, claim.Agent)
	unchanged, err := store.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, spec.Version, unchanged.Version)
	// Pause at the claim check and prove that completion already holds the
	// same spec-row lock as preparation, rather than testing by timing sleeps.
	pauseClaimCheck.Store(true)
	completed := make(chan error, 1)
	go func() { completed <- store.RecordCompletion(ctx, spec.Slug, replacement) }()
	select {
	case <-claimCheck:
	case <-ctx.Done():
		t.Fatal("completion did not reach claim check")
	}
	probe, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	var version int32
	lockErr := probe.QueryRow(ctx, `SELECT version FROM specs WHERE project_slug = $1 AND slug = $2 FOR UPDATE NOWAIT`, "run-ownership", spec.Slug).Scan(&version)
	close(resume)
	completionErr := <-completed
	require.NoError(t, probe.Close(ctx))
	var postgresErr *pgconn.PgError
	require.ErrorAs(t, lockErr, &postgresErr)
	require.Equal(t, "55P03", postgresErr.Code, "completion must lock the spec before checking its lease")
	require.NoError(t, completionErr)
}
