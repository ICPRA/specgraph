// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestManualCompletion(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	require.NoError(t, store.SetProjectManaged(ctx, "test", true))
	t.Cleanup(func() { require.NoError(t, store.SetProjectManaged(ctx, "test", false)) })
	spec, err := store.CreateSpec(ctx, "manual", "Operator task", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = store.CreateSpec(ctx, "upstream-manual", "Original", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = store.AddEdge(ctx, spec.Slug, "upstream-manual", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	changedIntent := "Updated dependency"
	_, err = store.UpdateSpec(ctx, "upstream-manual", &changedIntent, nil, nil, nil, nil)
	require.NoError(t, err)
	_, _, err = store.ManualComplete(ctx, spec.Slug, "operator", spec.Version+1, "stale", "")
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	_, err = store.ClaimSpec(ctx, spec.Slug, "agent", time.Minute)
	require.NoError(t, err)
	_, _, err = store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "busy", "")
	require.ErrorIs(t, err, storage.ErrSpecAlreadyClaimed)
	claim, err := store.GetActiveClaim(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, "agent", claim.Agent)
	require.NoError(t, store.UnclaimSpec(ctx, spec.Slug, "agent"))
	version, replayed, err := store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "key", "Human assertion")
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, spec.Version+1, version)
	completed, err := store.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, storage.SpecStage("done"), completed.Stage)
	audit, err := store.ListManualCompletions(ctx, spec.Slug)
	require.NoError(t, err)
	require.Len(t, audit, 1)
	require.Equal(t, "operator", audit[0].Actor)
	require.Equal(t, spec.Version, audit[0].SourceVersion)
	require.Equal(t, version, audit[0].ResultVersion)
	events, err := store.GetExecutionEvents(ctx, spec.Slug, 0)
	require.NoError(t, err)
	require.Empty(t, events)
	changes, err := store.ListChanges(ctx, spec.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Equal(t, version, changes[len(changes)-1].Version)
	deps, err := store.GetDependenciesWithEdgeData(ctx, spec.Slug)
	require.NoError(t, err)
	require.Len(t, deps, 1)
	require.Equal(t, deps[0].UpstreamContentHash, deps[0].ContentHashAtLink)
	metadata, err := store.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Empty(t, metadata.Runs)
	require.Empty(t, metadata.Deliveries)
	require.Empty(t, metadata.Acceptances)
	replayedVersion, replayed, err := store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "key", "Human assertion")
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, version, replayedVersion)
	for _, tc := range []struct{ actor, key, note string }{
		{"other", "key", "Human assertion"},
		{"operator", "key", "changed"},
		{"operator", "new", "Human assertion"},
	} {
		_, _, err := store.ManualComplete(ctx, spec.Slug, tc.actor, spec.Version, tc.key, tc.note)
		require.ErrorIs(t, err, storage.ErrManualCompletionConflict)
	}
	audit, err = store.ListManualCompletions(ctx, spec.Slug)
	require.NoError(t, err)
	require.Len(t, audit, 1)
}

func TestManualCompletionRollback(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	spec, err := store.CreateSpec(ctx, "manual-rollback", "Rollback assertion", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(ctx, `ALTER TABLE manual_completions ADD CONSTRAINT reject_test_note CHECK (note <> 'reject-test')`)
	require.NoError(t, err)
	defer func() {
		_, err := pool.Exec(ctx, `ALTER TABLE manual_completions DROP CONSTRAINT reject_test_note`)
		require.NoError(t, err)
	}()
	_, _, err = store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "retry-key", "reject-test")
	require.Error(t, err)
	actual, err := store.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, spec.Version, actual.Version)
	require.Equal(t, spec.Stage, actual.Stage)
	audit, err := store.ListManualCompletions(ctx, spec.Slug)
	require.NoError(t, err)
	require.Empty(t, audit)
	changes, err := store.ListChanges(ctx, spec.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	_, replayed, err := store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "retry-key", "valid")
	require.NoError(t, err)
	require.False(t, replayed)
}

func TestManualCompletionConcurrent(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	spec, err := store.CreateSpec(ctx, "manual-race", "Concurrent assertion", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, key := range []string{"one", "two"} {
		go func() {
			<-start
			_, _, err := store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, key, "")
			results <- err
		}()
	}
	close(start)
	first, second := <-results, <-results
	require.True(t, (first == nil && errors.Is(second, storage.ErrManualCompletionConflict)) || (second == nil && errors.Is(first, storage.ErrManualCompletionConflict)), "first=%v second=%v", first, second)
	audit, err := store.ListManualCompletions(ctx, spec.Slug)
	require.NoError(t, err)
	require.Len(t, audit, 1)
}

func TestManualCompletionCannotReviveTerminalSpec(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	for _, stage := range []string{"abandoned", "superseded"} {
		t.Run(stage, func(t *testing.T) {
			spec, err := store.CreateSpec(ctx, "manual-terminal-"+stage, "Retired obligation", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			if stage == "abandoned" {
				spec, err = store.LifecycleAbandonSpec(ctx, spec.Slug, "Requirement removed")
			} else {
				_, _, err = store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "original", "Original completion")
				require.NoError(t, err)
				_, err = store.CreateSpec(ctx, "manual-replacement", "Replacement obligation", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
				spec, _, err = store.LifecycleSupersedeSpec(ctx, spec.Slug, "manual-replacement", "Requirement replaced")
			}
			require.NoError(t, err)
			before, err := store.ListManualCompletions(ctx, spec.Slug)
			require.NoError(t, err)
			_, _, err = store.ManualComplete(ctx, spec.Slug, "operator", spec.Version, "revive", "Must not turn withdrawal into completion")
			require.ErrorIs(t, err, storage.ErrSpecTerminal)
			actual, err := store.GetSpec(ctx, spec.Slug)
			require.NoError(t, err)
			require.Equal(t, spec.Stage, actual.Stage)
			require.Equal(t, spec.Version, actual.Version)
			after, err := store.ListManualCompletions(ctx, spec.Slug)
			require.NoError(t, err)
			require.Equal(t, before, after)
			if stage == "superseded" {
				_, replayed, err := store.ManualComplete(ctx, spec.Slug, "operator", 1, "original", "Original completion")
				require.NoError(t, err)
				require.True(t, replayed, "historical receipt remains readable, not reapplied")
				actual, err = store.GetSpec(ctx, spec.Slug)
				require.NoError(t, err)
				require.Equal(t, storage.SpecStageSuperseded, actual.Stage)
			}
		})
	}
}
