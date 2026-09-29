// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestDependencyEditRemovesSelfDependency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("dependency-remove-self"))
	spec, err := s.CreateSpec(ctx, "task", "Repair self dependency", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, spec.Slug, spec.Slug, storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	state, err := s.ReadDependencyEditState(ctx, spec.Slug)
	require.NoError(t, err)
	result, err := s.RemoveDependency(ctx, spec.Slug, "operator", storage.DependencyEditRequest{
		Prerequisite: spec.Slug, ExpectedVersion: spec.Version, ExpectedPrerequisiteVersion: spec.Version,
		ExpectedRevision: state.Revision, Reason: "Repair the self dependency", IdempotencyKey: "remove-self",
	})
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, "remove", result.Operation)
	_, err = s.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	deps, err := s.GetDependencies(ctx, spec.Slug)
	require.NoError(t, err)
	require.Empty(t, deps)
}

func TestDependencyEditRejectsNewCycles(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("dependency-cycle"))
	for _, slug := range []string{"a", "b", "c"} {
		_, err := s.CreateSpec(ctx, slug, "Cycle fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	_, err := s.AddEdge(ctx, "a", "b", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, "c", "b", storage.EdgeTypeBlocks)
	require.NoError(t, err)
	before, err := s.ReadDependencyEditState(ctx, "c")
	require.NoError(t, err)
	beforeEdges, err := s.GetDependencies(ctx, "c")
	require.NoError(t, err)
	request := storage.DependencyEditRequest{Prerequisite: "a", ExpectedVersion: 1, ExpectedPrerequisiteVersion: 1, ExpectedRevision: before.Revision, Reason: "Cycle must be rejected", IdempotencyKey: "cycle"}
	_, err = s.AddDependency(ctx, "c", "operator", request)
	require.ErrorIs(t, err, storage.ErrDependencyCycle)
	request.Prerequisite, request.IdempotencyKey = "c", "self-cycle"
	_, err = s.AddDependency(ctx, "c", "operator", request)
	require.ErrorIs(t, err, storage.ErrDependencyCycle)
	after, err := s.ReadDependencyEditState(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, before, after, "rejected edits must not change revision or receipts")
	afterEdges, err := s.GetDependencies(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, beforeEdges, afterEdges)
	// Raw historical/imported edges remain readable and repairable.
	_, err = s.AddEdge(ctx, "c", "a", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	state, err := s.ReadDependencyEditState(ctx, "c")
	require.NoError(t, err)
	request.Prerequisite, request.IdempotencyKey, request.ExpectedRevision = "a", "existing-cycle", state.Revision
	result, err := s.AddDependency(ctx, "c", "operator", request)
	require.NoError(t, err)
	require.False(t, result.Changed)
	request.IdempotencyKey = "repair-cycle"
	result, err = s.RemoveDependency(ctx, "c", "operator", request)
	require.NoError(t, err)
	require.True(t, result.Changed)
	dependencies, err := s.GetDependencies(ctx, "c")
	require.NoError(t, err)
	require.Empty(t, dependencies)
}

func TestDependencyEditReceiptAndReplay(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	dependent, err := store.CreateSpec(ctx, "edit-dependent", "Dependent", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	prerequisite, err := store.CreateSpec(ctx, "edit-prerequisite", "Prerequisite", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	baseline, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Empty(t, baseline.Operations)
	request := storage.DependencyEditRequest{Prerequisite: prerequisite.Slug, ExpectedVersion: dependent.Version, ExpectedPrerequisiteVersion: prerequisite.Version, ExpectedRevision: baseline.Revision, Reason: "Requires shared contract", IdempotencyKey: "edit-first"}
	result, err := store.AddDependency(ctx, dependent.Slug, "operator", request)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, "add", result.Operation)
	require.False(t, result.Replayed)
	require.Equal(t, dependent.Slug, result.Dependent)
	require.Equal(t, prerequisite.Slug, result.Prerequisite)
	require.Greater(t, result.Revision, baseline.Revision)
	state, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Equal(t, dependent.Version, state.SpecVersion)
	require.Equal(t, result.Revision, state.Revision)
	require.Len(t, state.Operations, 1)
	receipt := state.Operations[0]
	require.NotEmpty(t, receipt.ID)
	require.Equal(t, "add", receipt.Operation)
	require.Equal(t, "operator", receipt.Actor)
	require.Equal(t, request.Reason, receipt.Reason)
	require.Equal(t, prerequisite.Slug, receipt.Prerequisite)
	require.Equal(t, result.Revision, receipt.Revision)
	require.True(t, receipt.Changed)
	require.False(t, receipt.CreatedAt.IsZero())
	dependencies, err := store.GetDependenciesWithEdgeData(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Len(t, dependencies, 1)
	require.Equal(t, prerequisite.ContentHash, dependencies[0].ContentHashAtLink)

	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()
	var stored storage.DependencyEditRequest
	require.NoError(t, pool.QueryRow(ctx, `SELECT prerequisite_slug, expected_version, expected_prerequisite_version, expected_revision, reason, idempotency_key FROM dependency_edits WHERE id = $1`, receipt.ID).Scan(&stored.Prerequisite, &stored.ExpectedVersion, &stored.ExpectedPrerequisiteVersion, &stored.ExpectedRevision, &stored.Reason, &stored.IdempotencyKey))
	require.Equal(t, request, stored)

	changedIntent := "Updated dependent"
	_, err = store.UpdateSpec(ctx, dependent.Slug, &changedIntent, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.RemoveEdge(ctx, dependent.Slug, prerequisite.Slug, storage.EdgeTypeDependsOn))
	replayed, err := store.AddDependency(ctx, dependent.Slug, "operator", request)
	require.NoError(t, err)
	result.Replayed = true
	require.Equal(t, result, replayed)
	state, err = store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Greater(t, state.Revision, result.Revision)
	require.Equal(t, dependent.Version+1, state.SpecVersion)
	require.Equal(t, []storage.DependencyEditRecord{receipt}, state.Operations)
	dependencies, err = store.GetDependenciesWithEdgeData(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Empty(t, dependencies)

	for _, field := range []string{"actor", "dependent", "prerequisite", "reason", "version", "prerequisiteVersion", "revision"} {
		t.Run("conflicting-"+field, func(t *testing.T) {
			conflict := request
			actor, slug := "operator", dependent.Slug
			switch field {
			case "actor":
				actor = "other-operator"
			case "dependent":
				slug = "other-dependent"
			case "prerequisite":
				conflict.Prerequisite = "other-prerequisite"
			case "reason":
				conflict.Reason = "Different reason"
			case "version":
				conflict.ExpectedVersion++
			case "prerequisiteVersion":
				conflict.ExpectedPrerequisiteVersion++
			case "revision":
				conflict.ExpectedRevision++
			}
			_, err := store.AddDependency(ctx, slug, actor, conflict)
			require.ErrorIs(t, err, storage.ErrConcurrentModification)
		})
	}
}

func TestDependencyEditBaselinesAndNoOp(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	dependent, err := store.CreateSpec(ctx, "baseline-dependent", "Dependent", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	prerequisite, err := store.CreateSpec(ctx, "baseline-prerequisite", "Prerequisite", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	state, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	request := storage.DependencyEditRequest{Prerequisite: prerequisite.Slug, ExpectedVersion: dependent.Version, ExpectedPrerequisiteVersion: prerequisite.Version, ExpectedRevision: state.Revision, Reason: "Review", IdempotencyKey: "baseline"}
	for _, field := range []string{"version", "prerequisiteVersion", "revision"} {
		t.Run("stale-"+field, func(t *testing.T) {
			stale := request
			switch field {
			case "version":
				stale.ExpectedVersion++
			case "prerequisiteVersion":
				stale.ExpectedPrerequisiteVersion++
			case "revision":
				stale.ExpectedRevision++
			}
			_, err := store.AddDependency(ctx, dependent.Slug, "operator", stale)
			require.ErrorIs(t, err, storage.ErrConcurrentModification)
			_, err = store.RemoveDependency(ctx, dependent.Slug, "operator", stale)
			require.ErrorIs(t, err, storage.ErrConcurrentModification)
		})
	}
	unchanged, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Equal(t, state, unchanged)
	_, err = store.AddEdge(ctx, dependent.Slug, prerequisite.Slug, storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = store.AddDependency(ctx, dependent.Slug, "operator", request)
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	state, err = store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Empty(t, state.Operations, "native edges must not invent operator attribution")
	request.ExpectedRevision = state.Revision
	for i := range 21 {
		request.IdempotencyKey = fmt.Sprintf("duplicate-%d", i)
		result, err := store.AddDependency(ctx, dependent.Slug, "operator", request)
		require.NoError(t, err)
		require.False(t, result.Changed)
		require.False(t, result.Replayed)
		require.Equal(t, state.Revision, result.Revision)
	}
	after, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Equal(t, state.Revision, after.Revision)
	require.Len(t, after.Operations, 20)
}

func TestDependencyEditProjectIsolation(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()
	dependent, err := store.CreateSpec(ctx, "local-dependent", "Dependent", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	other := newStore(t, postgres.WithProject("dependency-other"))
	prerequisite, err := other.CreateSpec(ctx, "foreign-prerequisite", "Other project", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	request := storage.DependencyEditRequest{Prerequisite: prerequisite.Slug, ExpectedVersion: dependent.Version, ExpectedPrerequisiteVersion: prerequisite.Version, Reason: "Wrong project", IdempotencyKey: "foreign"}
	_, err = store.AddDependency(ctx, dependent.Slug, "operator", request)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = store.RemoveDependency(ctx, dependent.Slug, "operator", request)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = other.AddDependency(ctx, dependent.Slug, "operator", request)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = other.ReadDependencyEditState(ctx, dependent.Slug)
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	state, err := store.ReadDependencyEditState(ctx, dependent.Slug)
	require.NoError(t, err)
	require.Empty(t, state.Operations)
}

func TestDependencyEditRemoveRepresentations(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()
	for _, representation := range []string{"depends-on", "blocks", "both"} {
		t.Run(representation, func(t *testing.T) {
			clearDatabase(t, store)
			dependent, err := store.CreateSpec(ctx, "remove-dependent-"+representation, "Dependent", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			prerequisite, err := store.CreateSpec(ctx, "remove-prerequisite-"+representation, "Prerequisite", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			dependent, err = store.GetSpec(ctx, dependent.Slug)
			require.NoError(t, err)
			prerequisite, err = store.GetSpec(ctx, prerequisite.Slug)
			require.NoError(t, err)
			edgeCount := 0
			if representation != "blocks" {
				_, err = store.AddEdge(ctx, dependent.Slug, prerequisite.Slug, storage.EdgeTypeDependsOn)
				require.NoError(t, err)
				edgeCount++
			}
			if representation != "depends-on" {
				_, err = store.AddEdge(ctx, prerequisite.Slug, dependent.Slug, storage.EdgeTypeBlocks)
				require.NoError(t, err)
				edgeCount++
			}
			baseline, err := store.ReadDependencyEditState(ctx, dependent.Slug)
			require.NoError(t, err)
			request := storage.DependencyEditRequest{Prerequisite: prerequisite.Slug, ExpectedVersion: dependent.Version, ExpectedPrerequisiteVersion: prerequisite.Version, ExpectedRevision: baseline.Revision, Reason: "Dependency reviewed", IdempotencyKey: "existing-add"}
			added, err := store.AddDependency(ctx, dependent.Slug, "operator", request)
			require.NoError(t, err)
			require.False(t, added.Changed, "either representation already satisfies add")
			require.Equal(t, baseline.Revision, added.Revision)
			_, err = store.RemoveDependency(ctx, dependent.Slug, "operator", request)
			require.ErrorIs(t, err, storage.ErrConcurrentModification, "add receipt cannot authorize remove")
			var actualEdges int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM edges WHERE project_slug = 'test' AND ((from_slug = $1 AND to_slug = $2 AND edge_type = 'DEPENDS_ON') OR (from_slug = $2 AND to_slug = $1 AND edge_type = 'BLOCKS'))`, dependent.Slug, prerequisite.Slug).Scan(&actualEdges))
			require.Equal(t, edgeCount, actualEdges)

			request.IdempotencyKey = "remove-existing"
			removed, err := store.RemoveDependency(ctx, dependent.Slug, "operator", request)
			require.NoError(t, err)
			require.Equal(t, "remove", removed.Operation)
			require.True(t, removed.Changed)
			require.False(t, removed.Replayed)
			require.Greater(t, removed.Revision, baseline.Revision)
			dependencies, err := store.GetDependencies(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Empty(t, dependencies)
			actualDependent, err := store.GetSpec(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Equal(t, dependent, actualDependent)
			actualPrerequisite, err := store.GetSpec(ctx, prerequisite.Slug)
			require.NoError(t, err)
			require.Equal(t, prerequisite, actualPrerequisite)
			state, err := store.ReadDependencyEditState(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Len(t, state.Operations, 2)
			require.Equal(t, "remove", state.Operations[0].Operation)
			require.Equal(t, "add", state.Operations[1].Operation)
			var inserts, deletes int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE operation = 'INSERT'), count(*) FILTER (WHERE operation = 'DELETE') FROM dependency_changes WHERE project_slug = 'test' AND dependent_slug = $1`, dependent.Slug).Scan(&inserts, &deletes))
			require.Equal(t, edgeCount, inserts, "original relation history is retained")
			require.Equal(t, edgeCount, deletes, "each removed representation is recorded")
			_, err = store.AddDependency(ctx, dependent.Slug, "operator", request)
			require.ErrorIs(t, err, storage.ErrConcurrentModification, "remove receipt cannot authorize add")

			noOp := request
			noOp.IdempotencyKey = "remove-absent"
			noOp.ExpectedRevision = removed.Revision
			unchanged, err := store.RemoveDependency(ctx, dependent.Slug, "operator", noOp)
			require.NoError(t, err)
			require.False(t, unchanged.Changed)
			require.Equal(t, removed.Revision, unchanged.Revision)
			state, err = store.ReadDependencyEditState(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Len(t, state.Operations, 3)
			require.False(t, state.Operations[0].Changed)
			_, err = store.AddEdge(ctx, prerequisite.Slug, dependent.Slug, storage.EdgeTypeBlocks)
			require.NoError(t, err)
			changedIntent := "Later primary version"
			_, err = store.UpdateSpec(ctx, dependent.Slug, &changedIntent, nil, nil, nil, nil)
			require.NoError(t, err)
			replayed, err := store.RemoveDependency(ctx, dependent.Slug, "operator", request)
			require.NoError(t, err)
			removed.Replayed = true
			require.Equal(t, removed, replayed)
			dependencies, err = store.GetDependencies(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Len(t, dependencies, 1, "replay must not remove a later relation")
			state, err = store.ReadDependencyEditState(ctx, dependent.Slug)
			require.NoError(t, err)
			require.Len(t, state.Operations, 3)
		})
	}
}
