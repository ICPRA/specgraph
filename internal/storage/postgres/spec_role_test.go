// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSpecRole_ReadPathsPreserveExplicitRole(t *testing.T) {
	store := newStore(t)
	clearDatabase(t, store)
	ctx := context.Background()

	for _, slug := range []string{"role-work", "role-summary", "role-composed"} {
		created, err := store.CreateSpec(ctx, slug, "Role fixture", "p1", "medium",
			storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, storage.SpecRoleWork, created.Role)
	}

	done := string(storage.SpecStageDone)
	before, err := store.UpdateSpec(ctx, "role-summary", nil, &done, nil, nil, nil)
	require.NoError(t, err)
	beforeChanges, err := store.ListChanges(ctx, before.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)

	// Seed only the explicit role; subdivision is a separate transaction contract.
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	defer pool.Close()
	tag, err := pool.Exec(ctx, `UPDATE specs SET role = 'summary' WHERE project_slug = 'test' AND slug = $1`, before.Slug)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	children, err := store.StoreDecomposeOutput(ctx, "role-composed", &storage.DecomposeOutput{
		Strategy: storage.StrategyVerticalSlice,
		Slices:   []storage.DecomposeSlice{{ID: "native", Intent: "Native slice"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"role-composed/native"}, children)
	_, err = store.GetSlice(ctx, children[0])
	require.NoError(t, err)
	_, err = store.GetSpec(ctx, children[0])
	require.ErrorIs(t, err, storage.ErrSpecNotFound)

	expectedRoles := map[string]storage.SpecRole{
		"role-work":     storage.SpecRoleWork,
		"role-summary":  storage.SpecRoleSummary,
		"role-composed": storage.SpecRoleWork,
	}
	for slug, role := range expectedRoles {
		got, getErr := store.GetSpec(ctx, slug)
		require.NoError(t, getErr)
		require.Equal(t, role, got.Role, slug)
	}

	listed, err := store.ListSpecs(ctx, "", "", 0)
	require.NoError(t, err)
	require.Len(t, listed, len(expectedRoles))
	for _, spec := range listed {
		require.Equal(t, expectedRoles[spec.Slug], spec.Role, spec.Slug)
		if spec.Slug == before.Slug {
			require.Equal(t, storage.SpecStageDone, spec.Stage)
		}
	}
	batch, err := store.BatchGetSpecs(ctx, []string{"role-work", "role-summary", "role-composed"})
	require.NoError(t, err)
	require.Len(t, batch, len(expectedRoles))
	for slug, role := range expectedRoles {
		require.Equal(t, role, batch[slug].Role, slug)
	}
	require.Equal(t, storage.SpecStageDone, batch[before.Slug].Stage)

	after, err := store.GetSpec(ctx, before.Slug)
	require.NoError(t, err)
	before.Role = storage.SpecRoleSummary
	require.Equal(t, before, after, "role must not rewrite stage, provenance, content, or version")
	afterChanges, err := store.ListChanges(ctx, before.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Equal(t, beforeChanges, afterChanges)
}
