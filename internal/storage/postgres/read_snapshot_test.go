// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestReadSnapshotDoesNotMixSubdivisionCommits(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("read-snapshot"))
	parent, err := s.CreateSpec(ctx, "parent", "Original", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	err = s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		before, err := s.GetSpec(snapshotCtx, parent.Slug)
		if err != nil {
			return err
		}
		require.Equal(t, storage.SpecRoleWork, before.Role)
		// Publish on another connection between the first and subsequent reads.
		_, err = s.SubdivideSpec(ctx, parent.Slug, "operator", storage.SubdivideRequest{
			ExpectedVersion: parent.Version, Reason: "Split work",
			Children: []storage.ChildSpecDraft{{Slug: "child", Intent: "Child work", Priority: "p2", Complexity: "low"}},
		})
		if err != nil {
			return err
		}
		specs, err := s.ListSpecs(snapshotCtx, "", "", 0)
		if err != nil {
			return err
		}
		require.Len(t, specs, 1)
		require.Equal(t, storage.SpecRoleWork, specs[0].Role)
		graph, err := s.GetFullGraph(snapshotCtx)
		if err != nil {
			return err
		}
		for _, node := range graph.Nodes {
			require.NotEqual(t, "child", node.Slug)
		}
		for _, edge := range graph.Edges {
			require.NotEqual(t, storage.EdgeTypeComposes, edge.EdgeType)
		}
		return nil
	})
	require.NoError(t, err)
	after, err := s.GetSpec(ctx, parent.Slug)
	require.NoError(t, err)
	require.Equal(t, storage.SpecRoleSummary, after.Role)
	_, err = s.GetSpec(ctx, "child")
	require.NoError(t, err)
	changed := "Must not write"
	err = s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		_, err := s.UpdateSpec(snapshotCtx, parent.Slug, &changed, nil, nil, nil, nil)
		return err
	})
	var postgresErr *pgconn.PgError
	require.ErrorAs(t, err, &postgresErr)
	require.Equal(t, "25006", postgresErr.Code)
	after, err = s.GetSpec(ctx, parent.Slug)
	require.NoError(t, err)
	require.Equal(t, "Original", after.Intent)
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		return s.RunReadSnapshot(txCtx, func(context.Context) error { return nil })
	})
	require.ErrorContains(t, err, "cannot nest")
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = s.RunReadSnapshot(cancelled, func(context.Context) error { cancel(); return nil })
	require.Error(t, err, "a failed commit cannot advertise a successful snapshot")
}
