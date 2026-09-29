// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestSliceCannotReuseASpecGraphIdentity(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("node-namespace"))
	for _, slug := range []string{"parent", "parent/shared"} {
		_, err := s.CreateSpec(ctx, slug, slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	err := s.CreateSlice(ctx, &storage.Slice{Slug: "parent/shared", ParentSlug: "parent", SliceID: "shared", Intent: "Collision"})
	require.Error(t, err)
	_, err = s.GetSlice(ctx, "parent/shared")
	require.ErrorIs(t, err, storage.ErrSliceNotFound)
	_, err = s.GetSpec(ctx, "parent/shared")
	require.NoError(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := s.CreateSpec(ctx, "parent/race", "Work", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		results <- err
	}()
	go func() {
		<-start
		results <- s.CreateSlice(ctx, &storage.Slice{Slug: "parent/race", ParentSlug: "parent", SliceID: "race", Intent: "Slice"})
	}()
	close(start)
	successes := 0
	for range 2 {
		if <-results == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	var identities int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT
		(SELECT count(*) FROM specs WHERE project_slug='node-namespace' AND slug='parent/race') +
		(SELECT count(*) FROM slices WHERE project_slug='node-namespace' AND slug='parent/race')`).Scan(&identities))
	require.Equal(t, 1, identities)
}
