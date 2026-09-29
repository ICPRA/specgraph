// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestSummaryIsNotAnExecutableWorkItem(t *testing.T) {
	ctx := context.Background()
	project := "summary-execution"
	s := newStore(t, postgres.WithProject(project))
	require.NoError(t, s.SetProjectManaged(ctx, project, true))
	approved := "approved"
	for _, slug := range []string{"parent", "dependent-a", "dependent-b"} {
		_, err := s.CreateSpec(ctx, slug, slug, "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
	}
	run, err := s.PrepareRun(ctx, "parent", "fixture")
	require.NoError(t, err)
	delivery, err := s.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
	require.NoError(t, err)
	require.NoError(t, s.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
	_, err = s.AddEdge(ctx, "dependent-a", "parent", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, "parent", "dependent-b", storage.EdgeTypeBlocks)
	require.NoError(t, err)
	// Seed the future subdivision result; do not rewrite the previous work stage.
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET role='summary' WHERE project_slug=$1 AND slug='parent'`, project)
	require.NoError(t, err)
	parent, err := s.GetSpec(ctx, "parent")
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageApproved, parent.Stage)
	require.Equal(t, storage.SpecRoleSummary, parent.Role)
	_, err = s.ClaimSpec(ctx, "parent", run, time.Minute)
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	_, err = s.GenerateBundle(ctx, "parent")
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	_, err = s.PrepareRun(ctx, "parent", "fixture")
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	require.ErrorIs(t, s.RecordCompletion(ctx, "parent", run), storage.ErrSummaryNotExecutable)
	_, _, err = s.ManualComplete(ctx, "parent", "operator", parent.Version, "summary-manual", "Not a work-item completion")
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	// Historical done does not establish acceptance of the new summary structure.
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET stage='done' WHERE project_slug=$1 AND slug='parent'`, project)
	require.NoError(t, err)
	ready, err := s.GetReady(ctx)
	require.NoError(t, err)
	require.Empty(t, ready)
	for _, slug := range []string{"dependent-a", "dependent-b"} {
		_, err = s.PrepareRun(ctx, slug, "fixture")
		require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
		claim, err := s.GetActiveClaim(ctx, slug)
		require.NoError(t, err)
		require.Nil(t, claim)
	}
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 1)
	require.Equal(t, "prepared", metadata.Runs[0].State, "no assertion of host process termination")
	require.Len(t, metadata.Acceptances, 1, "retain historical acceptance")
}
