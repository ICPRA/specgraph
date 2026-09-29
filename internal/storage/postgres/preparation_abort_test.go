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

func TestPreparationAbortFencesDelayedPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := newStore(t, postgres.WithProject("abort-unpublished"))
	_, err := s.CreateSpec(ctx, "task", "Goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture.","version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"env"}`)
	request := storage.PreparationAbortRequest{TaskSlug: "task", Workspace: "workspace", IdempotencyKey: "unknown", Target: target, Note: "Withdraw pending request"}
	late := make(chan error, 1)
	require.NoError(t, s.RunInTransaction(ctx, func(txCtx context.Context) error {
		result, err := s.AbortPreparation(txCtx, "manager", &request)
		require.NoError(t, err)
		require.Nil(t, result.RunID)
		go func() {
			_, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "unknown", target)
			late <- err
		}()
		require.Eventually(t, func() bool {
			var waiting bool
			err := s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid))>0 AND query='SELECT slug FROM projects WHERE slug = $1 FOR NO KEY UPDATE')`).Scan(&waiting)
			return err == nil && waiting
		}, 5*time.Second, 10*time.Millisecond)
		return nil
	}))
	require.ErrorIs(t, <-late, storage.ErrPreparationCancelled)
	result, err := s.AbortPreparation(ctx, "manager", &request)
	require.NoError(t, err)
	require.True(t, result.Replayed)
	require.Nil(t, result.RunID)
	changed := request
	changed.Note = "Different intent"
	_, err = s.AbortPreparation(ctx, "manager", &changed)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	var count int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_bindings WHERE project_slug='abort-unpublished'`).Scan(&count))
	require.Zero(t, count)
	_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "fresh", target)
	require.NoError(t, err)
}

func TestPreparationAbortReconcilesPublishedButNotAdmittedRun(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("abort-published"))
	_, err := s.CreateSpec(ctx, "task", "Goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture.","version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "published", target)
	require.NoError(t, err)
	request := storage.PreparationAbortRequest{TaskSlug: "task", Workspace: "workspace", IdempotencyKey: "published", Target: target, Note: "Cancel unknown result"}
	result, err := s.AbortPreparation(ctx, "manager", &request)
	require.NoError(t, err)
	require.Equal(t, run, *result.RunID)
	status, err := s.ReadRunDispatch(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "manager", status.Cancellation.Actor)
	newTarget := json.RawMessage(`{"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture.","version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"env","threadId":"thread2","workspace":"workspace","createCommandId":"create2","startCommandId":"start2","messageId":"message2"}`)
	active, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "active", newTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, active, "env", "thread2"))
	pkg, err := s.ReadRunContext(ctx, active)
	require.NoError(t, err)
	admission, err := s.AuthorizeRunDispatch(ctx, active, "manager", pkg.PackageID, newTarget)
	require.NoError(t, err)
	request.IdempotencyKey = "active"
	request.Target = newTarget
	result, err = s.AbortPreparation(ctx, "manager", &request)
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	require.Equal(t, active, *result.RunID)
	require.NoError(t, s.ResolveRunDispatch(ctx, active, admission.ID, "manager", "stopped_writing", "Checked"))
	_, err = s.AbortPreparation(ctx, "manager", &request)
	require.NoError(t, err)
}
