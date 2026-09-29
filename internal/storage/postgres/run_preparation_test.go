// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestRunPreparationRetryPreservesExecutionAndLease(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("run-preparation-retry"))
	target := json.RawMessage(`{"environmentId":"local","projectId":"native-project","threadId":"native-thread","messageId":"dispatch-message","modelSelection":{"instanceId":"configured-instance","model":"configured-model"},"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture."}`)
	_, err := s.CreateSpec(ctx, "task", "Work", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.ErrorIs(t, err, storage.ErrSpecNotApproved)
	var count int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_preparations WHERE project_slug='run-preparation-retry'`).Scan(&count))
	require.Zero(t, count, "failed preparations cannot retain a success receipt")
	approved := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	type preparation struct {
		id       string
		replayed bool
		err      error
	}
	results := make(chan preparation, 2)
	for range 2 {
		go func() {
			id, replayed, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
			results <- preparation{id, replayed, err}
		}()
	}
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, first.id, second.id)
	require.NotEqual(t, first.replayed, second.replayed, "one request creates and the other replays")
	run := first.id
	claim, err := s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	packageBefore, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	var packageFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(packageBefore.Body, &packageFields))
	require.JSONEq(t, string(target), string(packageFields["dispatch_target"]))
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 1)
	require.NotNil(t, metadata.Runs[0].DispatchMessageID)
	require.Equal(t, "dispatch-message", *metadata.Runs[0].DispatchMessageID)
	again, replayed, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, run, again)
	claimAfter, err := s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, claim, claimAfter)
	packageAfter, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	require.Equal(t, packageBefore, packageAfter)
	var changedPurpose map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(target, &changedPurpose))
	changedPurpose["workPurpose"] = json.RawMessage(`"implementation"`)
	changedTarget, err := json.Marshal(changedPurpose)
	require.NoError(t, err)
	_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", changedTarget)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "recovery cannot change the recorded work purpose")
	_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", json.RawMessage(`{"environmentId":"other","workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture."}`))
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "a retry cannot silently choose another target")
	_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", nil)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "a retry cannot discard the recorded target")
	_, replayed, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", json.RawMessage(`{"purposeGuidance":"Write requirements for this lifecycle fixture.","workPurpose":"requirements","messageId":"dispatch-message","modelSelection":{"model":"configured-model","instanceId":"configured-instance"},"threadId":"native-thread","projectId":"native-project","environmentId":"local"}`))
	require.NoError(t, err)
	require.True(t, replayed, "JSON object key order is not a changed target")
	for _, invalid := range []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`[]`), json.RawMessage(`"target"`)} {
		_, _, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "invalid-target", invalid)
		require.ErrorIs(t, err, storage.ErrInvalidRunPreparation)
	}
	for _, input := range [][3]string{{"task", "other-workspace", "operator"}, {"task", "workspace", "other-operator"}, {"other-task", "workspace", "operator"}} {
		_, _, err = s.PrepareRunForOperator(ctx, input[0], input[1], input[2], "prepare", target)
		require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	}
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, run, "other", "native-thread"), storage.ErrRunBindingConflict, "first bind must respect the prepared environment")
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, run, "local", "other-thread"), storage.ErrRunBindingConflict, "first bind must respect the prepared thread")
	require.ErrorIs(t, s.BindRunThread(ctx, run, "native-thread"), storage.ErrRunBindingConflict, "legacy binding cannot discard the planned environment")
	var state, thread, environment string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT state,thread_ref,environment_id FROM run_bindings WHERE id=$1`, run).Scan(&state, &thread, &environment))
	require.Equal(t, "prepared", state)
	require.Empty(t, thread)
	require.Empty(t, environment)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "native-thread"))
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "native-thread"))
	require.NoError(t, s.UnclaimSpec(ctx, "task", run))
	again, replayed, err = s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, run, again)
	claimAfter, err = s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	require.Nil(t, claimAfter, "retry does not restart a released execution")
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_bindings WHERE project_slug='run-preparation-retry'`).Scan(&count))
	require.Equal(t, 1, count)
}
