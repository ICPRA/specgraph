// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestRunEnvironmentBindingAndMail(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("run-environments"))
	_, err := s.CreateSpec(ctx, "task", "Bound environment fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO run_bindings(project_slug,id,task_spec_slug,state) VALUES
		('run-environments','environment-run-a','task','prepared'),
		('run-environments','environment-run-b','task','prepared'),
		('run-environments','environment-legacy','task','prepared'),
		('run-environments','environment-cancelled','task','cancelled'),
		('run-environments','environment-stale','task','prepared'),
		('run-environments','environment-race','task','prepared')`)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-a", "same-thread"))
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, "environment-run-b", "env-b", "same-thread"))
	require.NoError(t, s.BindRunThread(ctx, "environment-legacy", "legacy-thread"))
	require.NoError(t, s.BindRunThread(ctx, "environment-legacy", "legacy-thread"))
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, "environment-legacy", "env-a", "legacy-thread"), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.BindRunThread(ctx, "environment-run-a", "same-thread"), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-b", "same-thread"), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-a", "other-thread"), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, "environment-cancelled", "env-a", "thread"), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, "missing", "env-a", "thread"), storage.ErrRunBindingNotFound)
	other := newStore(t, postgres.WithProject("run-environments-other"))
	require.ErrorIs(t, other.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-a", "same-thread"), storage.ErrRunBindingNotFound)

	var before, after time.Time
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT updated_at FROM run_bindings WHERE id='environment-run-a'`).Scan(&before))
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-a", "same-thread"))
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT updated_at FROM run_bindings WHERE id='environment-run-a'`).Scan(&after))
	require.Equal(t, before, after, "replay must not reset the binding timestamp")
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	for _, run := range metadata.Runs {
		switch run.ID {
		case "environment-run-a":
			require.Equal(t, "env-a", run.EnvironmentID)
		case "environment-run-b":
			require.Equal(t, "env-b", run.EnvironmentID)
		case "environment-legacy":
			require.Empty(t, run.EnvironmentID)
		}
	}

	grantA, err := s.GrantMailBridge(ctx, "env-a", "apikey:bridge", "operator")
	require.NoError(t, err)
	grantB, err := s.GrantMailBridge(ctx, "env-b", "apikey:bridge", "operator")
	require.NoError(t, err)
	scopeA := storage.MailScope{EnvironmentID: "env-a", ThreadID: "same-thread", ProviderSessionID: "session", ProviderInstanceID: "instance"}
	scopeB := scopeA
	scopeB.EnvironmentID = "env-b"
	_, err = s.ApproveMailBinding(ctx, grantB.ID, "environment-run-a", scopeB, "operator")
	require.ErrorIs(t, err, storage.ErrMailConflict)
	_, err = s.ApproveMailBinding(ctx, grantA.ID, "environment-run-a", scopeA, "operator")
	require.NoError(t, err)
	_, err = s.ApproveMailBinding(ctx, grantB.ID, "environment-run-b", scopeB, "operator")
	require.NoError(t, err)
	legacyScope := scopeB
	legacyScope.ThreadID = "legacy-thread"
	_, err = s.ApproveMailBinding(ctx, grantB.ID, "environment-legacy", legacyScope, "operator")
	require.NoError(t, err, "legacy empty-environment runs retain explicitly granted mail access")
	for _, test := range []struct {
		scope storage.MailScope
		run   string
	}{{scopeA, "environment-run-a"}, {scopeB, "environment-run-b"}, {legacyScope, "environment-legacy"}} {
		called := false
		require.NoError(t, s.WithMailActor(ctx, "apikey:bridge", test.scope, func(_ context.Context, actor storage.MailActor) error {
			called = true
			require.Equal(t, test.run, actor.RunID)
			return nil
		}))
		require.True(t, called)
	}
	// A previously persisted mismatching approval must not bypass resolution.
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, "environment-stale", "env-a", "stale-thread"))
	_, err = s.Pool().Exec(ctx, `INSERT INTO mail_bindings(project_slug,id,grant_id,environment_id,thread_id,provider_session_id,provider_instance_id,run_id,approved_by,approved_at)
		VALUES('run-environments','environment-stale-approval',$1,'env-b','stale-thread','session','instance','environment-stale','fixture',now())`, grantB.ID)
	require.NoError(t, err)
	staleScope := scopeB
	staleScope.ThreadID = "stale-thread"
	require.ErrorIs(t, s.WithMailActor(ctx, "apikey:bridge", staleScope, func(context.Context, storage.MailActor) error {
		t.Fatal("mismatching environment resolved an actor")
		return nil
	}), storage.ErrMailForbidden)

	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='completed' WHERE id='environment-run-a'`)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, "environment-run-a", "env-a", "same-thread"))
	var state string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT state,updated_at FROM run_bindings WHERE id='environment-run-a'`).Scan(&state, &after))
	require.Equal(t, "completed", state)
	require.Equal(t, before, after)

	results := make(chan error, 2)
	for _, environment := range []string{"env-a", "env-b"} {
		go func() {
			results <- s.BindRunThreadInEnvironment(ctx, "environment-race", environment, "same-thread")
		}()
	}
	first, second := <-results, <-results
	if first == nil {
		require.ErrorIs(t, second, storage.ErrRunBindingConflict)
	} else {
		require.ErrorIs(t, first, storage.ErrRunBindingConflict)
		require.NoError(t, second)
	}
}
