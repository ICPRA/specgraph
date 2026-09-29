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

func TestCancelPreparationFencesLateDispatchAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("cancel-preparation"))
	_, err := s.CreateSpec(ctx, "task", "Goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture.","version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
	pkg, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	require.NoError(t, s.CancelRunPreparation(ctx, run, pkg.PackageID, "operator", "Native create was rejected"))
	require.NoError(t, s.CancelRunPreparation(ctx, run, pkg.PackageID, "operator", "Native create was rejected"))
	require.ErrorIs(t, s.CancelRunPreparation(ctx, run, pkg.PackageID, "other", "Native create was rejected"), storage.ErrRunBindingConflict)
	status, err := s.ReadRunDispatch(ctx, run)
	require.NoError(t, err)
	require.Nil(t, status.Admission)
	require.NotNil(t, status.Cancellation)
	require.Equal(t, "operator", status.Cancellation.Actor)
	_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.ClaimSpec(ctx, "task", run, 0)
	require.ErrorIs(t, err, storage.ErrDispatchResolved)
	_, err = s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	previous, replayed, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, run, previous)
	newTarget := json.RawMessage(`{"workPurpose":"requirements","purposeGuidance":"Write requirements for this lifecycle fixture.","version":1,"promptFormatVersion":"vacpms-run-v2","environmentId":"env","threadId":"new-thread","workspace":"workspace","createCommandId":"new-create","startCommandId":"new-start","messageId":"new-message"}`)
	newRun, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "new-prepare", newTarget)
	require.NoError(t, err)
	require.NotEqual(t, run, newRun)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, newRun, "env", "new-thread"))
	newPackage, err := s.ReadRunContext(ctx, newRun)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, newRun, "operator", newPackage.PackageID, newTarget)
	require.NoError(t, err)
	require.ErrorIs(t, s.CancelRunPreparation(ctx, newRun, newPackage.PackageID, "operator", "cancel"), storage.ErrDispatchResponsibilityHeld)
}
