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

func TestDispatchAuthorizationRetainsUnknownResponsibilityAfterExpiry(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("dispatch-responsibility"))
	require.NoError(t, s.SetProjectManaged(ctx, "dispatch-responsibility", true))
	_, err := s.CreateSpec(ctx, "task", "Goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	spec, err := s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this ordinary lifecycle fixture.","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
	pkg, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	admission, err := s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.NoError(t, err)
	require.False(t, admission.Replayed)
	delivery, err := s.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
	require.NoError(t, err)
	require.NoError(t, s.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
	replayed, err := s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, admission.ID, replayed.ID)
	_, err = s.AuthorizeRunDispatch(ctx, run, "other-operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug='dispatch-responsibility' AND spec_slug='task'`)
	require.NoError(t, err)
	released, err := s.ReleaseExpiredClaims(ctx)
	require.NoError(t, err)
	require.Zero(t, released)
	ready, err := s.GetReady(ctx)
	require.NoError(t, err)
	require.Empty(t, ready)
	_, err = s.PrepareRun(ctx, "task", "new-workspace")
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	require.ErrorIs(t, s.UnclaimSpec(ctx, "task", run), storage.ErrDispatchResponsibilityHeld)
	_, _, err = s.ManualComplete(ctx, "task", "operator", spec.Version, "manual", "done")
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrAgentNotClaimOwner)
	require.ErrorIs(t, s.ResolveRunDispatch(ctx, run, admission.ID, "operator", "host_confirmed", "assertion"), storage.ErrInvalidRunPreparation)
	require.NoError(t, s.ResolveRunDispatch(ctx, run, admission.ID, "operator", "isolated_workspace", "Original workspace isolated"))
	require.NoError(t, s.ResolveRunDispatch(ctx, run, admission.ID, "operator", "isolated_workspace", "Original workspace isolated"))
	require.ErrorIs(t, s.ResolveRunDispatch(ctx, run, admission.ID, "operator", "isolated_workspace", "Different assertion"), storage.ErrRunBindingConflict)
	_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrDispatchResolved)
	var runState string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT state FROM run_bindings WHERE id=$1`, run).Scan(&runState))
	require.Equal(t, "handed_off", runState, "business handoff, not a claim that the native process stopped")
	_, err = s.ClaimSpec(ctx, "task", run, 0)
	require.ErrorIs(t, err, storage.ErrDispatchResolved, "released attempts cannot reclaim even with an old accepted delivery")
	require.ErrorIs(t, s.RecordCompletion(ctx, "task", run), storage.ErrAgentNotClaimOwner)
	newRun, err := s.PrepareRun(ctx, "task", "new-workspace")
	require.NoError(t, err)
	require.NotEqual(t, run, newRun)
	original, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	require.Equal(t, pkg, original)
}

func TestDispatchAuthorizationRejectsChangedPreparedRequirements(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("dispatch-stale"))
	_, err := s.CreateSpec(ctx, "task", "Original", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this ordinary lifecycle fixture.","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
	pkg, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	intent := "Revised requirement"
	_, err = s.UpdateSpec(ctx, "task", &intent, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrCompletionRequiresRequirementReview)
	var count int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM run_dispatches WHERE project_slug='dispatch-stale'`).Scan(&count))
	require.Zero(t, count)
}

func TestBusinessCompletionDoesNotProveDispatchHasStopped(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("dispatch-completion"))
	require.NoError(t, s.SetProjectManaged(ctx, "dispatch-completion", true))
	_, err := s.CreateSpec(ctx, "task", "Goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this ordinary lifecycle fixture.","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
	pkg, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	admission, err := s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.NoError(t, err)
	delivery, err := s.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
	require.NoError(t, err)
	require.NoError(t, s.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
	require.NoError(t, s.RecordCompletion(ctx, "task", run))
	var held bool
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT released_at IS NULL FROM run_dispatches WHERE run_id=$1`, run).Scan(&held))
	require.True(t, held)
	require.NoError(t, s.ResolveRunDispatch(ctx, run, admission.ID, "operator", "stopped_writing", "Operator verified writer stopped"))
	var state string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT state FROM run_bindings WHERE id=$1`, run).Scan(&state))
	require.Equal(t, "completed", state, "operator handoff must not overwrite completed business history")
}

func TestDispatchStopConfirmationGuardsAbandon(t *testing.T) {
	for _, releaseKind := range []string{"", "isolated_workspace", "stopped_writing"} {
		t.Run("prior-"+releaseKind, func(t *testing.T) {
			ctx := context.Background()
			s := newStore(t, postgres.WithProject("stop-confirm-"+releaseKind))
			for _, slug := range []string{"task", "downstream"} {
				_, err := s.CreateSpec(ctx, slug, "Original "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
			}
			stage := "approved"
			_, err := s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
			require.NoError(t, err)
			_, err = s.AddEdge(ctx, "downstream", "task", storage.EdgeTypeDependsOn)
			require.NoError(t, err)
			downstream, err := s.GetSpec(ctx, "downstream")
			require.NoError(t, err)
			target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this ordinary lifecycle fixture.","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
			run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
			require.NoError(t, err)
			require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
			pkg, err := s.ReadRunContext(ctx, run)
			require.NoError(t, err)
			admission, err := s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
			require.NoError(t, err)
			if releaseKind != "" {
				require.NoError(t, s.ResolveRunDispatch(ctx, run, admission.ID, "earlier-operator", releaseKind, "Earlier release reason"))
			}
			before, err := s.ReadRunDispatch(ctx, run)
			require.NoError(t, err)
			require.Nil(t, before.StopConfirmation)
			_, err = s.LifecycleAbandonSpec(ctx, "task", "No longer applicable")
			require.ErrorIs(t, err, storage.ErrAbandonExecutionPending, "neither isolation nor an older stopped_writing assertion is explicit stop confirmation")
			require.ErrorIs(t, s.ConfirmRunStopped(ctx, run, admission.ID, "other-env", "thread", "operator", "Observed stopped"), storage.ErrRunBindingConflict)
			require.ErrorIs(t, s.ConfirmRunStopped(ctx, run, "other-admission", "env", "thread", "operator", "Observed stopped"), storage.ErrRunBindingConflict)
			require.NoError(t, s.ConfirmRunStopped(ctx, run, admission.ID, "env", "thread", "operator", "Observed stopped"))
			after, err := s.ReadRunDispatch(ctx, run)
			require.NoError(t, err)
			require.NotNil(t, after.StopConfirmation)
			require.Equal(t, "operator", after.StopConfirmation.Actor)
			require.Equal(t, "Observed stopped", after.StopConfirmation.Note)
			require.NotZero(t, after.StopConfirmation.ConfirmedAt)
			if before.Resolution != nil {
				require.Equal(t, before.Resolution, after.Resolution, "stop confirmation must preserve prior isolation/release audit")
			} else {
				require.Equal(t, "stopped_writing", after.Resolution.Kind)
			}
			require.NoError(t, s.ConfirmRunStopped(ctx, run, admission.ID, "env", "thread", "operator", "Observed stopped"))
			replayed, err := s.ReadRunDispatch(ctx, run)
			require.NoError(t, err)
			require.Equal(t, after, replayed, "repeat must not rewrite confirmation timestamps or history")
			require.ErrorIs(t, s.ConfirmRunStopped(ctx, run, admission.ID, "env", "thread", "operator", "Different observation"), storage.ErrRunBindingConflict)
			claim, err := s.GetActiveClaim(ctx, "task")
			require.NoError(t, err)
			require.Nil(t, claim)
			abandoned, err := s.LifecycleAbandonSpec(ctx, "task", "No longer applicable")
			require.NoError(t, err)
			require.Equal(t, storage.SpecStageAbandoned, abandoned.Stage)
			changes, err := s.ListChanges(ctx, "task", storage.ChangeLogFilter{SinceVersion: abandoned.Version - 1})
			require.NoError(t, err)
			require.Len(t, changes, 1)
			require.Equal(t, "No longer applicable", changes[0].Reason)
			unchanged, err := s.GetSpec(ctx, "downstream")
			require.NoError(t, err)
			require.Equal(t, downstream, unchanged)
			_, err = s.ClaimSpec(ctx, "task", "new-agent", 0)
			require.ErrorIs(t, err, storage.ErrSpecTerminal, "abandoned nodes cannot regain claim metadata")
			_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
			require.ErrorIs(t, err, storage.ErrDispatchResolved)
		})
	}
}

func TestUnadmittedPreparationCanBeAbandonedWithoutStopRecord(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("abandon-unadmitted"))
	_, err := s.CreateSpec(ctx, "task", "Never authorized to start", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	stage := "approved"
	_, err = s.UpdateSpec(ctx, "task", nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this ordinary lifecycle fixture.","environmentId":"env","threadId":"thread","workspace":"workspace","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "task", "workspace", "operator", "prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "env", "thread"))
	pkg, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	claim, err := s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	require.Equal(t, run, claim.Agent)
	abandoned, err := s.LifecycleAbandonSpec(ctx, "task", "Unstarted work withdrawn")
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageAbandoned, abandoned.Stage)
	claim, err = s.GetActiveClaim(ctx, "task")
	require.NoError(t, err)
	require.Nil(t, claim)
	_, err = s.AuthorizeRunDispatch(ctx, run, "operator", pkg.PackageID, target)
	require.ErrorIs(t, err, storage.ErrSpecNotApproved)
	status, err := s.ReadRunDispatch(ctx, run)
	require.NoError(t, err)
	require.Nil(t, status.Admission)
	require.Nil(t, status.StopConfirmation)
	require.Nil(t, status.Cancellation)
	retained, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	require.Equal(t, pkg, retained, "preparation history remains without fabricated stop/cancellation")
	_, err = s.ClaimSpec(ctx, "task", "late-agent", 0)
	require.ErrorIs(t, err, storage.ErrSpecTerminal)
}
