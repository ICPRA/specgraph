// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestNodeOwnershipHandoff(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("node-ownership"))
	require.NoError(t, s.SetProjectManaged(ctx, "node-ownership", true))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	first, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "First manager", Role: "admin"}, nil)
	require.NoError(t, err)
	second, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Second manager", Role: "admin"}, nil)
	require.NoError(t, err)
	firstCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: first.ID, UserKind: storage.KindHuman})
	secondCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: second.ID, UserKind: storage.KindHuman})
	makeTask := func(slug string) *storage.Spec {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Original "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		spec, err := s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		return spec
	}
	take := func(actor context.Context, slug string, version int32, owner *string, key string) *storage.NodeOwnershipOperation {
		t.Helper()
		op, err := s.BeginNodeOwnershipTake(actor, storage.BeginNodeOwnershipTakeRequest{
			TaskSlug: slug, ExpectedVersion: version, ExpectedOwnerUserID: owner, IdempotencyKey: key, Reason: "Named responsibility change",
		})
		require.NoError(t, err)
		return op
	}

	plain := makeTask("plain-work")
	firstTake := take(firstCtx, plain.Slug, plain.Version, nil, "plain-first")
	require.Equal(t, "committed", firstTake.Status)
	require.Equal(t, first.ID, *firstTake.AfterOwnerUserID)
	replay, err := s.BeginNodeOwnershipTake(firstCtx, storage.BeginNodeOwnershipTakeRequest{
		TaskSlug: plain.Slug, ExpectedVersion: plain.Version, IdempotencyKey: "plain-first", Reason: "Named responsibility change",
	})
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, firstTake.ID, replay.ID)
	_, err = s.BeginNodeOwnershipTake(secondCtx, storage.BeginNodeOwnershipTakeRequest{
		TaskSlug: plain.Slug, ExpectedVersion: plain.Version, ExpectedOwnerUserID: &first.ID, IdempotencyKey: "plain-stale", Reason: "Stale view cannot replace the owner",
	})
	require.ErrorIs(t, err, storage.ErrNodeOwnershipConflict)
	secondTake := take(secondCtx, plain.Slug, *firstTake.ToVersion, &first.ID, "plain-second")
	require.Equal(t, first.ID, *secondTake.BeforeOwnerUserID)
	require.Equal(t, second.ID, *secondTake.AfterOwnerUserID)
	manualVersion, _, err := s.ManualComplete(ctx, plain.Slug, second.ID, *secondTake.ToVersion, "plain-manual", "Human completed the work")
	require.NoError(t, err)
	returned, err := s.ReturnNodeOwnership(firstCtx, storage.ReturnNodeOwnershipRequest{
		TaskSlug: plain.Slug, ExpectedVersion: manualVersion, ExpectedOwnerUserID: &second.ID, IdempotencyKey: "plain-return", Reason: "Clear named owner after completion",
	})
	require.NoError(t, err)
	require.Nil(t, returned.AfterOwnerUserID)
	plainCurrent, err := s.GetSpec(ctx, plain.Slug)
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageDone, plainCurrent.Stage)
	plainState, err := s.ReadNodeOwnership(ctx, plain.Slug)
	require.NoError(t, err)
	require.Nil(t, plainState.HumanOwnerUserID)

	raw := makeTask("raw-prepared")
	rawRun, err := s.PrepareRun(ctx, raw.Slug, "C:/raw-prepared")
	require.NoError(t, err)
	rawContext, err := s.ReadRunContext(ctx, rawRun)
	require.NoError(t, err)
	rawPending := take(firstCtx, raw.Slug, raw.Version, nil, "raw-pending")
	require.Equal(t, "pending", rawPending.Status)
	require.Len(t, rawPending.Preparations, 1)
	require.True(t, rawPending.Preparations[0].Raw)
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, rawRun, "local", "raw-late-thread"), storage.ErrNodeOwnershipPending)
	require.NoError(t, s.CancelRunPreparation(ctx, rawRun, rawContext.PackageID, first.ID, "Cancel never-admitted raw preparation"))
	rawCommitted, err := s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{
		OperationID: rawPending.ID, ExpectedVersion: raw.Version, Handoffs: []storage.NodeOwnershipHandoff{}, PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{},
	})
	require.NoError(t, err)
	require.Equal(t, "committed", rawCommitted.Status)
	_, err = s.PrepareRun(ctx, raw.Slug, "C:/raw-new")
	require.ErrorIs(t, err, storage.ErrNodeOwnershipConflict)

	bound := makeTask("raw-bound")
	boundRun, err := s.PrepareRun(ctx, bound.Slug, "C:/raw-bound")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, boundRun, "local", "raw-bound-thread"))
	boundContext, err := s.ReadRunContext(ctx, boundRun)
	require.NoError(t, err)
	boundPending := take(firstCtx, bound.Slug, bound.Version, nil, "bound-pending")
	require.Equal(t, "bound", boundPending.Preparations[0].State)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, boundRun, "local", "raw-bound-thread"), "exact bind replay remains valid")
	require.NoError(t, s.RecordProgress(ctx, bound.Slug, boundRun, "Bound raw run handed off its phase"))
	events, err := s.GetExecutionEvents(ctx, bound.Slug, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	boundCommitted, err := s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{
		OperationID: boundPending.ID, ExpectedVersion: bound.Version, Handoffs: []storage.NodeOwnershipHandoff{},
		PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{{RunID: boundRun, PackageID: boundContext.PackageID,
			Summary: storage.NodeOwnershipSummary{Kind: "progress", ProgressEventID: &events[0].ID}, StopNote: "Observed old bound thread stopped"}},
	})
	require.NoError(t, err)
	require.Equal(t, "committed", boundCommitted.Status)
	var boundState string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT state FROM run_bindings WHERE project_slug=$1 AND id=$2`, "node-ownership", boundRun).Scan(&boundState))
	require.Equal(t, "preparation_cancelled", boundState)

	claimOnly := makeTask("claim-only")
	claim, err := s.ClaimSpec(ctx, claimOnly.Slug, "legacy-claim-agent", time.Hour)
	require.NoError(t, err)
	claimPending := take(firstCtx, claimOnly.Slug, claimOnly.Version, nil, "claim-pending")
	require.NotNil(t, claimPending.FrozenClaim)
	require.Equal(t, claim.ClaimedAt, claimPending.FrozenClaim.ClaimedAt)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET claimed_at=claimed_at+interval '1 second' WHERE project_slug=$1 AND spec_slug=$2`, "node-ownership", claimOnly.Slug)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordProgress(ctx, claimOnly.Slug, claim.Agent, "Wrong claim instance"), storage.ErrNodeOwnershipPending)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET claimed_at=$3 WHERE project_slug=$1 AND spec_slug=$2`, "node-ownership", claimOnly.Slug, claim.ClaimedAt)
	require.NoError(t, err)
	require.NoError(t, s.RecordProgress(ctx, claimOnly.Slug, claim.Agent, "Legacy claimant supplied exact phase"))
	claimEvents, err := s.GetExecutionEvents(ctx, claimOnly.Slug, 0)
	require.NoError(t, err)
	claimCommitted, err := s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{
		OperationID: claimPending.ID, ExpectedVersion: claimOnly.Version, Handoffs: []storage.NodeOwnershipHandoff{}, PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{},
		ClaimHandoff: &storage.NodeOwnershipClaimHandoff{Agent: claim.Agent, ClaimedAt: claim.ClaimedAt,
			Summary: storage.NodeOwnershipSummary{Kind: "progress", ProgressEventID: &claimEvents[0].ID}, StopNote: "Observed legacy claimant stopped"},
	})
	require.NoError(t, err)
	require.Equal(t, "committed", claimCommitted.Status)
	remainingClaim, err := s.GetActiveClaim(ctx, claimOnly.Slug)
	require.NoError(t, err)
	require.Nil(t, remainingClaim)

	admitted := makeTask("admitted-work")
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate the bounded handoff fixture.","environmentId":"local","threadId":"owner-thread","projectId":"native","workspace":"C:/admitted-work","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	run, _, err := s.PrepareRunForOperator(ctx, admitted.Slug, "C:/admitted-work", first.ID, "admitted-prepare", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "owner-thread"))
	prepared, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	grant, err := s.AuthorizeRunDispatch(ctx, run, first.ID, prepared.PackageID, target)
	require.NoError(t, err)
	pending := take(firstCtx, admitted.Slug, admitted.Version, nil, "admitted-take")
	require.Len(t, pending.Dispatches, 1)
	require.Equal(t, grant.ID, pending.Dispatches[0].AdmissionID)
	scope := storage.MailScope{EnvironmentID: "local", ThreadID: "owner-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	summaryReq := storage.RecordOwnNodeHandoffSummaryRequest{EventID: "owner-progress-1", Message: "Completed the current phase and handed over"}
	project, err := s.ProjectForOwnHandoffSummary(firstCtx, scope, "native", summaryReq)
	require.NoError(t, err)
	require.Equal(t, "node-ownership", project)
	summary, err := s.RecordOwnNodeHandoffSummary(firstCtx, scope, "native", summaryReq)
	require.NoError(t, err)
	require.Equal(t, run, summary.RunID)
	require.False(t, summary.Replayed)
	require.NoError(t, s.ResolveRunDispatch(ctx, run, grant.ID, first.ID, "isolated_workspace", "Workspace isolated but old process not confirmed stopped"))
	_, err = s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{OperationID: pending.ID, ExpectedVersion: admitted.Version,
		Handoffs:         []storage.NodeOwnershipHandoff{{RunID: run, AdmissionID: grant.ID, Summary: storage.NodeOwnershipSummary{Kind: "progress", ProgressEventID: &summary.EventID}}},
		PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{}})
	require.ErrorIs(t, err, storage.ErrNodeOwnershipHandoffRequired)
	require.NoError(t, s.ConfirmRunStopped(ctx, run, grant.ID, "local", "owner-thread", first.ID, "Observed original thread stopped"))
	project, err = s.ProjectForOwnHandoffSummary(firstCtx, scope, "native", summaryReq)
	require.NoError(t, err, "stopped run can resolve only its exact prior event")
	summaryReplay, err := s.RecordOwnNodeHandoffSummary(firstCtx, scope, "native", summaryReq)
	require.NoError(t, err)
	require.True(t, summaryReplay.Replayed)
	completedTake, err := s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{OperationID: pending.ID, ExpectedVersion: admitted.Version,
		Handoffs:         []storage.NodeOwnershipHandoff{{RunID: run, AdmissionID: grant.ID, Summary: storage.NodeOwnershipSummary{Kind: "progress", ProgressEventID: &summary.EventID}}},
		PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{}})
	require.NoError(t, err)
	require.Equal(t, "committed", completedTake.Status)
	completedReplay, err := s.CommitNodeOwnershipTake(firstCtx, storage.CommitNodeOwnershipTakeRequest{OperationID: pending.ID, ExpectedVersion: admitted.Version,
		Handoffs:         []storage.NodeOwnershipHandoff{{RunID: run, AdmissionID: grant.ID, Summary: storage.NodeOwnershipSummary{Kind: "progress", ProgressEventID: &summary.EventID}}},
		PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{}})
	require.NoError(t, err)
	require.True(t, completedReplay.Replayed)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{}`), first.ID)
	require.ErrorIs(t, err, storage.ErrNodeOwnershipConflict)
	require.ErrorIs(t, s.RecordCompletion(ctx, admitted.Slug, run), storage.ErrNodeOwnershipConflict)
	read, err := s.ReadNodeOwnership(ctx, admitted.Slug)
	require.NoError(t, err)
	require.Equal(t, first.ID, *read.HumanOwnerUserID)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	found := false
	for _, item := range metadata.NodeOwners {
		if item.TaskSlug == admitted.Slug {
			found = true
			require.Equal(t, first.ID, *item.HumanOwnerUserID)
		}
	}
	require.True(t, found)
}

func TestNodeOwnershipProgramStopUsesCurrentAttempt(t *testing.T) {
	f := newProgramLoopFixture(t, "node-owner-program")
	first := f.admit(f.prepare("program-work", mechanicalLoop()))
	firstAttempt := first.CurrentAttempt.ID
	f.exit(first, 0)
	second, err := f.s.AuthorizeNextProgramAttempt(f.host, f.scope, first.Context.RunID, firstAttempt)
	require.NoError(t, err)
	require.True(t, second.MayStart)
	secondAttempt := second.Run.CurrentAttempt.ID
	versioned, err := f.s.GetSpec(f.human, "program-work")
	require.NoError(t, err)
	op, err := f.s.BeginNodeOwnershipTake(f.human, storage.BeginNodeOwnershipTakeRequest{
		TaskSlug: "program-work", ExpectedVersion: versioned.Version, IdempotencyKey: "program-take", Reason: "Named human program handoff",
	})
	require.NoError(t, err)
	require.Equal(t, "pending", op.Status)
	require.Equal(t, secondAttempt, *op.Dispatches[0].AttemptID)
	f.exit(second.Run, 0)
	_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, first.Context.RunID, secondAttempt)
	require.ErrorIs(t, err, storage.ErrNodeOwnershipPending, "pending human takeover cannot grant a third physical attempt")
	state, err := f.s.ReadNodeOwnership(f.human, "program-work")
	require.NoError(t, err)
	require.Equal(t, secondAttempt, *state.DispatchObservations[0].CurrentAttemptID)
	require.NotNil(t, state.DispatchObservations[0].ProgramResult)
	wrong := storage.ConfirmProgramRunStoppedRequest{RunID: first.Context.RunID, AdmissionID: op.Dispatches[0].AdmissionID,
		AttemptID: firstAttempt, EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID, Note: "Observed current program stopped"}
	_, err = f.s.ConfirmProgramRunStopped(f.human, wrong)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	wrong.AttemptID = secondAttempt
	wrong.NativeProjectID = "another-native-project"
	_, err = f.s.ConfirmProgramRunStopped(f.human, wrong)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	wrong.NativeProjectID = f.scope.NativeProjectID
	require.NoError(t, f.s.ResolveRunDispatch(f.human, first.Context.RunID, op.Dispatches[0].AdmissionID, f.consumerOwner(), "isolated_workspace", "Workspace isolated before physical stop was observed"))
	stopped, err := f.s.ConfirmProgramRunStopped(f.human, wrong)
	require.NoError(t, err)
	require.NotNil(t, stopped.StopConfirmation)
	require.Equal(t, secondAttempt, *stopped.StopConfirmation.ProgramAttemptID)
	require.Equal(t, "isolated_workspace", stopped.Resolution.Kind)
	stopReplay, err := f.s.ConfirmProgramRunStopped(f.human, wrong)
	require.NoError(t, err)
	require.Equal(t, secondAttempt, *stopReplay.StopConfirmation.ProgramAttemptID)
	_, err = f.s.CommitNodeOwnershipTake(f.human, storage.CommitNodeOwnershipTakeRequest{OperationID: op.ID, ExpectedVersion: versioned.Version,
		Handoffs: []storage.NodeOwnershipHandoff{{RunID: first.Context.RunID, AdmissionID: op.Dispatches[0].AdmissionID,
			Summary: storage.NodeOwnershipSummary{Kind: "program_result", ProgramAttemptID: &secondAttempt}}},
		PreparedHandoffs: []storage.NodeOwnershipPreparedHandoff{}})
	require.NoError(t, err)
	var attempts int
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT count(*) FROM program_attempts WHERE project_slug=$1 AND run_id=$2`, "node-owner-program", first.Context.RunID).Scan(&attempts))
	require.Equal(t, 2, attempts)
}
