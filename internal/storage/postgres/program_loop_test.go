// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

// All results below are synthetic observations; no fixture command is executed.
type programLoopFixture struct {
	t           *testing.T
	s           *postgres.Store
	human, host context.Context
	consumer    *auth.Identity
	scope       storage.ProgramHostScope
	command     storage.ProgramCommand
}

func TestProgramLoopJudgmentCASAndCompletionProof(t *testing.T) {
	f := newProgramLoopFixture(t, "program-judgment-cas")
	r := f.admit(f.prepare("completed", judgmentLoop(2)))
	f.exit(r, 0)
	j1, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "true", "Original satisfied judgment", nil)
	require.NoError(t, err)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
	require.NoError(t, err)
	completed, err := f.s.ReadProgramLoop(f.human, r.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, "completed", completed.LoopState.Status)
	require.Equal(t, completed.Completion, completed.LoopState.Completion)
	require.Equal(t, r.CurrentAttempt.ID, *completed.Completion.AttemptID)
	require.Equal(t, j1.ID, *completed.Completion.JudgmentID)
	f.assertMetadata(completed)
	var count int
	var originalBasis, originalContext []byte
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT count(*) FROM execution_events WHERE project_slug='program-judgment-cas' AND spec_slug='completed' AND agent=$1 AND event_type='completion'`, r.Context.RunID).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT program_completion_basis,completion_context FROM execution_events WHERE id=$1`, completed.Completion.EventID).Scan(&originalBasis, &originalContext))
	require.JSONEq(t, `{"attemptId":"`+r.CurrentAttempt.ID+`","judgmentId":"`+j1.ID+`"}`, string(originalBasis))
	j2, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "false", "Later correction cannot alter prior completion", &j1.ID)
	require.NoError(t, err)
	require.Equal(t, j1.ID, *j2.PredecessorID)
	_, err = f.s.StopProgramLoop(f.human, r.Context.RunID, "Stop only future attempts")
	require.NoError(t, err)
	current, err := f.s.ReadProgramLoop(f.human, r.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, "false", current.LoopState.Condition.Value)
	require.Equal(t, j2.ID, current.LoopState.Condition.JudgmentID)
	require.Equal(t, "completed", current.LoopState.Status)
	require.Equal(t, j1.ID, *current.Completion.JudgmentID)
	f.assertMetadata(current)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
	require.NoError(t, err, "replay consumes the original event basis, not later judgment or stop")
	var replayBasis, replayContext []byte
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT program_completion_basis,completion_context FROM execution_events WHERE id=$1`, completed.Completion.EventID).Scan(&replayBasis, &replayContext))
	require.JSONEq(t, string(originalBasis), string(replayBasis))
	require.JSONEq(t, string(originalContext), string(replayContext))
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT count(*) FROM execution_events WHERE project_slug='program-judgment-cas' AND spec_slug='completed' AND agent=$1 AND event_type='completion'`, r.Context.RunID).Scan(&count))
	require.Equal(t, 1, count)
	_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
	require.ErrorIs(t, err, storage.ErrProgramLoopStopped)
	_, err = f.s.Pool().Exec(f.human, `UPDATE execution_events SET program_completion_basis=NULL WHERE id=$1`, completed.Completion.EventID)
	require.NoError(t, err)
	legacy, err := f.s.ReadProgramLoop(f.human, r.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, "completed", legacy.LoopState.Status)
	require.Equal(t, completed.Completion.EventID, legacy.Completion.EventID)
	require.Nil(t, legacy.Completion.AttemptID)
	require.Nil(t, legacy.Completion.JudgmentID)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
	require.NoError(t, err, "legacy exact completion replays event-only without inventing a basis")
	var absentBasis []byte
	require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT program_completion_basis FROM execution_events WHERE id=$1`, completed.Completion.EventID).Scan(&absentBasis))
	require.Empty(t, absentBasis)

	other := f.admit(f.prepare("correct-before-completion", judgmentLoop(2)))
	f.exit(other, 0)
	first, err := f.s.RecordProgramLoopJudgment(f.human, other.Context.RunID, other.CurrentAttempt.ID, "true", "Initially satisfied", nil)
	require.NoError(t, err)
	second, err := f.s.RecordProgramLoopJudgment(f.human, other.Context.RunID, other.CurrentAttempt.ID, "false", "Correction before completion", &first.ID)
	require.NoError(t, err)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, other.Context.RunID)
	require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
	oldReplay, err := f.s.RecordProgramLoopJudgment(f.human, other.Context.RunID, other.CurrentAttempt.ID, "true", "Initially satisfied", nil)
	require.NoError(t, err)
	require.Equal(t, first.ID, oldReplay.ID, "old predecessor replay returns its exact successor after newer correction")
	type correction struct {
		event *storage.ProgramLoopEvent
		err   error
	}
	results := make(chan correction, 2)
	for _, value := range []string{"true", "unknown"} {
		go func(value string) {
			event, err := f.s.RecordProgramLoopJudgment(f.human, other.Context.RunID, other.CurrentAttempt.ID, value, "Concurrent correction "+value, &second.ID)
			results <- correction{event, err}
		}(value)
	}
	left, right := <-results, <-results
	require.True(t, (left.err == nil) != (right.err == nil), "one predecessor admits one successor")
	if left.err != nil {
		require.ErrorIs(t, left.err, storage.ErrProgramLoopConflict)
	} else {
		require.ErrorIs(t, right.err, storage.ErrProgramLoopConflict)
	}
	page, err := f.s.ReadProgramLoopHistory(f.human, other.Context.RunID, "", "")
	require.NoError(t, err)
	require.Len(t, page.Events, 3)
	var serialized []byte
	serialized, err = json.Marshal(page.Events)
	require.NoError(t, err)
	require.Contains(t, string(serialized), `"predecessorId"`)
}

func newProgramLoopFixture(t *testing.T, project string) *programLoopFixture {
	t.Helper()
	ctx := context.Background()
	s := newStore(t, postgres.WithProject(project))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Synthetic loop authorizer", Role: "admin"}, nil)
	require.NoError(t, err)
	service, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Synthetic loop host", Role: "reader", OwnerUserID: human.ID})
	require.NoError(t, err)
	consumer := &auth.Identity{UserID: service.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}
	scope := storage.ProgramHostScope{EnvironmentID: "loop-fixture-env", NativeProjectID: "loop-fixture-native"}
	return &programLoopFixture{t: t, s: s, human: auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman}), host: auth.WithIdentity(ctx, consumer), consumer: consumer, scope: scope, command: storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{}, Cwd: `C:\fixture\workspace`, EnvironmentID: scope.EnvironmentID, NativeProjectID: scope.NativeProjectID, TimeoutMS: 1000, WorkPurpose: "coordination", CompleteOnSuccess: true}}
}

func (f *programLoopFixture) task(slug string) {
	f.t.Helper()
	_, err := f.s.CreateSpec(f.human, slug, "Synthetic program loop task", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(f.t, err)
	stage := "approved"
	_, err = f.s.UpdateSpec(f.human, slug, nil, &stage, nil, nil, nil)
	require.NoError(f.t, err)
}

func (f *programLoopFixture) prepare(slug string, loop *storage.ProgramLoopConfig) *storage.ProgramRun {
	f.t.Helper()
	f.task(slug)
	command := f.command
	command.Loop = loop
	r, err := f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: slug, IdempotencyKey: "prepare-" + slug, Command: command}, f.consumer)
	require.NoError(f.t, err)
	require.Nil(f.t, r.CurrentAttempt)
	if loop != nil {
		require.Equal(f.t, "prepared", r.LoopState.Status)
	}
	f.assertMetadata(r)
	return r
}

func (f *programLoopFixture) admit(r *storage.ProgramRun) *storage.ProgramRun {
	f.t.Helper()
	a, err := f.s.AuthorizeProgramRun(f.host, f.scope, r.Context.RunID)
	require.NoError(f.t, err)
	require.True(f.t, a.MayStart)
	require.Equal(f.t, 1, a.Run.CurrentAttempt.Ordinal)
	f.assertMetadata(a.Run)
	return a.Run
}

func (f *programLoopFixture) assertMetadata(run *storage.ProgramRun) {
	f.t.Helper()
	metadata, err := f.s.ReadWorkbenchMetadata(f.human)
	require.NoError(f.t, err)
	for _, item := range metadata.Runs {
		if item.ID != run.Context.RunID {
			continue
		}
		if run.LoopState == nil {
			require.Nil(f.t, item.ProgramLoop)
			return
		}
		require.NotNil(f.t, item.ProgramLoop)
		require.Equal(f.t, run.LoopState.Status, item.ProgramLoop.Status)
		if run.CurrentAttempt == nil {
			require.Nil(f.t, item.ProgramLoop.AttemptID)
			require.Nil(f.t, item.ProgramLoop.AttemptOrdinal)
		} else {
			require.Equal(f.t, run.CurrentAttempt.ID, *item.ProgramLoop.AttemptID)
			require.Equal(f.t, run.CurrentAttempt.Ordinal, *item.ProgramLoop.AttemptOrdinal)
		}
		return
	}
	f.t.Fatal("program run missing from workbench metadata")
}

func (f *programLoopFixture) exit(r *storage.ProgramRun, code int64) storage.ProgramObservation {
	f.t.Helper()
	stamp := time.Date(2026, 9, 27, 1, r.CurrentAttempt.Ordinal, 0, 0, time.UTC)
	obs := storage.ProgramObservation{Outcome: "exited", ExitCode: &code, ObservedAt: stamp, FinishedAt: &stamp}
	_, err := f.s.RecordProgramAttemptResult(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID, &obs)
	require.NoError(f.t, err)
	return obs
}

func judgmentLoop(max int) *storage.ProgramLoopConfig {
	return &storage.ProgramLoopConfig{Kind: "innovative", Termination: storage.ProgramLoopTermination{Kind: "judgment", Judgment: &storage.ProgramLoopJudgment{Criterion: "Synthetic output satisfies the fixture criterion"}}, MaxAttempts: &max, SynchronousCompletionBasis: "Fixture authorization declares normal return ends this invocation's writes; not process-tree proof"}
}

func mechanicalLoop() *storage.ProgramLoopConfig {
	return &storage.ProgramLoopConfig{Kind: "mechanical", Termination: storage.ProgramLoopTermination{Kind: "exit_code", ExitCode: &storage.ProgramLoopExitCode{StopCode: 7, ContinueCodes: []int64{0}}}, SynchronousCompletionBasis: "Fixture authorization declares normal return ends this invocation's writes; not process-tree proof"}
}

func TestProgramLoopSerialAdmissionAndExactResults(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-serial")
	r := f.admit(f.prepare("mechanical", mechanicalLoop()))
	id, first := r.Context.RunID, r.CurrentAttempt.ID
	obs := f.exit(r, 0)
	_, err := f.s.RecordProgramResult(f.host, f.scope, id, &obs)
	require.ErrorIs(t, err, storage.ErrProgramLoopConflict)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, id)
	require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable, "zero exit with false termination cannot complete")
	_, err = f.s.Pool().Exec(f.human, `UPDATE claims SET lease_expires=now()-interval '1 hour' WHERE project_slug='program-loop-serial' AND agent=$1`, id)
	require.NoError(t, err)
	var admissions [2]*storage.ProgramAdmission
	var errs [2]error
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			admissions[i], errs[i] = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, id, first)
		}(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NotEqual(t, admissions[0].MayStart, admissions[1].MayStart)
	require.Equal(t, admissions[0].Run.CurrentAttempt.ID, admissions[1].Run.CurrentAttempt.ID)
	r = admissions[0].Run
	second := r.CurrentAttempt.ID
	require.Equal(t, 2, r.CurrentAttempt.Ordinal)
	require.Nil(t, r.Result)
	_, err = f.s.RecordProgramAttemptResult(f.host, f.scope, id, first, &obs)
	require.NoError(t, err)
	current, err := f.s.ReadProgramRun(f.host, f.scope, id)
	require.NoError(t, err)
	require.Equal(t, second, current.CurrentAttempt.ID)
	require.Equal(t, "admitted", current.State)
	require.Nil(t, current.Result)
	f.assertMetadata(current)
	f.exit(r, 0)
	next, err := f.s.AuthorizeNextProgramAttempt(f.host, f.scope, id, second)
	require.NoError(t, err)
	require.True(t, next.MayStart)
	require.Equal(t, 3, next.Run.CurrentAttempt.Ordinal)
	replay, err := f.s.AuthorizeNextProgramAttempt(f.host, f.scope, id, first)
	require.NoError(t, err)
	require.False(t, replay.MayStart)
	require.Equal(t, second, replay.Run.CurrentAttempt.ID, "replay names the original successor, not newest attempt")
	firstReplay, err := f.s.AuthorizeProgramRun(f.host, f.scope, id)
	require.NoError(t, err)
	require.False(t, firstReplay.MayStart)
	f.exit(next.Run, 7)
	current, err = f.s.ReadProgramRun(f.host, f.scope, id)
	require.NoError(t, err)
	require.Equal(t, "stopped", current.LoopState.Status)
	f.assertMetadata(current)
	_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, id, current.CurrentAttempt.ID)
	require.ErrorIs(t, err, storage.ErrProgramLoopStopped)
	_, err = f.s.CompleteProgramRun(f.host, f.scope, id)
	require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable, "nonzero stop is not a normal-success completion")
	history, err := f.s.ReadProgramLoopHistory(f.human, id, "", "")
	require.NoError(t, err)
	require.Len(t, history.Attempts, 3)
	require.Empty(t, history.Events)
	for _, a := range history.Attempts {
		require.NotNil(t, a.Result)
	}
}

func TestProgramLoopJudgmentAndPersistentStop(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-judgment")
	for _, last := range []string{"true", "false", "unknown"} {
		t.Run(last, func(t *testing.T) {
			r := f.admit(f.prepare("judgment-"+last, judgmentLoop(2)))
			f.exit(r, 0)
			_, err := f.s.RecordProgramLoopJudgment(f.host, r.Context.RunID, r.CurrentAttempt.ID, "false", "Host account is not a semantic judge", nil)
			require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
			unknown, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "unknown", "Synthetic evidence is not yet decisive", nil)
			require.NoError(t, err)
			current, err := f.s.ReadProgramRun(f.host, f.scope, r.Context.RunID)
			require.NoError(t, err)
			require.Equal(t, "waiting", current.LoopState.Status)
			f.assertMetadata(current)
			_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
			require.ErrorIs(t, err, storage.ErrProgramLoopWaiting)
			decision, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "false", "Synthetic criterion not yet satisfied", &unknown.ID)
			require.NoError(t, err)
			require.NotEqual(t, unknown.ID, decision.ID)
			_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
			require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
			next, err := f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
			require.NoError(t, err)
			require.True(t, next.MayStart)
			require.Equal(t, decision.ID, *next.Run.CurrentAttempt.ConsumedJudgmentID)
			corrected, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "true", "Later correction keeps consumed false", &decision.ID)
			require.NoError(t, err)
			require.Equal(t, decision.ID, *next.Run.CurrentAttempt.ConsumedJudgmentID)
			require.Equal(t, decision.ID, *corrected.PredecessorID)
			r = next.Run
			f.exit(r, 0)
			_, err = f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, last, "Synthetic final judgment", nil)
			require.NoError(t, err)
			current, err = f.s.ReadProgramRun(f.host, f.scope, r.Context.RunID)
			require.NoError(t, err)
			f.assertMetadata(current)
			if last == "true" {
				require.Equal(t, "stopped", current.LoopState.Status)
				_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
				require.NoError(t, err)
			} else {
				require.Equal(t, "needs_human", current.LoopState.Status)
				_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
				require.ErrorIs(t, err, storage.ErrProgramLoopNeedsHuman)
				_, err = f.s.CompleteProgramRun(f.host, f.scope, r.Context.RunID)
				require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
			}
		})
	}
	for _, phase := range []string{"prepared", "waiting", "between"} {
		r := f.prepare("cancel-"+phase, judgmentLoop(3))
		if phase != "prepared" {
			r = f.admit(r)
			f.exit(r, 0)
		}
		if phase == "between" {
			_, err := f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "false", "Synthetic ready state", nil)
			require.NoError(t, err)
		}
		stop, err := f.s.HostStopProgramLoop(f.host, f.scope, r.Context.RunID, "Persistent user cancellation reported by saved host")
		require.NoError(t, err)
		require.Equal(t, "stop", stop.Kind)
		current, err := f.s.ReadProgramRun(f.host, f.scope, r.Context.RunID)
		require.NoError(t, err)
		require.Equal(t, "cancelled", current.LoopState.Status)
		f.assertMetadata(current)
		require.Nil(t, current.Dispatch.StopConfirmation)
		if phase == "prepared" {
			_, err = f.s.AuthorizeProgramRun(f.host, f.scope, r.Context.RunID)
			require.ErrorIs(t, err, storage.ErrProgramLoopStopped)
		} else {
			if phase == "waiting" {
				_, err = f.s.RecordProgramLoopJudgment(f.human, r.Context.RunID, r.CurrentAttempt.ID, "false", "Late judgment cannot undo persistent stop", nil)
				require.NoError(t, err)
			}
			_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
			require.ErrorIs(t, err, storage.ErrProgramLoopStopped)
		}
	}
}

func TestProgramLoopUnknownAndTimeoutNeverContinue(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-unknown")
	for _, outcome := range []string{"unknown-code", "timed_out", "cancel_requested", "unconfirmed"} {
		r := f.admit(f.prepare(outcome, mechanicalLoop()))
		var err error
		if outcome == "unknown-code" {
			f.exit(r, 99)
		} else {
			observation := storage.ProgramObservation{Outcome: outcome, ObservedAt: time.Now().UTC()}
			_, err = f.s.RecordProgramAttemptResult(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID, &observation)
			require.NoError(t, err)
			require.Nil(t, observation.TimedOutAt, "recording must not synthesize timestamps in the caller's observation")
			require.Nil(t, observation.CancelRequestedAt)
			if outcome != "unconfirmed" {
				f.exit(r, 0)
			}
		}
		current, err := f.s.ReadProgramRun(f.host, f.scope, r.Context.RunID)
		require.NoError(t, err)
		require.Equal(t, "waiting", current.LoopState.Status)
		f.assertMetadata(current)
		require.Equal(t, "unknown", current.LoopState.Condition.Value)
		_, err = f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
		require.ErrorIs(t, err, storage.ErrProgramLoopWaiting)
	}
}

func TestProgramLoopFrozenSourcesAndDiscovery(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-sources")
	for _, change := range []string{"input", "dependency", "ancestor", "noop", "released"} {
		r := f.prepare("source-"+change, mechanicalLoop())
		if change == "ancestor" || change == "noop" {
			f.task("parent-" + change)
			_, err := f.s.AddEdge(f.human, "parent-"+change, r.Context.TaskSlug, storage.EdgeTypeComposes)
			require.NoError(t, err)
			// Prepare after the parent is part of the actual frozen scope.
			require.NoError(t, f.s.CancelRunPreparation(f.human, r.Context.RunID, r.Context.PackageID, f.consumerOwner(), "Replace unstarted synthetic scope fixture"))
			command := f.command
			command.Loop = mechanicalLoop()
			r, err = f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: r.Context.TaskSlug, IdempotencyKey: "reprepare-" + change, Command: command}, f.consumer)
			require.NoError(t, err)
		}
		r = f.admit(r)
		f.exit(r, 0)
		var err error
		switch change {
		case "input":
			err = f.s.StoreShapeOutput(f.human, r.Context.TaskSlug, &storage.ShapeOutput{ChosenApproach: "Changed synthetic execution input"})
		case "dependency":
			f.task("new-dependency")
			_, err = f.s.AddEdge(f.human, r.Context.TaskSlug, "new-dependency", storage.EdgeTypeDependsOn)
		case "ancestor":
			err = f.s.StoreShapeOutput(f.human, "parent-"+change, &storage.ShapeOutput{ScopeOut: []string{"Changed inherited boundary"}})
		case "noop":
			intent := "Synthetic program loop task"
			_, err = f.s.UpdateSpec(f.human, "parent-"+change, &intent, nil, nil, nil, nil)
		case "released":
			err = f.s.ResolveRunDispatch(f.human, r.Context.RunID, r.Dispatch.Admission.ID, f.consumerOwner(), "isolated_workspace", "Synthetic released responsibility")
		}
		require.NoError(t, err)
		next, err := f.s.AuthorizeNextProgramAttempt(f.host, f.scope, r.Context.RunID, r.CurrentAttempt.ID)
		if change == "noop" {
			require.NoError(t, err)
			require.True(t, next.MayStart)
		} else {
			require.Error(t, err)
		}
	}
	for i := range 6 {
		r := f.admit(f.prepare(fmt.Sprintf("discover-%d", i), judgmentLoop(2)))
		f.exit(r, 0)
	}
	cursor := ""
	seen := map[string]bool{}
	for {
		page, err := f.s.ListHostHooks(f.host, storage.DeliveryHookHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, 2, cursor)
		require.NoError(t, err)
		for _, h := range page.Hooks {
			require.Equal(t, "program_loop", h.Kind)
			require.Equal(t, "program-loop-sources", h.ProgramLoop.Project)
			require.False(t, seen[h.ProgramLoop.RunID])
			seen[h.ProgramLoop.RunID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	require.GreaterOrEqual(t, len(seen), 6, "waiting candidates paginate and cannot hide later ready work")
}

func (f *programLoopFixture) consumerOwner() string {
	identity, _ := auth.IdentityFromContext(f.human)
	return identity.UserID
}

func TestProgramLoopHistoryIndependentPages(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-history")
	r := f.admit(f.prepare("history", judgmentLoop(100)))
	for i := 2; i <= 51; i++ {
		_, err := f.s.Pool().Exec(f.human, `INSERT INTO program_attempts(id,project_slug,run_id,admission_id,ordinal,granted_at) VALUES($1,'program-loop-history',$2,$3,$4,now())`, fmt.Sprintf("history-attempt-%d", i), r.Context.RunID, r.Dispatch.Admission.ID, i)
		require.NoError(t, err)
	}
	for i := 0; i < 51; i++ {
		_, err := f.s.Pool().Exec(f.human, `INSERT INTO program_loop_events(id,project_slug,run_id,attempt_id,kind,value,actor_user_id,reason,recorded_at) VALUES($1,'program-loop-history',$2,$3,'condition','unknown',$4,'Synthetic pagination fixture',now())`, fmt.Sprintf("history-event-%d", i), r.Context.RunID, r.CurrentAttempt.ID, f.consumerOwner())
		require.NoError(t, err)
	}
	page, err := f.s.HostReadProgramLoopHistory(f.host, f.scope, r.Context.RunID, "", "")
	require.NoError(t, err)
	require.Len(t, page.Attempts, 50)
	require.Len(t, page.Events, 50)
	require.Equal(t, "50", page.NextAttemptCursor)
	require.NotEmpty(t, page.NextEventCursor)
	next, err := f.s.HostReadProgramLoopHistory(f.host, f.scope, r.Context.RunID, page.NextAttemptCursor, page.NextEventCursor)
	require.NoError(t, err)
	require.Len(t, next.Attempts, 1)
	require.Len(t, next.Events, 1)
	require.Empty(t, next.NextAttemptCursor)
	require.Empty(t, next.NextEventCursor)
	current, err := f.s.ReadProgramRun(f.host, f.scope, r.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, 51, current.CurrentAttempt.Ordinal)
}

func TestProgramLoopConfigurationBoundary(t *testing.T) {
	f := newProgramLoopFixture(t, "program-loop-config")
	f.task("config")
	for i, change := range []string{"kind", "no-max", "zero-max", "no-basis", "duplicate", "overlap", "union", "no-criterion"} {
		loop := judgmentLoop(2)
		switch change {
		case "kind":
			loop.Kind = "retry"
		case "no-max":
			loop.MaxAttempts = nil
		case "zero-max":
			zero := 0
			loop.MaxAttempts = &zero
		case "no-basis":
			loop.SynchronousCompletionBasis = ""
		case "duplicate":
			loop = mechanicalLoop()
			loop.Termination.ExitCode.ContinueCodes = []int64{0, 0}
		case "overlap":
			loop = mechanicalLoop()
			loop.Termination.ExitCode.ContinueCodes = []int64{7}
		case "union":
			loop.Termination.ExitCode = &storage.ProgramLoopExitCode{ContinueCodes: []int64{}}
		case "no-criterion":
			loop.Termination.Judgment.Criterion = ""
		}
		command := f.command
		command.Loop = loop
		_, err := f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: "config", IdempotencyKey: fmt.Sprintf("bad-%d", i), Command: command}, f.consumer)
		require.ErrorIs(t, err, storage.ErrInvalidProgramLoop, change)
	}
}

func TestProgramLoopCompletionHookContinuationChecksOriginalFact(t *testing.T) {
	f := newCompletionHookFixture(t, "program-loop-hook-source")
	for _, changed := range []bool{false, true} {
		suffix := fmt.Sprint(changed)
		source := f.create("source-" + suffix)
		f.create("target-" + suffix)
		command := storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{}, Cwd: `C:\fixture\workspace`, EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID, TimeoutMS: 1000, WorkPurpose: "coordination", Loop: mechanicalLoop()}
		r, err := f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: "target-" + suffix, IdempotencyKey: "prepare-" + suffix, Command: command}, f.consumer)
		require.NoError(t, err)
		hook, err := f.s.ArmHumanCompletionProgramHook(f.human, f.request(source, r, "arm-"+suffix), f.consumer)
		require.NoError(t, err)
		f.manual(source.Slug, "fact-"+suffix)
		admission, err := f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
		require.NoError(t, err)
		require.True(t, admission.MayStart)
		stamp, code := time.Now().UTC(), int64(0)
		scope := storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}
		_, err = f.s.RecordProgramAttemptResult(f.host, scope, r.Context.RunID, admission.Run.CurrentAttempt.ID, &storage.ProgramObservation{Outcome: "exited", ExitCode: &code, ObservedAt: stamp, FinishedAt: &stamp})
		require.NoError(t, err)
		if changed {
			intent := "Changed source after the recorded completion"
			_, err = f.s.UpdateSpec(f.human, source.Slug, &intent, nil, nil, nil, nil)
			require.NoError(t, err)
		}
		next, err := f.s.AuthorizeNextProgramAttempt(f.host, scope, r.Context.RunID, admission.Run.CurrentAttempt.ID)
		if changed {
			require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
		} else {
			require.NoError(t, err)
			require.True(t, next.MayStart)
		}
	}
}
