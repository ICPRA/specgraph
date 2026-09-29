// SPDX-License-Identifier: Apache-2.0

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

// All commands and host observations here are inert fixture data; no process is run.
func TestProgramRunStorage(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("program-run"))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Program fixture authorizer", Role: "admin"}, nil)
	require.NoError(t, err)
	service, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Program fixture consumer", Role: "reader", OwnerUserID: human.ID})
	require.NoError(t, err)
	humanCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman, EffectiveRole: auth.RoleAdmin})
	consumer := &auth.Identity{UserID: service.ID, UserKind: storage.KindServiceAccount, EffectiveRole: auth.RoleReader, Source: "apikey"}
	hostCtx := auth.WithIdentity(ctx, consumer)
	create := func(slug string) {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Fixed program fixture task", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
	}
	create("task")
	create("qa-missing")
	create("blocked")
	create("prerequisite")
	_, err = s.AddEdge(ctx, "blocked", "prerequisite", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	command := storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{"--fixture", "literal value"}, Cwd: `C:\fixture\workspace`, EnvironmentID: "program-env", NativeProjectID: "program-native", TimeoutMS: 10000, WorkPurpose: "coordination"}
	req := storage.PrepareProgramRunRequest{TaskSlug: "task", IdempotencyKey: "prepare-program", Command: command}
	_, err = s.PrepareProgramRun(hostCtx, &req, consumer)
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
	bad := req
	bad.Command.Executable = "relative.exe"
	_, err = s.PrepareProgramRun(humanCtx, &bad, consumer)
	require.ErrorIs(t, err, storage.ErrInvalidProgramRun)
	bad = req
	bad.TaskSlug, bad.IdempotencyKey, bad.Command.WorkPurpose = "qa-missing", "qa-missing", "implementation"
	_, err = s.PrepareProgramRun(humanCtx, &bad, consumer)
	require.ErrorIs(t, err, storage.ErrInvalidRunPreparation, "program kind cannot waive implementation QA")
	bad = req
	bad.TaskSlug, bad.IdempotencyKey = "blocked", "blocked"
	_, err = s.PrepareProgramRun(humanCtx, &bad, consumer)
	require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
	run, err := s.PrepareProgramRun(humanCtx, &req, consumer)
	require.NoError(t, err)
	require.Equal(t, "program", run.ExecutorKind)
	require.Equal(t, "prepared", run.State)
	require.Nil(t, run.Result)
	var attempts int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM program_attempts WHERE project_slug='program-run' AND run_id=$1`, run.Context.RunID).Scan(&attempts))
	require.Zero(t, attempts, "preparation is not a physical program attempt")
	var legacyResultColumn bool
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='run_bindings' AND column_name='program_result')`).Scan(&legacyResultColumn))
	require.False(t, legacyResultColumn, "the attempt is the only program result storage owner")
	var thread string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT thread_ref FROM run_bindings WHERE project_slug='program-run' AND id=$1`, run.Context.RunID).Scan(&thread))
	require.Empty(t, thread)
	var body struct {
		Target storage.ProgramTarget `json:"program_target"`
		Agent  json.RawMessage       `json:"dispatch_target"`
	}
	require.NoError(t, json.Unmarshal(run.Context.Body, &body))
	require.Nil(t, body.Agent)
	require.Equal(t, command, body.Target.ProgramCommand)
	require.Equal(t, human.ID, body.Target.AuthorizedByUserID)
	require.Equal(t, service.ID, body.Target.HostConsumerUserID)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Len(t, metadata.Runs, 1)
	require.Equal(t, "program", metadata.Runs[0].ExecutorKind)
	require.Equal(t, command.NativeProjectID, *metadata.Runs[0].NativeProjectID)
	require.Equal(t, command.WorkPurpose, *metadata.Runs[0].WorkPurpose)
	require.Empty(t, metadata.Runs[0].ThreadRef)
	again, err := s.PrepareProgramRun(humanCtx, &req, consumer)
	require.NoError(t, err)
	require.True(t, again.Replayed)
	require.Equal(t, run.Context, again.Context)
	bad = req
	bad.Command.Args = []string{"changed"}
	_, err = s.PrepareProgramRun(humanCtx, &bad, consumer)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	bad = req
	bad.Command.CompleteOnSuccess = true
	_, err = s.PrepareProgramRun(humanCtx, &bad, consumer)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "completion agreement is frozen in the original target")
	_, _, err = s.PrepareRunForOperator(ctx, "task", command.Cwd, human.ID, req.IdempotencyKey, nil)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "agent preparation cannot reuse a program receipt")
	require.ErrorIs(t, s.BindRunThreadInEnvironment(ctx, run.Context.RunID, "program-env", "forbidden-agent-thread"), storage.ErrRunBindingConflict)
	_, err = s.AuthorizeRunDispatch(ctx, run.Context.RunID, human.ID, run.Context.PackageID, json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Negative agent admission fixture"}`))
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.RecordCompletion(hostCtx, "task", run.Context.RunID), storage.ErrProgramCompletionUnavailable)
	hostScope := storage.ProgramHostScope{EnvironmentID: "program-env", NativeProjectID: "program-native"}
	_, err = s.AuthorizeProgramRun(humanCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug='program-run' AND spec_slug='task'`)
	require.NoError(t, err)
	_, err = s.AuthorizeProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrAgentNotClaimOwner, "original lease remains a first-admission gate")
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM program_attempts WHERE project_slug='program-run' AND run_id=$1`, run.Context.RunID).Scan(&attempts))
	require.Zero(t, attempts, "a rejected admission cannot create an attempt")
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()+interval '1 minute' WHERE project_slug='program-run' AND spec_slug='task'`)
	require.NoError(t, err)
	admitted, err := s.AuthorizeProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.NoError(t, err)
	require.True(t, admitted.MayStart)
	require.Equal(t, human.ID, admitted.Run.Dispatch.Admission.Actor)
	var attemptID, admissionID string
	var ordinal int
	var encoded []byte
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT id,admission_id,ordinal,result FROM program_attempts WHERE project_slug='program-run' AND run_id=$1`, run.Context.RunID).Scan(&attemptID, &admissionID, &ordinal, &encoded))
	require.NotEmpty(t, attemptID)
	require.Equal(t, admitted.Run.Dispatch.Admission.ID, admissionID)
	require.Equal(t, 1, ordinal)
	require.Nil(t, encoded, "admission creates the attempt before a result is reported")
	replayed, err := s.AuthorizeProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.NoError(t, err)
	require.False(t, replayed.MayStart, "admission replay cannot cause a second physical execution")
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM program_attempts WHERE project_slug='program-run' AND run_id=$1`, run.Context.RunID).Scan(&attempts))
	require.Equal(t, 1, attempts)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug='program-run' AND spec_slug='task'`)
	require.NoError(t, err)
	_, err = s.PrepareRun(ctx, "task", `C:\fixture\another`)
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	_, err = s.LifecycleAbandonSpec(ctx, "task", "Cannot prove program stopped")
	require.ErrorIs(t, err, storage.ErrAbandonExecutionPending)
	observed := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	_, err = s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &storage.ProgramObservation{Outcome: "unconfirmed", ObservedAt: observed})
	require.NoError(t, err)
	timedOut := observed.Add(time.Second)
	_, err = s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &storage.ProgramObservation{Outcome: "timed_out", ObservedAt: timedOut})
	require.NoError(t, err)
	cancelledAt := observed.Add(2 * time.Second)
	_, err = s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &storage.ProgramObservation{Outcome: "cancel_requested", ObservedAt: cancelledAt})
	require.NoError(t, err)
	finished := observed.Add(3 * time.Second)
	zero := int64(0)
	exit := storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, ObservedAt: finished, FinishedAt: &finished}
	result, err := s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &exit)
	require.NoError(t, err)
	require.Nil(t, exit.TimedOutAt, "recording must not merge prior facts into the caller's observation")
	require.Nil(t, exit.CancelRequestedAt)
	require.Equal(t, timedOut, *result.TimedOutAt, "late exit zero retains prior timeout fact")
	require.Equal(t, cancelledAt, *result.CancelRequestedAt)
	require.Equal(t, service.ID, result.ReporterUserID)
	repeatResult, err := s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &exit)
	require.NoError(t, err)
	require.Equal(t, result, repeatResult)
	nonzero := int64(7)
	conflict := exit
	conflict.ExitCode = &nonzero
	_, err = s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &conflict)
	require.ErrorIs(t, err, storage.ErrProgramResultConflict)
	_, err = s.RecordProgramResult(humanCtx, hostScope, run.Context.RunID, &exit)
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
	require.ErrorIs(t, s.RecordCompletion(hostCtx, "task", run.Context.RunID), storage.ErrProgramCompletionUnavailable)
	_, err = s.PrepareRun(ctx, "task", `C:\fixture\another`)
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld, "result state still retains unresolved admission")
	after, err := s.GetSpec(ctx, "task")
	require.NoError(t, err)
	require.NotEqual(t, "done", string(after.Stage))
	saved, err := s.ReadProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, "exited", saved.State)
	require.Equal(t, run.Context, saved.Context)
	require.Equal(t, result, saved.Result)
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT result FROM program_attempts WHERE project_slug='program-run' AND run_id=$1 AND id=$2 AND admission_id=$3 AND ordinal=1`, run.Context.RunID, attemptID, admissionID).Scan(&encoded))
	var storedResult storage.ProgramRunResult
	require.NoError(t, json.Unmarshal(encoded, &storedResult))
	require.Equal(t, *result, storedResult, "late exit and duplicate reports update the original attempt, not another source")
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM program_attempts WHERE project_slug='program-run' AND run_id=$1`, run.Context.RunID).Scan(&attempts))
	require.Equal(t, 1, attempts)
	_, err = s.EnsureProject(ctx, "program-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "program-other")
	require.NoError(t, err)
	_, err = other.ReadProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrRunContextNotFound)
	_, err = s.Pool().Exec(ctx, `DELETE FROM program_attempts WHERE project_slug='program-run' AND id=$1`, attemptID)
	require.NoError(t, err)
	_, err = s.ReadProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "an admitted run missing its attempt is not an unexecuted preparation")
	_, err = s.AuthorizeProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.RecordProgramResult(hostCtx, hostScope, run.Context.RunID, &exit)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.CompleteProgramRun(hostCtx, hostScope, run.Context.RunID)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
}

func TestProgramRunCompletion(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("program-completion"))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Completion fixture authorizer", Role: "admin"}, nil)
	require.NoError(t, err)
	service, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Completion fixture consumer", Role: "reader", OwnerUserID: human.ID})
	require.NoError(t, err)
	humanCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	consumer := &auth.Identity{UserID: service.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}
	hostCtx := auth.WithIdentity(ctx, consumer)
	scope := storage.ProgramHostScope{EnvironmentID: "program-env", NativeProjectID: "program-native"}
	command := storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{}, Cwd: `C:\fixture\workspace`, EnvironmentID: scope.EnvironmentID, NativeProjectID: scope.NativeProjectID, TimeoutMS: 10000, WorkPurpose: "coordination", CompleteOnSuccess: true}
	create := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Completion fixture input", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	create("reviewer")
	reviewer, err := s.PrepareRun(ctx, "reviewer", "fixture-review")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, reviewer, "local", "program-review-thread"))
	for _, purpose := range []string{"requirements", "design", "implementation"} {
		forbidden := command
		forbidden.WorkPurpose = purpose
		_, err := s.PrepareProgramRun(humanCtx, &storage.PrepareProgramRunRequest{TaskSlug: "reviewer", IdempotencyKey: "forbidden-" + purpose, Command: forbidden}, consumer)
		require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
	}
	requirementSource := create("requirements-source")
	assigned, err := s.AssignReview(humanCtx, &storage.AssignReviewRequest{TaskSlug: "requirements-source", Kind: "requirements", Sources: []storage.ReviewSource{requirementSource}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer}, nil)
	require.NoError(t, err)
	approved, err := s.ReviewSource(humanCtx, "requirements-source", assigned.Reviews[0].Request.ID, "accepted", "Synthetic source approval for program purpose fixtures")
	require.NoError(t, err)
	designSource, planSource := create("design-source"), create("plan-source")
	for _, purpose := range []string{"test_design", "test_execution", "requirements_review", "design_review", "investigation", "coordination", "knowledge"} {
		t.Run(purpose, func(t *testing.T) {
			create(purpose)
			cmd := command
			cmd.WorkPurpose = purpose
			if purpose == "test_design" {
				cmd.QABasis = &storage.DispatchQABasis{RequirementDecisionIDs: []string{approved.Decision.ID}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{designSource}, TestPlanSources: []storage.ReviewSource{}, Applicability: "Synthetic source fixture applies to this test design"}
			}
			if purpose == "test_execution" {
				cmd.QABasis = &storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{planSource}, Applicability: "Synthetic plan fixture applies to this test execution"}
			}
			run, err := s.PrepareProgramRun(humanCtx, &storage.PrepareProgramRunRequest{TaskSlug: purpose, IdempotencyKey: "purpose-" + purpose, Command: cmd}, consumer)
			require.NoError(t, err)
			_, err = s.AuthorizeProgramRun(hostCtx, scope, run.Context.RunID)
			require.NoError(t, err)
			finished, zero := time.Date(2026, 9, 27, 0, 1, 0, 0, time.UTC), int64(0)
			_, err = s.RecordProgramResult(hostCtx, scope, run.Context.RunID, &storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, ObservedAt: finished, FinishedAt: &finished})
			require.NoError(t, err)
			require.NoError(t, s.RecordCompletion(hostCtx, purpose, run.Context.RunID))
			saved, err := s.ReadProgramRun(hostCtx, scope, run.Context.RunID)
			require.NoError(t, err)
			require.Equal(t, "completed", saved.State)
		})
	}
	for _, scenario := range []string{"normal", "expired", "default", "nonzero", "timed_out", "cancel_requested", "no-result", "unadmitted", "human-hold", "input-changed", "dependency-changed", "competing-claim", "competing-dispatch", "released", "preparation-cancelled", "other-completion"} {
		t.Run(scenario, func(t *testing.T) {
			source := create(scenario)
			cmd := command
			cmd.CompleteOnSuccess = scenario != "default"
			run, err := s.PrepareProgramRun(humanCtx, &storage.PrepareProgramRunRequest{TaskSlug: scenario, IdempotencyKey: "prepare-" + scenario, Command: cmd}, consumer)
			require.NoError(t, err)
			id := run.Context.RunID
			if scenario == "unadmitted" {
				_, err = s.CompleteProgramRun(hostCtx, scope, id)
				require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
				return
			}
			admission, err := s.AuthorizeProgramRun(hostCtx, scope, id)
			require.NoError(t, err)
			if scenario == "no-result" {
				_, err = s.CompleteProgramRun(hostCtx, scope, id)
				require.ErrorIs(t, err, storage.ErrProgramCompletionUnavailable)
				return
			}
			finished := time.Date(2026, 9, 27, 0, 1, 0, 0, time.UTC)
			if scenario == "timed_out" || scenario == "cancel_requested" {
				_, err = s.RecordProgramResult(hostCtx, scope, id, &storage.ProgramObservation{Outcome: scenario, ObservedAt: finished.Add(-time.Second)})
				require.NoError(t, err)
			}
			code := int64(0)
			if scenario == "nonzero" {
				code = 7
			}
			exit := storage.ProgramObservation{Outcome: "exited", ExitCode: &code, ObservedAt: finished, FinishedAt: &finished}
			observed, err := s.RecordProgramResult(hostCtx, scope, id, &exit)
			require.NoError(t, err)
			want := error(nil)
			switch scenario {
			case "default", "nonzero", "timed_out", "cancel_requested":
				want = storage.ErrProgramCompletionUnavailable
			case "expired":
				_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 hour' WHERE project_slug='program-completion' AND agent=$1`, id)
			case "competing-claim":
				_, err = s.Pool().Exec(ctx, `UPDATE claims SET agent='new-owner' WHERE project_slug='program-completion' AND agent=$1`, id)
				want = storage.ErrRunBindingConflict
			case "competing-dispatch":
				err = s.ResolveRunDispatch(ctx, id, admission.Run.Dispatch.Admission.ID, human.ID, "isolated_workspace", "Synthetic replacement responsibility")
				require.NoError(t, err)
				other, prepareErr := s.PrepareProgramRun(humanCtx, &storage.PrepareProgramRunRequest{TaskSlug: scenario, IdempotencyKey: "prepare-competitor", Command: command}, consumer)
				require.NoError(t, prepareErr)
				_, err = s.AuthorizeProgramRun(hostCtx, scope, other.Context.RunID)
				want = storage.ErrRunBindingConflict
			case "released":
				err = s.ResolveRunDispatch(ctx, id, admission.Run.Dispatch.Admission.ID, human.ID, "isolated_workspace", "Synthetic responsibility release")
				want = storage.ErrRunBindingConflict
			case "preparation-cancelled":
				_, err = s.Pool().Exec(ctx, `INSERT INTO run_preparation_cancellations(run_id,project_slug,package_id,actor,note,cancelled_at) VALUES($1,'program-completion',$2,$3,'Synthetic cancellation fence',now())`, id, run.Context.PackageID, human.ID)
				want = storage.ErrRunBindingConflict
			case "human-hold":
				maximum := int32(1)
				hold, assignErr := s.AssignReview(humanCtx, &storage.AssignReviewRequest{TaskSlug: scenario, Kind: "requirements", Sources: []storage.ReviewSource{source}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer, MaxReviewRounds: &maximum}, nil)
				require.NoError(t, assignErr)
				_, err = s.SubmitReview(humanCtx, storage.MailScope{EnvironmentID: "local", ThreadID: "program-review-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}, hold.Reviews[0].Request.ID, "rejected", "Synthetic human hold")
				want = storage.ErrReviewHumanHold
			case "input-changed":
				err = s.StoreShapeOutput(ctx, scenario, &storage.ShapeOutput{ChosenApproach: "Changed unsubmitted input"})
				want = storage.ErrCompletionRequiresRequirementReview
			case "dependency-changed":
				_, err = s.AddEdge(ctx, scenario, "reviewer", storage.EdgeTypeDependsOn)
				want = storage.ErrExecutionDependenciesChanged
			case "other-completion":
				stage := "done"
				_, err = s.UpdateSpec(ctx, scenario, nil, &stage, nil, nil, nil)
				require.NoError(t, err)
				_, err = s.Pool().Exec(ctx, `INSERT INTO execution_events(id,spec_slug,project_slug,agent,event_type,message,created_at) VALUES('evt_program_other',$1,'program-completion','other-run','completion','Synthetic other completion',now())`, scenario)
				want = storage.ErrRunBindingConflict
			}
			require.NoError(t, err)
			if want != nil {
				_, err = s.CompleteProgramRun(hostCtx, scope, id)
				require.ErrorIs(t, err, want)
				require.ErrorIs(t, s.RecordCompletion(hostCtx, scenario, id), want, "original completion entry shares the same guards")
				saved, err := s.ReadProgramRun(hostCtx, scope, id)
				require.NoError(t, err)
				require.Equal(t, observed, saved.Result, "completion rejection preserves committed observation")
				require.Equal(t, "exited", saved.State)
				return
			}
			_, err = s.CompleteProgramRun(humanCtx, scope, id)
			require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
			require.ErrorIs(t, s.RecordCompletion(humanCtx, scenario, id), storage.ErrProgramRunForbidden)
			require.ErrorIs(t, s.RecordCompletion(ctx, scenario, id), storage.ErrProgramRunForbidden)
			wrongHost := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindServiceAccount, Source: "apikey"})
			require.ErrorIs(t, s.RecordCompletion(wrongHost, scenario, id), storage.ErrProgramRunForbidden)
			_, err = s.CompleteProgramRun(hostCtx, storage.ProgramHostScope{EnvironmentID: scope.EnvironmentID, NativeProjectID: "wrong-project"}, id)
			require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
			receipt, err := s.CompleteProgramRun(hostCtx, scope, id)
			require.NoError(t, err)
			require.Equal(t, storage.RunSelfCompletion{RunID: id, Completed: scenario}, receipt)
			repeated, err := s.CompleteProgramRun(hostCtx, scope, id)
			require.NoError(t, err)
			require.Equal(t, receipt, repeated)
			events, err := s.GetExecutionEvents(ctx, scenario, 0)
			require.NoError(t, err)
			require.Len(t, events, 1)
			_, err = s.RecordProgramResult(hostCtx, scope, id, &exit)
			require.NoError(t, err)
			saved, err := s.ReadProgramRun(hostCtx, scope, id)
			require.NoError(t, err)
			require.Equal(t, "completed", saved.State)
			require.Equal(t, observed, saved.Result)
			require.Nil(t, saved.Dispatch.Resolution, "task completion does not release process responsibility")
			require.Nil(t, saved.Dispatch.StopConfirmation)
			stage := "approved"
			_, err = s.UpdateSpec(ctx, scenario, nil, &stage, nil, nil, nil)
			require.NoError(t, err)
			_, err = s.CompleteProgramRun(hostCtx, scope, id)
			require.ErrorIs(t, err, storage.ErrRunBindingConflict, "old successful exit cannot close reopened work")
			events, err = s.GetExecutionEvents(ctx, scenario, 0)
			require.NoError(t, err)
			require.Len(t, events, 1)
			reopened, err := s.GetSpec(ctx, scenario)
			require.NoError(t, err)
			require.Equal(t, storage.SpecStageApproved, reopened.Stage)
		})
	}
	_, err = s.EnsureProject(ctx, "program-completion-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "program-completion-other")
	require.NoError(t, err)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	for _, run := range metadata.Runs {
		if run.ExecutorKind == "program" {
			_, err = other.CompleteProgramRun(hostCtx, scope, run.ID)
			require.ErrorIs(t, err, storage.ErrRunContextNotFound)
			break
		}
	}
}

func TestProgramPreparationCancellation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("program-cancellation"))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	authorizer, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Program authorizer", Role: "admin"}, nil)
	require.NoError(t, err)
	manager, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Program cancelling manager", Role: "admin"}, nil)
	require.NoError(t, err)
	service, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Program consumer", Role: "reader", OwnerUserID: authorizer.ID})
	require.NoError(t, err)
	authorCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: authorizer.ID, UserKind: storage.KindHuman})
	managerCtx := auth.WithIdentity(ctx, &auth.Identity{UserID: manager.ID, UserKind: storage.KindHuman})
	consumer := &auth.Identity{UserID: service.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}
	hostCtx := auth.WithIdentity(ctx, consumer)
	scope := storage.ProgramHostScope{EnvironmentID: "cancel-env", NativeProjectID: "cancel-native"}
	command := storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{}, Cwd: `C:\fixture\workspace`, EnvironmentID: scope.EnvironmentID, NativeProjectID: scope.NativeProjectID, TimeoutMS: 1000, WorkPurpose: "coordination"}
	for _, slug := range []string{"normal", "replacement", "admitted"} {
		_, err := s.CreateSpec(ctx, slug, "Cancellation fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
	}
	prepare := func(slug, key string) *storage.ProgramRun {
		t.Helper()
		run, err := s.PrepareProgramRun(authorCtx, &storage.PrepareProgramRunRequest{TaskSlug: slug, IdempotencyKey: key, Command: command}, consumer)
		require.NoError(t, err)
		return run
	}
	run := prepare("normal", "cancel-normal")
	id, pkg := run.Context.RunID, run.Context.PackageID
	note := "Human cancels this unstarted program"
	require.ErrorIs(t, s.CancelRunPreparation(managerCtx, id, "wrong-package", manager.ID, note), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.CancelRunPreparation(ctx, id, pkg, manager.ID, note), storage.ErrProgramRunForbidden)
	require.ErrorIs(t, s.CancelRunPreparation(hostCtx, id, pkg, service.ID, note), storage.ErrProgramRunForbidden)
	require.ErrorIs(t, s.CancelRunPreparation(authorCtx, id, pkg, manager.ID, note), storage.ErrProgramRunForbidden)
	require.NoError(t, s.CancelRunPreparation(managerCtx, id, pkg, manager.ID, note), "a verified manager need not be the original authorizer")
	require.NoError(t, s.CancelRunPreparation(managerCtx, id, pkg, manager.ID, note))
	require.ErrorIs(t, s.CancelRunPreparation(ctx, id, pkg, manager.ID, note), storage.ErrProgramRunForbidden, "replay still verifies the human identity")
	require.ErrorIs(t, s.CancelRunPreparation(authorCtx, id, pkg, authorizer.ID, note), storage.ErrRunBindingConflict)
	require.ErrorIs(t, s.CancelRunPreparation(managerCtx, id, pkg, manager.ID, "changed note"), storage.ErrRunBindingConflict)
	saved, err := s.ReadProgramRun(hostCtx, scope, id)
	require.NoError(t, err)
	require.Equal(t, "preparation_cancelled", saved.State)
	require.Equal(t, manager.ID, saved.Dispatch.Cancellation.Actor)
	require.Nil(t, saved.Dispatch.Admission)
	require.Nil(t, saved.Dispatch.StopConfirmation)
	require.Nil(t, saved.Result)
	claim, err := s.GetActiveClaim(ctx, "normal")
	require.NoError(t, err)
	require.Nil(t, claim)
	_, err = s.AuthorizeProgramRun(hostCtx, scope, id)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	replayed := prepare("normal", "cancel-normal")
	require.True(t, replayed.Replayed)
	require.Equal(t, id, replayed.Context.RunID)
	fresh := prepare("normal", "after-cancellation")
	require.NotEqual(t, id, fresh.Context.RunID)
	_, err = s.AuthorizeProgramRun(hostCtx, scope, fresh.Context.RunID)
	require.NoError(t, err)

	old := prepare("replacement", "expired-preparation")
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug='program-cancellation' AND agent=$1`, old.Context.RunID)
	require.NoError(t, err)
	replacement := prepare("replacement", "replacement-preparation")
	require.NoError(t, s.CancelRunPreparation(managerCtx, old.Context.RunID, old.Context.PackageID, manager.ID, note))
	claim, err = s.GetActiveClaim(ctx, "replacement")
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, replacement.Context.RunID, claim.Agent)
	var edgeHeld bool
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM edges WHERE project_slug='program-cancellation' AND from_slug='replacement' AND to_slug=$1 AND edge_type='CLAIMED_BY')`, replacement.Context.RunID).Scan(&edgeHeld))
	require.True(t, edgeHeld)
	_, err = s.AuthorizeProgramRun(hostCtx, scope, old.Context.RunID)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.AuthorizeProgramRun(hostCtx, scope, replacement.Context.RunID)
	require.NoError(t, err)

	admitted := prepare("admitted", "admitted-preparation")
	a, err := s.AuthorizeProgramRun(hostCtx, scope, admitted.Context.RunID)
	require.NoError(t, err)
	require.ErrorIs(t, s.CancelRunPreparation(managerCtx, admitted.Context.RunID, admitted.Context.PackageID, manager.ID, note), storage.ErrDispatchResponsibilityHeld)
	require.NoError(t, s.ResolveRunDispatch(ctx, admitted.Context.RunID, a.Run.Dispatch.Admission.ID, manager.ID, "isolated_workspace", "Synthetic release does not undo admission"))
	require.ErrorIs(t, s.CancelRunPreparation(managerCtx, admitted.Context.RunID, admitted.Context.PackageID, manager.ID, note), storage.ErrDispatchResponsibilityHeld)
}
