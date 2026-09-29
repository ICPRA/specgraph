// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

type completionHookFixture struct {
	t                *testing.T
	s                *postgres.Store
	human, host      context.Context
	person, consumer *auth.Identity
	project          string
	scope            storage.DeliveryHookHostScope
}

func newCompletionHookFixture(t *testing.T, project string) *completionHookFixture {
	t.Helper()
	s := newStore(t, postgres.WithProject(project))
	users, err := postgres.NewAuth(context.Background(), s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: project, Role: "admin"}, nil)
	require.NoError(t, err)
	service, err := users.CreateServiceAccount(context.Background(), &storage.User{Kind: storage.KindServiceAccount, DisplayName: project + " host", Role: "reader", OwnerUserID: human.ID})
	require.NoError(t, err)
	f := &completionHookFixture{t: t, s: s, project: project, person: &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman}, consumer: &auth.Identity{UserID: service.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}, scope: storage.DeliveryHookHostScope{EnvironmentID: project + "-env", NativeProjectID: project + "-native"}}
	f.human = auth.WithIdentity(context.Background(), f.person)
	f.host = auth.WithIdentity(context.Background(), f.consumer)
	return f
}

func (f *completionHookFixture) create(slug string) *storage.Spec {
	f.t.Helper()
	_, err := f.s.CreateSpec(f.human, slug, "Fixed completion hook fixture input for "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(f.t, err)
	stage := "approved"
	spec, err := f.s.UpdateSpec(f.human, slug, nil, &stage, nil, nil, nil)
	require.NoError(f.t, err)
	return spec
}

func (f *completionHookFixture) program(slug string, complete bool) *storage.ProgramRun {
	f.t.Helper()
	f.create(slug)
	// This command is inert fixture metadata. No ProcessRunner or project script is invoked.
	run, err := f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: slug, IdempotencyKey: "prepare-" + slug, Command: storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{"literal argument"}, Cwd: `C:\fixture\workspace`, EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID, TimeoutMS: 10000, WorkPurpose: "coordination", CompleteOnSuccess: complete}}, f.consumer)
	require.NoError(f.t, err)
	return run
}

func (f *completionHookFixture) request(source *storage.Spec, target *storage.ProgramRun, key string) storage.ArmCompletionHookRequest {
	return storage.ArmCompletionHookRequest{SourceTaskSlug: source.Slug, SourceSpecID: source.ID, TargetRunID: target.Context.RunID, TargetPackageID: target.Context.PackageID, IdempotencyKey: key, ConfirmTrigger: true}
}

func (f *completionHookFixture) read(id string) *storage.CompletionProgramHook {
	f.t.Helper()
	hook, err := f.s.ReadHumanCompletionProgramHook(f.human, id)
	require.NoError(f.t, err)
	return hook
}

func TestCompletionHookNodeDiscovery(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-discovery")
	source := f.create("source")
	f.create("empty")
	want := make(map[string]*storage.CompletionProgramHook)
	for _, state := range []string{"blocked", "unconfirmed", "cancelled", "admitted"} {
		target := f.program("target-"+state, false)
		hook, err := f.s.ArmHumanCompletionProgramHook(f.human, f.request(source, target, state), f.consumer)
		require.NoError(t, err)
		want[state] = hook
	}
	f.manual(source.Slug, "complete")
	// Keep one original hook untriggered alongside all triggered states.
	armed, err := f.s.ArmHumanCompletionProgramHook(f.human, f.request(source, f.program("target-armed", false), "armed"), f.consumer)
	require.NoError(t, err)
	want["armed"] = armed
	for _, state := range []string{"blocked", "unconfirmed"} {
		detail := "Private host diagnostic, not discovery metadata"
		_, err := f.s.RecordCompletionHookHostResult(f.host, f.scope, want[state].ID, storage.DeliveryHookHostResult{Status: state, Detail: &detail})
		require.NoError(t, err)
	}
	_, err = f.s.CancelHumanCompletionProgramHook(f.human, want["cancelled"].ID, "Private human cancellation text")
	require.NoError(t, err)
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, want["admitted"].ID)
	require.NoError(t, err)
	changed := "Changed source makes the original fact stale, not undiscoverable"
	_, err = f.s.UpdateSpec(f.human, source.Slug, &changed, nil, nil, nil, nil)
	require.NoError(t, err)
	otherSource := f.create("other-source")
	_, err = f.s.ArmHumanCompletionProgramHook(f.human, f.request(otherSource, f.program("other-target", false), "other"), f.consumer)
	require.NoError(t, err)
	foreign := newCompletionHookFixture(t, "completion-hook-discovery-foreign")
	_, err = foreign.s.ArmHumanCompletionProgramHook(foreign.human, foreign.request(foreign.create("source"), foreign.program("target", false), "foreign"), foreign.consumer)
	require.NoError(t, err)
	// Equal timestamps exercise the ID tie-break independently of creation order.
	_, err = f.s.Pool().Exec(f.human, `UPDATE delivery_test_hooks SET created_at='2026-09-27T00:00:00Z' WHERE project_slug=$1`, f.project)
	require.NoError(t, err)
	view, operation, err := server.ReadWorkbenchSpec(f.human, f.s, f.project, source.Slug)
	require.NoError(t, err, operation)
	hooks := view["completionHooks"].([]postgres.WorkbenchCompletionHook)
	require.Len(t, hooks, len(want), "no other source or project hooks")
	ids := make([]string, 0, len(hooks))
	for _, hook := range hooks {
		ids = append(ids, hook.ID)
		original := f.read(hook.ID)
		require.Equal(t, want[hook.State].ID, hook.ID)
		require.Equal(t, source.ID, hook.SourceSpecID)
		require.Equal(t, source.Slug, hook.SourceTaskSlug)
		require.Equal(t, "target-"+hook.State, hook.TargetTaskSlug)
		require.Equal(t, original.TargetRunID, hook.TargetRunID)
		require.Equal(t, original.TargetPackageID, hook.TargetPackageID)
		require.Equal(t, original.IdempotencyKey, hook.IdempotencyKey)
		require.Equal(t, original.RequestedFact, hook.RequestedFact)
		require.Equal(t, original.Fact, hook.Fact)
		require.Equal(t, original.TriggeredAt, hook.TriggeredAt)
		require.Equal(t, original.CancelledAt, hook.CancelledAt)
		require.Equal(t, original.DispatchStatus, hook.DispatchStatus)
		require.Equal(t, original.ProgramAdmissionID, hook.ProgramAdmissionID)
		if hook.State != "admitted" {
			run, err := f.s.ReadProgramRun(f.host, storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, hook.TargetRunID)
			require.NoError(t, err)
			require.Nil(t, run.Dispatch.Admission, "discovery must never authorize")
		}
	}
	require.True(t, sort.StringsAreSorted(ids), "created_at/id ordering must be deterministic")
	encoded, err := json.Marshal(hooks)
	require.NoError(t, err)
	for _, private := range []string{"never-executed", "literal argument", "Private", "hostConsumerUserId", "configuredByUserId", "admission", "dispatchDetail", "cancellationReason"} {
		require.NotContains(t, string(encoded), private)
	}
	empty, operation, err := server.ReadWorkbenchSpec(f.human, f.s, f.project, "empty")
	require.NoError(t, err, operation)
	encoded, err = json.Marshal(empty["completionHooks"])
	require.NoError(t, err)
	require.Equal(t, "[]", string(encoded))
	metadata, err := f.s.ReadWorkbenchMetadata(f.human)
	require.NoError(t, err)
	require.Empty(t, metadata.DeliveryHooks, "legacy delivery discovery remains delivery_test-only")
}

func (f *completionHookFixture) manual(slug, key string) storage.CompletionFactRef {
	f.t.Helper()
	spec, err := f.s.GetSpec(f.human, slug)
	require.NoError(f.t, err)
	_, _, err = f.s.ManualComplete(f.human, slug, f.person.UserID, spec.Version, key, "Explicit human completion fixture")
	require.NoError(f.t, err)
	facts, err := f.s.ListManualCompletions(f.human, slug)
	require.NoError(f.t, err)
	return storage.CompletionFactRef{Kind: "manual", ID: facts[0].ID}
}

func (f *completionHookFixture) holdSource(slug string) {
	f.t.Helper()
	reviewerSlug := "reviewer-" + slug
	f.create(reviewerSlug)
	run, err := f.s.PrepareRun(f.human, reviewerSlug, "review-fixture")
	require.NoError(f.t, err)
	require.NoError(f.t, f.s.BindRunThreadInEnvironment(f.human, run, "local", reviewerSlug))
	refs, err := f.s.ReadSpecSourceRefs(f.human, slug)
	require.NoError(f.t, err)
	maximum := int32(1)
	assigned, err := f.s.AssignReview(f.human, &storage.AssignReviewRequest{TaskSlug: slug, Kind: "requirements", Sources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: f.person.UserID}, ReviewerRunID: run, MaxReviewRounds: &maximum}, nil)
	require.NoError(f.t, err)
	result, err := f.s.SubmitReview(f.human, storage.MailScope{EnvironmentID: "local", ThreadID: reviewerSlug, ProviderSessionID: "session", ProviderInstanceID: "codex"}, assigned.Reviews[0].Request.ID, "rejected", "The source requires human intervention before new automatic execution")
	require.NoError(f.t, err)
	require.True(f.t, result.Status.Reviews[0].HumanHold)
}

func TestCompletionHookOriginalFactTransactions(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-facts")
	for _, kind := range []string{"manual", "execution", "program", "summary"} {
		t.Run(kind, func(t *testing.T) {
			source := f.create("source-" + kind)
			target := f.program("target-"+kind, false)
			var complete func(context.Context) error
			var sourceRun string
			var accepted *storage.SummaryAcceptance
			var summaryReq storage.SummaryAcceptRequest
			switch kind {
			case "manual":
				complete = func(ctx context.Context) error {
					_, _, err := f.s.ManualComplete(ctx, source.Slug, f.person.UserID, source.Version, "complete-"+kind, "Original human completion")
					return err
				}
			case "execution":
				body := json.RawMessage(`{"environmentId":"local","threadId":"completion-agent","workspace":"fixture","createCommandId":"create","startCommandId":"start","messageId":"message","workPurpose":"coordination","purposeGuidance":"Perform the coordination work"}`)
				var err error
				sourceRun, _, err = f.s.PrepareRunForOperator(f.human, source.Slug, "fixture", f.person.UserID, "agent-source", body)
				require.NoError(t, err)
				require.NoError(t, f.s.BindRunThreadInEnvironment(f.human, sourceRun, "local", "completion-agent"))
				complete = func(ctx context.Context) error { return f.s.RecordCompletion(ctx, source.Slug, sourceRun) }
			case "program":
				command := storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{}, Cwd: `C:\fixture\workspace`, EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID, TimeoutMS: 10000, WorkPurpose: "coordination", CompleteOnSuccess: true}
				run, err := f.s.PrepareProgramRun(f.human, &storage.PrepareProgramRunRequest{TaskSlug: source.Slug, IdempotencyKey: "source-program", Command: command}, f.consumer)
				require.NoError(t, err)
				sourceRun = run.Context.RunID
				scope := storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}
				_, err = f.s.AuthorizeProgramRun(f.host, scope, sourceRun)
				require.NoError(t, err)
				finished, zero := time.Now().UTC(), int64(0)
				_, err = f.s.RecordProgramResult(f.host, scope, sourceRun, &storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, ObservedAt: finished, FinishedAt: &finished})
				require.NoError(t, err)
				complete = func(ctx context.Context) error {
					return f.s.RecordCompletion(auth.WithIdentity(ctx, f.consumer), source.Slug, sourceRun)
				}
			case "summary":
				_, err := f.s.Pool().Exec(f.human, `UPDATE specs SET role='summary' WHERE project_slug=$1 AND slug=$2`, f.project, source.Slug)
				require.NoError(t, err)
				state, err := f.s.ReadSummary(f.human, source.Slug)
				require.NoError(t, err)
				refs, err := f.s.ReadSpecSourceRefs(f.human, source.Slug)
				require.NoError(t, err)
				summaryReq = storage.SummaryAcceptRequest{GoalSlug: source.Slug, Basis: "Explicit integration acceptance", GoalsSatisfied: true, IdempotencyKey: "source-summary", ExpectedReferences: state.References, EvidenceSources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: source.Slug, Field: "intent", ChangeID: refs["intent"]}}}
				complete = func(ctx context.Context) error {
					var err error
					accepted, err = f.s.AcceptSummary(ctx, summaryReq, nil)
					return err
				}
			}
			req := f.request(source, target, "arm-"+kind)
			hook, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
			require.NoError(t, err)
			require.Equal(t, req.IdempotencyKey, hook.IdempotencyKey)
			require.Nil(t, hook.RequestedFact)
			require.Nil(t, hook.Fact)
			rollback := errors.New("rollback original completion and its trigger")
			err = f.s.RunInTransaction(f.human, func(ctx context.Context) error {
				require.NoError(t, complete(ctx))
				pending, err := f.s.ArmHumanCompletionProgramHook(ctx, req, f.consumer)
				require.NoError(t, err)
				require.NotNil(t, pending.Fact, "the original owner publishes the trigger in the same transaction")
				return rollback
			})
			require.ErrorIs(t, err, rollback)
			require.Nil(t, f.read(hook.ID).Fact)
			require.NoError(t, complete(f.human))
			pending := f.read(hook.ID)
			require.Equal(t, "pending", pending.State)
			require.Equal(t, req.IdempotencyKey, pending.IdempotencyKey)
			require.Nil(t, pending.RequestedFact, "triggered fact must not backfill the original request")
			require.NotNil(t, pending.Fact)
			switch kind {
			case "manual":
				facts, err := f.s.ListManualCompletions(f.human, source.Slug)
				require.NoError(t, err)
				require.Len(t, facts, 1)
				require.Equal(t, storage.CompletionFactRef{Kind: "manual", ID: facts[0].ID}, *pending.Fact)
			case "summary":
				require.Equal(t, storage.CompletionFactRef{Kind: "summary", ID: accepted.ID}, *pending.Fact)
			default:
				events, err := f.s.GetExecutionEvents(f.human, source.Slug, 0)
				require.NoError(t, err)
				require.Len(t, events, 1)
				require.Equal(t, storage.CompletionFactRef{Kind: "execution", ID: events[0].ID}, *pending.Fact)
			}
			if kind != "execution" {
				require.NoError(t, complete(f.human))
			}
			replayed, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
			require.NoError(t, err)
			require.Equal(t, pending.Fact, replayed.Fact)
			require.Equal(t, pending.TriggeredAt, replayed.TriggeredAt)
			if kind == "summary" {
				ready, err := f.s.ReadHostCompletionHook(f.host, f.scope, hook.ID)
				require.NoError(t, err)
				require.Nil(t, ready.ReadinessError)
				secondReq := summaryReq
				secondReq.IdempotencyKey = "second-source-summary"
				second, err := f.s.AcceptSummary(f.human, secondReq, nil)
				require.NoError(t, err)
				require.NotEqual(t, accepted.ID, second.ID)
				_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
				require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "A1 is no longer the summary owner's current acceptance once A2 exists")
				_, err = f.s.RevokeSummaryAcceptance(f.human, second.ID, "The current source acceptance is withdrawn", nil)
				require.NoError(t, err)
				_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
				require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "revoking A2 cannot revive the unrevoked but superseded A1")
				_, err = f.s.CancelHumanCompletionProgramHook(f.human, hook.ID, "Replace the stale trigger only through explicit confirmation")
				require.NoError(t, err)
				late := req
				late.IdempotencyKey = "late-old-summary"
				late.Fact = pending.Fact
				_, err = f.s.ArmHumanCompletionProgramHook(f.human, late, f.consumer)
				require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "explicit selection still cannot authorize a non-current acceptance")
			}
		})
	}
}

func TestCompletionHookConfirmationIdentityAndStageOnly(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-boundaries")
	source := f.create("source")
	target := f.program("target", false)
	req := f.request(source, target, "arm")
	bad := req
	bad.ConfirmTrigger = false
	_, err := f.s.ArmHumanCompletionProgramHook(f.human, bad, f.consumer)
	require.ErrorIs(t, err, storage.ErrInvalidCompletionHook)
	_, err = f.s.ArmHumanCompletionProgramHook(f.host, req, f.consumer)
	require.Error(t, err, "a service/PM credential is not human confirmation")
	_, err = f.s.ArmHumanCompletionProgramHook(f.human, req, f.person)
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
	for _, field := range []string{"source-id", "package", "run"} {
		bad = req
		switch field {
		case "source-id":
			bad.SourceSpecID = "another-spec"
		case "package":
			bad.TargetPackageID = "another-package"
		case "run":
			bad.TargetRunID = "missing-run"
		}
		_, err = f.s.ArmHumanCompletionProgramHook(f.human, bad, f.consumer)
		require.Error(t, err, field)
	}
	hook, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
	require.NoError(t, err)
	otherService, err := f.s.ExistingAuth().CreateServiceAccount(f.human, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Another host", Role: "reader", OwnerUserID: f.person.UserID})
	require.NoError(t, err)
	otherIdentity := &auth.Identity{UserID: otherService.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}
	_, err = f.s.ArmHumanCompletionProgramHook(f.human, req, otherIdentity)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "replay cannot replace the original host consumer")
	_, err = f.s.HostAuthorizeCompletionHook(auth.WithIdentity(context.Background(), otherIdentity), f.scope, hook.ID)
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
	require.Equal(t, f.person.UserID, hook.ConfiguredByUserID)
	require.Equal(t, f.consumer.UserID, hook.HostConsumerUserID)
	require.Equal(t, "work", hook.SourceRole)
	_, err = f.s.AuthorizeProgramRun(f.host, storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, target.Context.RunID)
	require.ErrorIs(t, err, storage.ErrProgramHookRequired)
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "armed is not triggered")
	done := "done"
	_, err = f.s.UpdateSpec(f.human, source.Slug, nil, &done, nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, f.read(hook.ID).Fact, "stage-only update cannot manufacture a completion fact")
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
	cancelled, err := f.s.CancelHumanCompletionProgramHook(f.human, hook.ID, "Explicitly withdraw automatic start consent")
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled.State)
	run, err := f.s.ReadProgramRun(f.host, storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, target.Context.RunID)
	require.NoError(t, err)
	require.Equal(t, "prepared", run.State)
	require.Nil(t, run.Dispatch.Cancellation, "cancel hook is not cancel preparation or process stop")
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "cancelled trigger cannot silently fall back to manual start")
	_, err = f.s.CancelHumanDeliveryTestHook(f.human, hook.ID, "Wrong hook kind")
	require.ErrorIs(t, err, storage.ErrDeliveryHookNotFound, "the old hook reader must filter kind")
}

func TestCompletionHookDeadlockAndProgramProjectAssociation(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-association")
	for _, relation := range []storage.EdgeType{storage.EdgeTypeDependsOn, storage.EdgeTypeComposes} {
		suffix := strings.ToLower(string(relation))
		source := f.create("source-" + suffix)
		target := f.program("target-"+suffix, false)
		if relation == storage.EdgeTypeComposes {
			_, err := f.s.Pool().Exec(f.human, `UPDATE specs SET role='summary' WHERE project_slug=$1 AND slug=$2`, f.project, source.Slug)
			require.NoError(t, err)
		}
		_, err := f.s.AddEdge(f.human, source.Slug, target.Context.TaskSlug, relation)
		require.NoError(t, err)
		_, err = f.s.ArmHumanCompletionProgramHook(f.human, f.request(source, target, "deadlock-"+suffix), f.consumer)
		require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "source completion cannot wait for the program it would trigger")
		_, err = f.s.GetSpec(f.human, target.Context.TaskSlug)
		require.NoError(t, err, "reject configuration without deleting the work obligation")
	}
	project, err := f.s.ProjectForWorkbenchKnowledge(f.host, f.scope.EnvironmentID, f.scope.NativeProjectID)
	require.NoError(t, err)
	require.Equal(t, f.project, project, "program_target alone supplies the recorded native project association")
	_, err = f.s.ProjectForWorkbenchKnowledge(f.host, "other-environment", f.scope.NativeProjectID)
	require.ErrorIs(t, err, postgres.ErrWorkbenchProjectAssociationMissing)
	_, err = f.s.EnsureProject(f.human, "completion-hook-association-other")
	require.NoError(t, err)
	other, err := f.s.ScopedExisting(f.human, "completion-hook-association-other")
	require.NoError(t, err)
	f2 := *f
	f2.s = other
	f2.project = "completion-hook-association-other"
	f2.scope.EnvironmentID = "other-environment"
	f2.program("other-program", false)
	project, err = f.s.ProjectForWorkbenchKnowledge(f.host, f.scope.EnvironmentID, f.scope.NativeProjectID)
	require.NoError(t, err)
	require.Equal(t, f.project, project)
	f2.scope = f.scope
	f2.program("conflicting-program", false)
	_, err = f.s.ProjectForWorkbenchKnowledge(f.host, f.scope.EnvironmentID, f.scope.NativeProjectID)
	require.ErrorIs(t, err, postgres.ErrWorkbenchProjectAssociationAmbiguous, "two SpecGraph projects must not be guessed into one")
}

func TestCompletionHookUnifiedPagingPreservesDeliveryKind(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-paging")
	source := f.create("completion-source")
	fact := f.manual(source.Slug, "completion")
	want := []string{}
	for _, slug := range []string{"program-one", "program-two"} {
		target := f.program(slug, false)
		req := f.request(source, target, "arm-"+slug)
		req.Fact = &fact
		hook, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
		require.NoError(t, err)
		require.Equal(t, req.IdempotencyKey, hook.IdempotencyKey)
		require.Equal(t, &fact, hook.RequestedFact)
		require.Equal(t, &fact, hook.Fact)
		want = append(want, hook.ID)
	}
	plan := f.create("plan")
	refs, err := f.s.ReadSpecSourceRefs(f.human, plan.Slug)
	require.NoError(t, err)
	planSource := storage.ReviewSource{Kind: "specgraph", SpecSlug: plan.Slug, Field: "intent", ChangeID: refs["intent"]}
	f.create("implementation")
	sourceRun, err := f.s.PrepareRun(f.human, "implementation", "fixture-source")
	require.NoError(t, err)
	// Original assignment fixture mirrors the existing delivery-hook tests; sources are real references.
	sourceTarget, err := json.Marshal(map[string]any{"workPurpose": "implementation", "qaBasis": storage.DispatchQABasis{TestPlanSources: []storage.ReviewSource{planSource}}})
	require.NoError(t, err)
	_, err = f.s.Pool().Exec(f.human, `UPDATE context_packages SET body=body||jsonb_build_object('dispatch_target',$2::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, sourceRun, sourceTarget)
	require.NoError(t, err)
	f.create("test-target")
	commit := strings.Repeat("a", 40)
	body, err := json.Marshal(map[string]any{"environmentId": f.scope.EnvironmentID, "projectId": f.scope.NativeProjectID, "runtimeMode": "full-access", "threadId": "test-thread", "workspace": "test-workspace", "createCommandId": "create-test", "startCommandId": "start-test", "messageId": "message-test", "workPurpose": "test_execution", "purposeGuidance": "Run the fixed original plan", "gitBaseline": map[string]any{"isRepo": true, "commitSha": commit}, "qaBasis": storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{planSource}, Applicability: "Applies to the fixed submitted commit"}})
	require.NoError(t, err)
	testRun, _, err := f.s.PrepareRunForOperator(f.human, "test-target", "test-workspace", f.person.UserID, "test-preparation", body)
	require.NoError(t, err)
	deliveryHook, err := f.s.ArmHumanDeliveryTestHook(f.human, storage.ArmDeliveryHookRequest{SourceRunID: sourceRun, TargetRunID: testRun, CommitSHA: commit, IdempotencyKey: "delivery"}, f.consumer)
	require.NoError(t, err)
	_, err = f.s.CreateDelivery(f.human, sourceRun, json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"`+commit+`"}}}`), sourceRun)
	require.NoError(t, err)
	want = append(want, deliveryHook.ID)
	seen, kinds := []string{}, []string{}
	cursor := ""
	for {
		page, err := f.s.ListHostHooks(f.host, f.scope, 1, cursor)
		require.NoError(t, err)
		require.Len(t, page.Hooks, 1, "both hook kinds share one total page budget")
		hook := page.Hooks[0]
		kinds = append(kinds, hook.Kind)
		if hook.Kind == "delivery_test" {
			require.NotNil(t, hook.DeliveryTest)
			require.Nil(t, hook.CompletionProgram)
			seen = append(seen, hook.DeliveryTest.ID)
		} else {
			require.Equal(t, "completion_program", hook.Kind)
			require.Nil(t, hook.DeliveryTest)
			seen = append(seen, hook.CompletionProgram.ID)
		}
		if page.NextCursor == "" {
			break
		}
		require.NotEqual(t, cursor, page.NextCursor)
		cursor = page.NextCursor
		require.Less(t, len(seen), 4, "paging must terminate without repeating either kind")
	}
	require.ElementsMatch(t, want, seen)
	require.ElementsMatch(t, []string{"completion_program", "completion_program", "delivery_test"}, kinds)
	old, err := f.s.ListHostDeliveryTestHooks(f.host, f.scope, 100, "")
	require.NoError(t, err)
	require.Len(t, old.Hooks, 1)
	require.Equal(t, deliveryHook.ID, old.Hooks[0].ID)
	_, err = f.s.ReadHumanCompletionProgramHook(f.human, deliveryHook.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookNotFound)
	otherScope := f.scope
	otherScope.EnvironmentID = "other"
	page, err := f.s.ListHostHooks(f.host, otherScope, 10, "")
	require.NoError(t, err)
	require.Empty(t, page.Hooks)
	_, err = f.s.ListHostHooks(f.human, f.scope, 10, "")
	require.ErrorIs(t, err, storage.ErrProgramRunForbidden, "association cannot confer host authorization")
}

func TestCompletionHookStaleFactReconfirmationAndExactAdmission(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-rearm")
	source := f.create("source")
	target := f.program("target", false)
	h1, err := f.s.ArmHumanCompletionProgramHook(f.human, f.request(source, target, "first"), f.consumer)
	require.NoError(t, err)
	first := f.manual(source.Slug, "first-completion")
	require.Equal(t, &first, f.read(h1.ID).Fact)
	approved := "approved"
	_, err = f.s.UpdateSpec(f.human, source.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, h1.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
	second := f.manual(source.Slug, "second-completion")
	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, &first, f.read(h1.ID).Fact, "later completion cannot silently replace the selected event")
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, h1.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
	_, err = f.s.CancelHumanCompletionProgramHook(f.human, h1.ID, "Reconfirm the actual new completion")
	require.NoError(t, err)
	req := f.request(source, target, "second")
	req.Fact = &second
	h2, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
	require.NoError(t, err)
	_, err = f.s.Pool().Exec(f.human, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug=$1 AND spec_slug='target'`, f.project)
	require.NoError(t, err)
	admission, err := f.s.HostAuthorizeCompletionHook(f.host, f.scope, h2.ID)
	require.NoError(t, err, "a delayed trigger may renew only its target's existing claim")
	require.True(t, admission.MayStart)
	old, current := f.read(h1.ID), f.read(h2.ID)
	require.Nil(t, old.Admission, "H1 cannot inherit H2's admission merely because both used the same target")
	require.Nil(t, old.ProgramAdmissionID)
	require.Equal(t, admission.Run.Dispatch.Admission.ID, *current.ProgramAdmissionID)
	require.Equal(t, admission.Run.Dispatch.Admission.ID, current.Admission.ID)
	require.Equal(t, admission.Run.Dispatch.Admission.RunID, current.Admission.RunID)
	require.Equal(t, admission.Run.Dispatch.Admission.PackageID, current.Admission.PackageID)
	require.Equal(t, admission.Run.Dispatch.Admission.Actor, current.Admission.Actor)
	require.WithinDuration(t, admission.Run.Dispatch.Admission.AuthorizedAt, current.Admission.AuthorizedAt, time.Microsecond, "PostgreSQL timestamps preserve the instant at microsecond precision")
	var attemptID, admissionID string
	var ordinal int
	require.NoError(t, f.s.Pool().QueryRow(f.host, `SELECT id,admission_id,ordinal FROM program_attempts WHERE project_slug=$1 AND run_id=$2`, f.project, target.Context.RunID).Scan(&attemptID, &admissionID, &ordinal))
	require.NotEmpty(t, attemptID)
	require.Equal(t, *current.ProgramAdmissionID, admissionID)
	require.Equal(t, 1, ordinal)
	_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, h1.ID)
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
	changed := "Changed source after a real admission does not erase the attempt"
	_, err = f.s.UpdateSpec(f.human, source.Slug, &changed, nil, nil, nil, nil)
	require.NoError(t, err)
	f.holdSource(source.Slug)
	replayed, err := f.s.HostAuthorizeCompletionHook(f.host, f.scope, h2.ID)
	require.NoError(t, err)
	require.False(t, replayed.MayStart, "once admitted, never physically execute a second time")
	require.NotNil(t, f.read(h2.ID).Admission)
	_, err = f.s.CancelHumanCompletionProgramHook(f.human, h2.ID, "Cannot claim this already admitted program stopped")
	require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
	finished, zero := time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC), int64(0)
	result, err := f.s.RecordProgramResult(f.host, storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, target.Context.RunID, &storage.ProgramObservation{Outcome: "exited", ExitCode: &zero, ObservedAt: finished, FinishedAt: &finished})
	require.NoError(t, err)
	loaded, err := f.s.ReadHostCompletionHook(f.host, f.scope, h2.ID)
	require.NoError(t, err)
	require.Equal(t, result, loaded.Run.Result, "hook consumption reads the original program result, not a second result record")
	var attempts int
	require.NoError(t, f.s.Pool().QueryRow(f.host, `SELECT count(*) FROM program_attempts WHERE project_slug=$1 AND run_id=$2`, f.project, target.Context.RunID).Scan(&attempts))
	require.Equal(t, 1, attempts, "hook replay and result reporting retain the first attempt")
	var encoded []byte
	require.NoError(t, f.s.Pool().QueryRow(f.host, `SELECT result FROM program_attempts WHERE project_slug=$1 AND run_id=$2 AND id=$3 AND admission_id=$4 AND ordinal=1`, f.project, target.Context.RunID, attemptID, admissionID).Scan(&encoded))
	var storedResult storage.ProgramRunResult
	require.NoError(t, json.Unmarshal(encoded, &storedResult))
	require.Equal(t, *result, storedResult)
}

func TestCompletionHookSourceValidityClaimsAndScopes(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-validity")
	for _, scenario := range []string{"content", "missing-claim", "other-claim", "legacy", "human-hold"} {
		t.Run(scenario, func(t *testing.T) {
			source := f.create("source-" + scenario)
			target := f.program("target-"+scenario, false)
			fact := f.manual(source.Slug, "complete-"+scenario)
			req := f.request(source, target, "arm-"+scenario)
			req.Fact = &fact
			if scenario == "legacy" {
				_, err := f.s.Pool().Exec(f.human, `UPDATE manual_completions SET completion_context=NULL WHERE project_slug=$1 AND id=$2`, f.project, fact.ID)
				require.NoError(t, err)
				_, err = f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
				require.ErrorIs(t, err, storage.ErrCompletionHookConflict, "legacy unknown applicability must not be invented")
				return
			}
			hook, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
			require.NoError(t, err)
			for _, scope := range []storage.DeliveryHookHostScope{{EnvironmentID: "wrong", NativeProjectID: f.scope.NativeProjectID}, {EnvironmentID: f.scope.EnvironmentID, NativeProjectID: "wrong"}} {
				_, err = f.s.HostAuthorizeCompletionHook(f.host, scope, hook.ID)
				require.Error(t, err)
			}
			_, err = f.s.HostAuthorizeCompletionHook(f.human, f.scope, hook.ID)
			require.ErrorIs(t, err, storage.ErrProgramRunForbidden)
			switch scenario {
			case "content":
				intent := "Changed after the selected completion"
				_, err = f.s.UpdateSpec(f.human, source.Slug, &intent, nil, nil, nil, nil)
			case "missing-claim":
				_, err = f.s.Pool().Exec(f.human, `DELETE FROM claims WHERE project_slug=$1 AND spec_slug=$2`, f.project, target.Context.TaskSlug)
			case "other-claim":
				_, err = f.s.Pool().Exec(f.human, `UPDATE claims SET agent='another-owner',lease_expires=now()-interval '1 minute' WHERE project_slug=$1 AND spec_slug=$2`, f.project, target.Context.TaskSlug)
			case "human-hold":
				f.holdSource(source.Slug)
				err = nil
			}
			require.NoError(t, err)
			_, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
			require.Error(t, err, "invalid source or missing/replaced claim cannot be renewed into eligibility")
			if scenario == "human-hold" {
				require.ErrorIs(t, err, storage.ErrReviewHumanHold)
			}
			run, err := f.s.ReadProgramRun(f.host, storage.ProgramHostScope{EnvironmentID: f.scope.EnvironmentID, NativeProjectID: f.scope.NativeProjectID}, target.Context.RunID)
			require.NoError(t, err)
			require.Nil(t, run.Dispatch.Admission)
		})
	}
}

func TestCompletionHookAncestorAndDecisionScope(t *testing.T) {
	f := newCompletionHookFixture(t, "completion-hook-inherited-scope")
	for _, scenario := range []string{"ancestor-change", "decision-change", "noops-and-unrelated", "composition-aba", "decision-aba", "legacy-scope", "already-admitted"} {
		t.Run(scenario, func(t *testing.T) {
			source := f.create("source-" + scenario)
			parent := f.create("parent-" + scenario)
			target := f.program("target-"+scenario, false)
			decision, err := f.s.CreateDecision(f.human, "decision-"+scenario, "Original applicable decision", "Original decision body", "Original rationale", "", nil, "", nil, "", "", "")
			require.NoError(t, err)
			if scenario != "composition-aba" {
				_, err = f.s.AddEdge(f.human, parent.Slug, source.Slug, storage.EdgeTypeComposes)
				require.NoError(t, err)
			}
			if scenario != "decision-aba" {
				_, err = f.s.AddEdge(f.human, parent.Slug, decision.Slug, storage.EdgeTypeDecidedIn)
				require.NoError(t, err)
			}
			fact := f.manual(source.Slug, "complete-"+scenario)
			req := f.request(source, target, "arm-"+scenario)
			req.Fact = &fact
			hook, err := f.s.ArmHumanCompletionProgramHook(f.human, req, f.consumer)
			require.NoError(t, err)
			var firstAdmission *storage.ProgramAdmission
			if scenario == "already-admitted" {
				firstAdmission, err = f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
				require.NoError(t, err)
				require.True(t, firstAdmission.MayStart)
			}
			switch scenario {
			case "ancestor-change", "already-admitted":
				intent := "A new inherited requirement after the source completed"
				_, err = f.s.UpdateSpec(f.human, parent.Slug, &intent, nil, nil, nil, nil)
				require.NoError(t, err)
				if scenario == "already-admitted" {
					body := "Changed associated decision after the one real admission"
					_, err = f.s.UpdateDecision(f.human, decision.Slug, 0, nil, nil, &body, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					require.NoError(t, err)
				}
			case "decision-change":
				body := "A materially changed decision after the source completed"
				_, err = f.s.UpdateDecision(f.human, decision.Slug, 0, nil, nil, &body, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				require.NoError(t, err)
			case "composition-aba":
				_, err = f.s.AddEdge(f.human, parent.Slug, source.Slug, storage.EdgeTypeComposes)
				require.NoError(t, err)
				require.NoError(t, f.s.RemoveEdge(f.human, parent.Slug, source.Slug, storage.EdgeTypeComposes))
			case "decision-aba":
				_, err = f.s.AddEdge(f.human, parent.Slug, decision.Slug, storage.EdgeTypeDecidedIn)
				require.NoError(t, err)
				require.NoError(t, f.s.RemoveEdge(f.human, parent.Slug, decision.Slug, storage.EdgeTypeDecidedIn))
			case "legacy-scope":
				_, err = f.s.Pool().Exec(f.human, `UPDATE manual_completions SET completion_context=completion_context-'scope' WHERE project_slug=$1 AND id=$2`, f.project, fact.ID)
				require.NoError(t, err)
			case "noops-and-unrelated":
				_, err = f.s.UpdateSpec(f.human, parent.Slug, &parent.Intent, nil, nil, nil, nil)
				require.NoError(t, err)
				_, err = f.s.UpdateDecision(f.human, decision.Slug, 0, &decision.Title, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				require.NoError(t, err)
				unrelated, err := f.s.CreateDecision(f.human, "unrelated-decision", "Unrelated", "Body", "Rationale", "", nil, "", nil, "", "", "")
				require.NoError(t, err)
				changed := "An unrelated decision changed"
				_, err = f.s.UpdateDecision(f.human, unrelated.Slug, 0, &changed, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				require.NoError(t, err)
				otherParent, otherChild := f.create("unrelated-parent"), f.create("unrelated-child")
				_, err = f.s.AddEdge(f.human, otherParent.Slug, otherChild.Slug, storage.EdgeTypeComposes)
				require.NoError(t, err)
				require.NoError(t, f.s.RemoveEdge(f.human, otherParent.Slug, otherChild.Slug, storage.EdgeTypeComposes))
			}
			current, err := f.s.GetSpec(f.human, source.Slug)
			require.NoError(t, err)
			require.Equal(t, storage.SpecStageDone, current.Stage, "the source remains done; changed applicability independently gates the pending program")
			admitted, err := f.s.HostAuthorizeCompletionHook(f.host, f.scope, hook.ID)
			switch scenario {
			case "noops-and-unrelated":
				require.NoError(t, err, "unchanged applicable content and unrelated graph history must not invalidate the fact")
				require.True(t, admitted.MayStart)
			case "already-admitted":
				require.NoError(t, err)
				require.False(t, admitted.MayStart)
				require.Equal(t, firstAdmission.Run.Dispatch.Admission.ID, admitted.Run.Dispatch.Admission.ID)
			default:
				require.ErrorIs(t, err, storage.ErrCompletionHookConflict)
				require.Nil(t, f.read(hook.ID).Admission)
			}
			if scenario == "legacy-scope" {
				var hasScope bool
				require.NoError(t, f.s.Pool().QueryRow(f.human, `SELECT completion_context ? 'scope' FROM manual_completions WHERE project_slug=$1 AND id=$2`, f.project, fact.ID).Scan(&hasScope))
				require.False(t, hasScope, "unknown historical scope must not be backfilled using today's graph")
			}
		})
	}
}
