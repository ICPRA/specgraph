// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

// Registered reports here are synthetic assertions; no test command or model is run.
func TestCandidateLoopFormalDeliveryBudget(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("candidate-loop"))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Candidate operator", Role: "admin"}, nil)
	require.NoError(t, err)
	operator := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	makeSpec := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Original "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		approved := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	makeSpec("reviewer")
	reviewer, err := s.PrepareRun(ctx, "reviewer", "C:/candidate-reviewer")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, reviewer, "local", "reviewer-thread"))
	approve := func(slug, kind string, requirements []string) string {
		t.Helper()
		source := makeSpec(slug)
		state, err := s.AssignReview(operator, &storage.AssignReviewRequest{TaskSlug: slug, Kind: kind,
			Sources: []storage.ReviewSource{source}, RequirementDecisionIDs: requirements,
			AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer}, nil)
		require.NoError(t, err)
		index := 0
		if kind == "design" {
			index = 1
		}
		decision, err := s.ReviewSource(operator, slug, state.Reviews[index].Request.ID, "accepted", "Synthetic source approval")
		require.NoError(t, err)
		return decision.Decision.ID
	}
	requirementID := approve("candidate-requirement", "requirements", nil)
	designID := approve("candidate-design", "design", []string{requirementID})
	plan := makeSpec("candidate-plan")
	additionalPlan := makeSpec("candidate-plan-two")
	makeSpec("implementation")
	basis := storage.DispatchQABasis{RequirementDecisionIDs: []string{requirementID}, DesignDecisionIDs: []string{designID},
		DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{plan, additionalPlan}, Applicability: "Synthetic fixed candidate plan"}
	target, err := json.Marshal(map[string]any{
		"workPurpose": "implementation", "purposeGuidance": "Produce one formal candidate and verify its assigned plan.",
		"environmentId": "local", "threadId": "candidate-thread", "workspace": "C:/candidate-impl",
		"createCommandId": "create-candidate", "startCommandId": "start-candidate", "messageId": "message-candidate", "qaBasis": basis,
	})
	require.NoError(t, err)
	run, _, err := s.PrepareRunForOperator(ctx, "implementation", "C:/candidate-impl", human.ID, "prepare-candidate", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "candidate-thread"))
	prepared, err := s.ReadRunContext(ctx, run)
	require.NoError(t, err)
	assertSummary := func(runID string, exact *storage.CandidateLoop) {
		t.Helper()
		metadata, err := s.ReadWorkbenchMetadata(ctx)
		require.NoError(t, err)
		for _, item := range metadata.Runs {
			if item.ID != runID {
				continue
			}
			require.NotNil(t, item.CandidateLoop)
			require.Equal(t, exact.Status, item.CandidateLoop.Status)
			require.Equal(t, exact.MaxAttempts, item.CandidateLoop.MaxAttempts)
			require.Equal(t, exact.ConfiguredBy, item.CandidateLoop.ConfiguredBy)
			if exact.CurrentAttempt == nil {
				require.Nil(t, item.CandidateLoop.AttemptID)
				require.Nil(t, item.CandidateLoop.AttemptOrdinal)
				require.Nil(t, item.CandidateLoop.DeliveryID)
			} else {
				require.Equal(t, exact.CurrentAttempt.ID, *item.CandidateLoop.AttemptID)
				require.Equal(t, exact.CurrentAttempt.Number, *item.CandidateLoop.AttemptOrdinal)
				require.Equal(t, exact.CurrentAttempt.DeliveryID, item.CandidateLoop.DeliveryID)
			}
			return
		}
		t.Fatalf("snapshot omitted exact candidate run %s", runID)
	}
	loop, err := s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: run, MaxAttempts: 2, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	require.Equal(t, "waiting", loop.Status)
	readBody, err := json.Marshal(map[string]string{"runId": run})
	require.NoError(t, err)
	readReply, err := server.ExecuteHumanCandidateLoopRead(operator, s, "candidate-loop", bytes.NewReader(readBody))
	require.NoError(t, err, "offline human exact-run detail does not need native host association")
	var offlineRead storage.CandidateLoop
	require.NoError(t, json.Unmarshal(readReply, &offlineRead))
	require.Equal(t, run, offlineRead.RunID)
	require.Equal(t, "waiting", offlineRead.Status)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{}`), human.ID)
	require.ErrorIs(t, err, storage.ErrInvalidCandidateLoop, "common delivery owner rejects a free candidate before dispatch")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: run, MaxAttempts: 3, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.ErrorIs(t, err, storage.ErrCandidateLoopConflict)
	_, err = s.Pool().Exec(ctx, `INSERT INTO run_bindings(id,project_slug,task_spec_slug,package_id,generation,thread_ref,workspace,state,created_at,updated_at,environment_id,executor_kind)
 SELECT 'candidate-other',project_slug,task_spec_slug,package_id,generation,thread_ref,workspace,state,created_at,updated_at,environment_id,executor_kind
 FROM run_bindings WHERE project_slug='candidate-loop' AND id=$1`, run)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, "candidate-other", human.ID, prepared.PackageID, target)
	require.ErrorIs(t, err, storage.ErrCandidateBudgetHeld, "a prepared other run cannot reset unresolved budget")
	_, err = s.Pool().Exec(ctx, `DELETE FROM run_bindings WHERE project_slug='candidate-loop' AND id='candidate-other'`)
	require.NoError(t, err)
	admission, err := s.AuthorizeRunDispatch(ctx, run, human.ID, prepared.PackageID, target)
	require.NoError(t, err)
	require.NotNil(t, admission.CandidateAttemptID)
	firstID := *admission.CandidateAttemptID
	replay, err := s.AuthorizeRunDispatch(ctx, run, human.ID, prepared.PackageID, target)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, firstID, *replay.CandidateAttemptID)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "candidate", loop.Status)
	require.Len(t, loop.Attempts, 1)
	assertSummary(run, loop)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{}`), human.ID, firstID)
	require.ErrorIs(t, err, storage.ErrInvalidCandidateLoop, "invalid Git snapshot cannot consume the sole formal delivery slot")
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{"git":{"head":{"isRepo":false,"commitSha":null}}}`), human.ID, firstID)
	require.ErrorIs(t, err, storage.ErrInvalidCandidateLoop)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Nil(t, loop.CurrentAttempt.DeliveryID)
	commit1 := strings.Repeat("b", 40)
	snapshot1 := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + commit1 + `"}}}`)
	delivery1, err := s.CreateDelivery(ctx, run, snapshot1, human.ID, firstID)
	require.NoError(t, err)
	replayedDelivery, err := s.CreateDelivery(ctx, run, snapshot1, human.ID, firstID)
	require.NoError(t, err)
	require.Equal(t, delivery1, replayedDelivery)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{"candidate":2}`), human.ID, firstID)
	require.ErrorIs(t, err, storage.ErrCandidateLoopConflict)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "waiting", loop.Status)
	assertSummary(run, loop)
	scope := storage.MailScope{EnvironmentID: "local", ThreadID: "candidate-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	_, err = s.NextOwnCandidateAttempt(operator, scope, firstID)
	require.ErrorIs(t, err, storage.ErrCandidateLoopWaiting)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: delivery1, CommitSHA: commit1, PlanSources: []storage.ReviewSource{plan}, Status: "failed",
		Summary: "Synthetic failed candidate", OutputRefs: []string{"fixture://candidate-1"},
	}}, nil)
	require.NoError(t, err)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "ready_next", loop.Status)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", run), storage.ErrCandidateConditionNotMet)
	beforeOwnership, err := s.GetSpec(ctx, "implementation")
	require.NoError(t, err)
	pendingOwnership, err := s.BeginNodeOwnershipTake(operator, storage.BeginNodeOwnershipTakeRequest{
		TaskSlug: "implementation", ExpectedVersion: beforeOwnership.Version, IdempotencyKey: "candidate-pending-owner",
		Reason: "Pause any new attempt while old work hands off",
	})
	require.NoError(t, err)
	require.Equal(t, "pending", pendingOwnership.Status)
	_, err = s.NextOwnCandidateAttempt(operator, scope, firstID)
	require.ErrorIs(t, err, storage.ErrNodeOwnershipPending)
	_, err = s.CancelNodeOwnershipTake(operator, storage.CancelNodeOwnershipTakeRequest{OperationID: pendingOwnership.ID, Reason: "Continue original candidate work"})
	require.NoError(t, err)
	next, err := s.NextOwnCandidateAttempt(operator, scope, firstID)
	require.NoError(t, err)
	require.Equal(t, 2, next.GrantedAttempt.Number)
	require.Equal(t, firstID, *next.GrantedAttempt.PredecessorID)
	require.Equal(t, "false", next.GrantedAttempt.ConsumedCondition.Value)
	secondID := next.GrantedAttempt.ID
	nextReplay, err := s.NextOwnCandidateAttempt(operator, scope, firstID)
	require.NoError(t, err)
	require.True(t, nextReplay.Replayed)
	require.Equal(t, secondID, nextReplay.GrantedAttempt.ID)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{"candidate":3}`), human.ID, firstID)
	require.ErrorIs(t, err, storage.ErrCandidateLoopConflict, "late old candidate cannot attach to the new attempt")
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "candidate", loop.Status, "max=2 does not exhaust an unsubmitted second candidate")
	commit2 := strings.Repeat("c", 40)
	snapshot2 := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + commit2 + `"}}}`)
	delivery2, err := s.CreateDelivery(ctx, run, snapshot2, human.ID, secondID)
	require.NoError(t, err)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: delivery2, CommitSHA: commit2, PlanSources: []storage.ReviewSource{plan}, Status: "passed",
		Command: "synthetic assertion", Summary: "Synthetic passed candidate", OutputRefs: []string{"fixture://candidate-2"},
	}}, nil)
	require.NoError(t, err)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "condition_met", loop.Status, "true wins even at the attempt maximum")
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", run), storage.ErrImplementationTestsRequired,
		"candidate stop plan is a subset, never a reduction of original QA completion obligations")
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: delivery2, CommitSHA: commit2, PlanSources: []storage.ReviewSource{additionalPlan}, Status: "passed",
		Command: "synthetic assertion", Summary: "Additional original QA plan passed", OutputRefs: []string{"fixture://extra-plan"},
	}}, nil)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "implementation", run))
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "completed", loop.Status)
	require.NotNil(t, loop.CompletedAt)
	assertSummary(run, loop)
	_, err = s.AbandonCandidateLoop(operator, run, "Cannot abandon completed work")
	require.ErrorIs(t, err, storage.ErrCandidateLoopCompleted)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: delivery2, CommitSHA: commit2, PlanSources: []storage.ReviewSource{plan}, Status: "failed",
		Summary: "Late revised assertion", OutputRefs: []string{"fixture://late"},
	}}, nil)
	require.NoError(t, err)
	loop, err = s.ReadCandidateLoop(ctx, run)
	require.NoError(t, err)
	require.Equal(t, "completed", loop.Status, "historical completion cannot become a fresh budget need")
	require.Equal(t, "false", loop.Condition.Value, "current report remains separately visible")

	prepareMore := func(slug string) (string, string, json.RawMessage, storage.MailScope) {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"workPurpose": "implementation", "purposeGuidance": "Synthetic formal candidate", "qaBasis": basis,
			"environmentId": "local", "threadId": "thread-" + slug, "workspace": "C:/" + slug,
			"createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug,
		})
		require.NoError(t, err)
		run, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, human.ID, "prepare-"+slug, body)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "thread-"+slug))
		prepared, err := s.ReadRunContext(ctx, run)
		require.NoError(t, err)
		return run, prepared.PackageID, body, storage.MailScope{EnvironmentID: "local", ThreadID: "thread-" + slug, ProviderSessionID: "session", ProviderInstanceID: "codex"}
	}
	makeSpec("max-one")
	maxRun, maxPackage, maxTarget, maxScope := prepareMore("max-one")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: maxRun, MaxAttempts: 1, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	maxAdmission, err := s.AuthorizeRunDispatch(ctx, maxRun, human.ID, maxPackage, maxTarget)
	require.NoError(t, err)
	maxLoop, err := s.ReadCandidateLoop(ctx, maxRun)
	require.NoError(t, err)
	require.Equal(t, "candidate", maxLoop.Status, "one granted but unsubmitted slot is still live")
	assertSummary(maxRun, maxLoop)
	maxCommit := strings.Repeat("d", 40)
	maxSnapshot := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + maxCommit + `"}}}`)
	maxDelivery, err := s.CreateDelivery(ctx, maxRun, maxSnapshot, human.ID, *maxAdmission.CandidateAttemptID)
	require.NoError(t, err)
	maxLoop, err = s.ReadCandidateLoop(ctx, maxRun)
	require.NoError(t, err)
	require.Equal(t, "waiting", maxLoop.Status, "missing registered report does not exhaust the last ongoing candidate")
	assertSummary(maxRun, maxLoop)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: maxDelivery, CommitSHA: maxCommit, PlanSources: []storage.ReviewSource{plan}, Status: "environment_blocked",
		Summary: "Synthetic environment blocker", OutputRefs: []string{"fixture://blocked"},
	}}, nil)
	require.NoError(t, err)
	maxLoop, err = s.ReadCandidateLoop(ctx, maxRun)
	require.NoError(t, err)
	require.Equal(t, "needs_human", maxLoop.Status)
	assertSummary(maxRun, maxLoop)
	_, err = s.NextOwnCandidateAttempt(operator, maxScope, *maxAdmission.CandidateAttemptID)
	require.ErrorIs(t, err, storage.ErrCandidateAttemptsExhausted)
	stopBody, err := json.Marshal(map[string]string{"runId": maxRun, "reason": "No more successors"})
	require.NoError(t, err)
	stopReply, err := server.ExecuteCandidateLoop(operator, s, "candidate-loop", "candidate-loop-stop", bytes.NewReader(stopBody))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(stopReply, &maxLoop))
	require.Equal(t, "stopped", maxLoop.Status)
	assertSummary(maxRun, maxLoop)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: maxDelivery, CommitSHA: maxCommit, PlanSources: []storage.ReviewSource{plan}, Status: "passed",
		Command: "synthetic assertion", Summary: "Late registered pass", OutputRefs: []string{"fixture://max-pass"},
	}}, nil)
	require.NoError(t, err)
	maxLoop, err = s.ReadCandidateLoop(ctx, maxRun)
	require.NoError(t, err)
	require.Equal(t, "condition_met", maxLoop.Status, "true can finish the current candidate after stop")
	assertSummary(maxRun, maxLoop)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: maxDelivery, CommitSHA: maxCommit, PlanSources: []storage.ReviewSource{additionalPlan}, Status: "passed",
		Command: "synthetic assertion", Summary: "Additional original QA plan passed", OutputRefs: []string{"fixture://max-extra"},
	}}, nil)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "max-one", maxRun))

	makeSpec("candidate-parent")
	makeSpec("scope-implementation")
	_, err = s.AddEdge(ctx, "candidate-parent", "scope-implementation", storage.EdgeTypeComposes)
	require.NoError(t, err)
	scopeRun, scopePackage, scopeTarget, scopeIdentity := prepareMore("scope-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: scopeRun, MaxAttempts: 2, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	scopeAdmission, err := s.AuthorizeRunDispatch(ctx, scopeRun, human.ID, scopePackage, scopeTarget)
	require.NoError(t, err)
	scopeCommit := strings.Repeat("e", 40)
	scopeSnapshot := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + scopeCommit + `"}}}`)
	scopeDelivery, err := s.CreateDelivery(ctx, scopeRun, scopeSnapshot, human.ID, *scopeAdmission.CandidateAttemptID)
	require.NoError(t, err)
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: scopeDelivery, CommitSHA: scopeCommit, PlanSources: []storage.ReviewSource{plan}, Status: "failed",
		Summary: "Synthetic scope candidate failed", OutputRefs: []string{"fixture://scope"},
	}}, nil)
	require.NoError(t, err)
	parentChanged := "Changed inherited scope"
	_, err = s.UpdateSpec(ctx, "candidate-parent", &parentChanged, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.NextOwnCandidateAttempt(operator, scopeIdentity, *scopeAdmission.CandidateAttemptID)
	require.ErrorIs(t, err, storage.ErrCompletionRequiresRequirementReview)
	parentOriginal := "Original candidate-parent"
	_, err = s.UpdateSpec(ctx, "candidate-parent", &parentOriginal, nil, nil, nil, nil)
	require.NoError(t, err)
	scopeRefs, err := s.ReadSpecSourceRefs(ctx, "scope-implementation")
	require.NoError(t, err)
	oneReviewRound := int32(1)
	held, err := s.AssignReview(operator, &storage.AssignReviewRequest{TaskSlug: "scope-implementation", Kind: "requirements",
		Sources:              []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "scope-implementation", Field: "intent", ChangeID: scopeRefs["intent"]}},
		AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer, MaxReviewRounds: &oneReviewRound}, nil)
	require.NoError(t, err)
	reviewScope := storage.MailScope{EnvironmentID: "local", ThreadID: "reviewer-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	heldID := held.Reviews[0].Request.ID
	rejectedReview, err := s.SubmitReview(operator, reviewScope, heldID, "rejected", "Synthetic hold before next candidate")
	require.NoError(t, err)
	require.True(t, rejectedReview.Status.Reviews[0].HumanHold)
	_, err = s.NextOwnCandidateAttempt(operator, scopeIdentity, *scopeAdmission.CandidateAttemptID)
	require.ErrorIs(t, err, storage.ErrReviewHumanHold)
	_, err = s.ReviewSource(operator, "scope-implementation", heldID, "accepted", "Human resolves the hold on this exact source")
	require.NoError(t, err)
	scopeNext, err := s.NextOwnCandidateAttempt(operator, scopeIdentity, *scopeAdmission.CandidateAttemptID)
	require.NoError(t, err, "restored content ignores revision-only drift")
	require.Equal(t, 2, scopeNext.GrantedAttempt.Number)
	abandonBody, err := json.Marshal(map[string]string{"runId": scopeRun, "reason": "Explicitly end this unresolved budget"})
	require.NoError(t, err)
	abandonReply, err := server.ExecuteCandidateLoop(operator, s, "candidate-loop", "candidate-loop-abandon", bytes.NewReader(abandonBody))
	require.NoError(t, err)
	var abandonedLoop *storage.CandidateLoop
	require.NoError(t, json.Unmarshal(abandonReply, &abandonedLoop))
	require.Equal(t, "abandoned", abandonedLoop.Status)
	assertSummary(scopeRun, abandonedLoop)
	_, err = s.NextOwnCandidateAttempt(operator, scopeIdentity, scopeNext.GrantedAttempt.ID)
	require.ErrorIs(t, err, storage.ErrCandidateLoopStopped)
	require.ErrorIs(t, s.RecordCompletion(ctx, "scope-implementation", scopeRun), storage.ErrCandidateLoopStopped)
	scopeDispatch, err := s.ReadRunDispatch(ctx, scopeRun)
	require.NoError(t, err)
	require.NotNil(t, scopeDispatch.Admission)
	require.Nil(t, scopeDispatch.Resolution, "abandon is not responsibility release")
	require.NoError(t, s.ResolveRunDispatch(ctx, scopeRun, scopeAdmission.ID, human.ID, "stopped_writing", "Original writer explicitly stopped"))
	replacementTarget, err := json.Marshal(map[string]any{
		"workPurpose": "implementation", "purposeGuidance": "Replacement after explicit abandonment and release", "qaBasis": basis,
		"environmentId": "local", "threadId": "replacement-thread", "workspace": "C:/scope-replacement",
		"createCommandId": "create-replacement", "startCommandId": "start-replacement", "messageId": "message-replacement",
	})
	require.NoError(t, err)
	replacement, _, err := s.PrepareRunForOperator(ctx, "scope-implementation", "C:/scope-replacement", human.ID, "prepare-scope-replacement", replacementTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, replacement, "local", "replacement-thread"))
	replacementContext, err := s.ReadRunContext(ctx, replacement)
	require.NoError(t, err)
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: replacement, MaxAttempts: 1, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err, "named abandonment plus original responsibility release frees the task budget")
	_, err = s.AuthorizeRunDispatch(ctx, replacement, human.ID, replacementContext.PackageID, replacementTarget)
	require.NoError(t, err)

	makeSpec("self-implementation")
	selfRun, selfPackage, selfTarget, selfScope := prepareMore("self-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: selfRun, MaxAttempts: 1, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	selfAdmission, err := s.AuthorizeRunDispatch(ctx, selfRun, human.ID, selfPackage, selfTarget)
	require.NoError(t, err)
	selfContext, err := s.ReadOwnDeliveryContext(operator, selfScope)
	require.NoError(t, err)
	require.Equal(t, *selfAdmission.CandidateAttemptID, *selfContext.CurrentAttemptID)
	selfCommit := strings.Repeat("f", 40)
	selfHead := storage.DeliveryHead{IsRepo: true, CommitSHA: &selfCommit}
	_, err = s.SubmitOwnDelivery(operator, selfScope, selfRun, "", "Formal self candidate", selfHead)
	require.ErrorIs(t, err, storage.ErrCandidateLoopConflict, "self submission must carry the explicit current attempt")
	selfReceipt, err := s.SubmitOwnDelivery(operator, selfScope, selfRun, *selfContext.CurrentAttemptID, "Formal self candidate", selfHead)
	require.NoError(t, err)
	require.Equal(t, *selfContext.CurrentAttemptID, *selfReceipt.AttemptID)
	selfReplay, err := s.SubmitOwnDelivery(operator, selfScope, selfRun, *selfContext.CurrentAttemptID, "Formal self candidate", selfHead)
	require.NoError(t, err)
	require.Equal(t, selfReceipt.DeliveryID, selfReplay.DeliveryID)
	_, err = s.SubmitOwnDelivery(operator, selfScope, selfRun, *selfContext.CurrentAttemptID, "Different formal candidate", selfHead)
	require.ErrorIs(t, err, storage.ErrCandidateLoopConflict)

	makeSpec("preflight-parent")
	makeSpec("preflight-implementation")
	_, err = s.AddEdge(ctx, "preflight-parent", "preflight-implementation", storage.EdgeTypeComposes)
	require.NoError(t, err)
	preflightRun, preflightPackage, preflightTarget, _ := prepareMore("preflight-implementation")
	changedAncestor := "Changed preflight ancestor"
	_, err = s.UpdateSpec(ctx, "preflight-parent", &changedAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: preflightRun, MaxAttempts: 1, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.ErrorIs(t, err, storage.ErrCompletionRequiresRequirementReview, "configuration cannot freeze a stale inherited scope")
	originalAncestor := "Original preflight-parent"
	_, err = s.UpdateSpec(ctx, "preflight-parent", &originalAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: preflightRun, MaxAttempts: 1, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	_, err = s.UpdateSpec(ctx, "preflight-parent", &changedAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, preflightRun, human.ID, preflightPackage, preflightTarget)
	require.ErrorIs(t, err, storage.ErrCompletionRequiresRequirementReview, "first grant rechecks the original scope")
	_, err = s.UpdateSpec(ctx, "preflight-parent", &originalAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	preflightAdmission, err := s.AuthorizeRunDispatch(ctx, preflightRun, human.ID, preflightPackage, preflightTarget)
	require.NoError(t, err)
	preflightCommit := strings.Repeat("a", 40)
	preflightSnapshot := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + preflightCommit + `"}}}`)
	preflightDelivery, err := s.CreateDelivery(ctx, preflightRun, preflightSnapshot, human.ID, *preflightAdmission.CandidateAttemptID)
	require.NoError(t, err)
	for _, assignedPlan := range []storage.ReviewSource{plan, additionalPlan} {
		_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
			DeliveryID: preflightDelivery, CommitSHA: preflightCommit, PlanSources: []storage.ReviewSource{assignedPlan}, Status: "passed",
			Command: "synthetic assertion", Summary: "Preflight verified plan", OutputRefs: []string{"fixture://preflight"},
		}}, nil)
		require.NoError(t, err)
	}
	_, err = s.UpdateSpec(ctx, "preflight-parent", &changedAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "preflight-implementation", preflightRun), storage.ErrCompletionRequiresRequirementReview,
		"formal pass does not override changed inherited requirements at completion")
	_, err = s.UpdateSpec(ctx, "preflight-parent", &originalAncestor, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "preflight-implementation", preflightRun))

	// Two original test_execution runs report on fixed commits; no test command is executed here.
	makeSpec("external-implementation")
	externalRun, externalPackage, externalTarget, externalScope := prepareMore("external-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: externalRun, MaxAttempts: 2, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "report"}, nil)
	require.NoError(t, err)
	externalAdmission, err := s.AuthorizeRunDispatch(ctx, externalRun, human.ID, externalPackage, externalTarget)
	require.NoError(t, err)
	candidateOneCommit := strings.Repeat("1", 40)
	candidateTwoCommit := strings.Repeat("2", 40)
	candidateSnapshot := func(commit string) json.RawMessage {
		return json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + commit + `"}}}`)
	}
	candidateOneDelivery, err := s.CreateDelivery(ctx, externalRun, candidateSnapshot(candidateOneCommit), human.ID, *externalAdmission.CandidateAttemptID)
	require.NoError(t, err)
	makeTestRun := func(slug, commit string) storage.MailScope {
		t.Helper()
		makeSpec(slug)
		testBasis := storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{},
			DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{plan, additionalPlan}, Applicability: "Synthetic fixed external verification"}
		testTarget, err := json.Marshal(map[string]any{
			"workPurpose": "test_execution", "purposeGuidance": "Verify only the fixed candidate commit", "qaBasis": testBasis,
			"environmentId": "local", "threadId": "thread-" + slug, "workspace": "C:/" + slug,
			"createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug,
			"gitBaseline": map[string]any{"isRepo": true, "commitSha": commit},
		})
		require.NoError(t, err)
		testRun, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, human.ID, "prepare-"+slug, testTarget)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, testRun, "local", "thread-"+slug))
		testContext, err := s.ReadRunContext(ctx, testRun)
		require.NoError(t, err)
		_, err = s.AuthorizeRunDispatch(ctx, testRun, human.ID, testContext.PackageID, testTarget)
		require.NoError(t, err)
		return storage.MailScope{EnvironmentID: "local", ThreadID: "thread-" + slug, ProviderSessionID: "session", ProviderInstanceID: "codex"}
	}
	reportFromTest := func(scope storage.MailScope, deliveryID, commit, status string, assignedPlan storage.ReviewSource) (*storage.TestReport, error) {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"scope": scope, "deliveryId": deliveryID, "commitSha": commit, "planSources": []storage.ReviewSource{assignedPlan},
			"status": status, "command": "synthetic registered assertion", "summary": "Synthetic external validation assertion",
			"outputRefs": []string{"fixture://external-report"},
		})
		require.NoError(t, err)
		return server.ExecuteAgentTestReport(operator, s, "candidate-loop", bytes.NewReader(body))
	}
	testOneScope := makeTestRun("test-one", candidateOneCommit)
	failedExternal, err := reportFromTest(testOneScope, candidateOneDelivery, candidateOneCommit, "failed", plan)
	require.NoError(t, err)
	require.Equal(t, candidateOneDelivery, failedExternal.DeliveryID)
	externalNext, err := s.NextOwnCandidateAttempt(operator, externalScope, *externalAdmission.CandidateAttemptID)
	require.NoError(t, err, "the original implementation MailScope consumes the external failed report")
	require.Equal(t, "false", externalNext.GrantedAttempt.ConsumedCondition.Value)
	require.Equal(t, failedExternal.ID, externalNext.GrantedAttempt.ConsumedCondition.Reports[0].ID)
	candidateTwoDelivery, err := s.CreateDelivery(ctx, externalRun, candidateSnapshot(candidateTwoCommit), human.ID, externalNext.GrantedAttempt.ID)
	require.NoError(t, err)
	_, err = reportFromTest(testOneScope, candidateTwoDelivery, candidateTwoCommit, "passed", plan)
	require.ErrorIs(t, err, storage.ErrInvalidTestReport, "T1's fixed C1 baseline cannot assert C2")
	noReports, err := s.ReadTestReports(ctx, candidateTwoDelivery, "")
	require.NoError(t, err)
	require.Empty(t, noReports.Reports)
	testTwoScope := makeTestRun("test-two", candidateTwoCommit)
	for _, assignedPlan := range []storage.ReviewSource{plan, additionalPlan} {
		passedExternal, err := reportFromTest(testTwoScope, candidateTwoDelivery, candidateTwoCommit, "passed", assignedPlan)
		require.NoError(t, err)
		require.Equal(t, candidateTwoDelivery, passedExternal.DeliveryID)
	}
	externalLoop, err := s.ReadCandidateLoop(ctx, externalRun)
	require.NoError(t, err)
	require.Equal(t, "condition_met", externalLoop.Status)
	require.NoError(t, s.RecordCompletion(ctx, "external-implementation", externalRun))

	makeBranchRun := func(slug string) (string, string, json.RawMessage) {
		t.Helper()
		makeSpec(slug)
		body, err := json.Marshal(map[string]any{
			"workPurpose": "coordination", "purposeGuidance": "Complete the fixed branch work", "environmentId": "local",
			"threadId": "thread-" + slug, "workspace": "C:/" + slug,
			"createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug,
		})
		require.NoError(t, err)
		branchRun, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, human.ID, "prepare-"+slug, body)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, branchRun, "local", "thread-"+slug))
		branchContext, err := s.ReadRunContext(ctx, branchRun)
		require.NoError(t, err)
		return branchRun, branchContext.PackageID, body
	}
	makeSpec("joined-implementation")
	joinedRun, joinedPackage, joinedTarget, joinedScope := prepareMore("joined-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: joinedRun, MaxAttempts: 2, TerminationKind: "report",
		PlanSources: []storage.ReviewSource{plan}, Join: &storage.CandidateJoinConfig{Mode: "all"}}, nil)
	require.NoError(t, err)
	joinedAdmission, err := s.AuthorizeRunDispatch(ctx, joinedRun, human.ID, joinedPackage, joinedTarget)
	require.NoError(t, err)
	joinedFirstID := *joinedAdmission.CandidateAttemptID
	joinedDeliveryOne, err := s.CreateDelivery(ctx, joinedRun, candidateSnapshot(candidateOneCommit), human.ID, joinedFirstID)
	require.NoError(t, err)
	_, err = reportFromTest(testOneScope, joinedDeliveryOne, candidateOneCommit, "failed", plan)
	require.NoError(t, err)
	joinedCurrent, err := s.ReadCandidateLoop(ctx, joinedRun)
	require.NoError(t, err)
	require.Equal(t, "ready_next", joinedCurrent.Status)
	require.Equal(t, "flow_missing", joinedCurrent.Join.Reason)
	_, err = s.NextOwnCandidateAttempt(operator, joinedScope, joinedFirstID)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	joinedBranchOne, joinedBranchOnePackage, joinedBranchOneTarget := makeBranchRun("joined-branch-one")
	decorative := storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "joined-decorative", CandidateAttemptID: joinedFirstID,
		Conditions: []storage.ArmReportBranchCondition{
			reportFlowCondition("formal", joinedDeliveryOne, plan),
			reportFlowCondition("other", candidateTwoDelivery, plan),
		},
		Branches: []storage.ArmReportBranchMember{{RunID: joinedBranchOne, ConditionKey: "other", When: "true"}},
	}
	_, err = s.ArmReportBranchFlow(operator, decorative, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchFlow, "the formal delivery condition cannot be decorative")
	decorative.IdempotencyKey = "joined-cross-attempt"
	decorative.CandidateAttemptID = *externalAdmission.CandidateAttemptID
	decorative.Branches[0].ConditionKey = "formal"
	_, err = s.ArmReportBranchFlow(operator, decorative, nil)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowConflict, "completed or stale attempt cannot receive a new flow")
	firstFlowRequest := storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "joined-first", CandidateAttemptID: joinedFirstID,
		Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("formal", joinedDeliveryOne, plan)},
		Branches:   []storage.ArmReportBranchMember{{RunID: joinedBranchOne, ConditionKey: "formal", When: "false"}},
	}
	firstFlow, err := s.ArmReportBranchFlow(operator, firstFlowRequest, nil)
	require.NoError(t, err)
	require.Equal(t, joinedFirstID, *firstFlow.CandidateAttemptID)
	_, err = s.NextOwnCandidateAttempt(operator, joinedScope, joinedFirstID)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied, "selected but incomplete branch blocks all")
	_, err = s.AuthorizeRunDispatch(ctx, joinedBranchOne, human.ID, joinedBranchOnePackage, joinedBranchOneTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "joined-branch-one", joinedBranchOne))
	joinedNext, err := s.NextOwnCandidateAttempt(operator, joinedScope, joinedFirstID)
	require.NoError(t, err)
	require.Equal(t, 2, joinedNext.GrantedAttempt.Number)
	require.Equal(t, joinedFirstID, joinedNext.GrantedAttempt.ConsumedJoin.SourceAttemptID)
	require.Equal(t, firstFlow.ID, joinedNext.GrantedAttempt.ConsumedJoin.FlowID)
	require.True(t, joinedNext.GrantedAttempt.ConsumedJoin.Evaluation.Satisfied)
	joinedSecondID := joinedNext.GrantedAttempt.ID
	joinedReplay, err := s.NextOwnCandidateAttempt(operator, joinedScope, joinedFirstID)
	require.NoError(t, err)
	require.True(t, joinedReplay.Replayed)
	require.Equal(t, firstFlow.ID, joinedReplay.GrantedAttempt.ConsumedJoin.FlowID)
	firstFlowReplay, err := s.ArmReportBranchFlow(operator, firstFlowRequest, nil)
	require.NoError(t, err, "the original idempotency key recovers an old flow without rebinding it")
	require.Equal(t, firstFlow.ID, firstFlowReplay.ID)
	joinedDeliveryTwo, err := s.CreateDelivery(ctx, joinedRun, candidateSnapshot(candidateTwoCommit), human.ID, joinedSecondID)
	require.NoError(t, err)
	for _, assignedPlan := range []storage.ReviewSource{plan, additionalPlan} {
		_, err = reportFromTest(testTwoScope, joinedDeliveryTwo, candidateTwoCommit, "passed", assignedPlan)
		require.NoError(t, err)
	}
	joinedCurrent, err = s.ReadCandidateLoop(ctx, joinedRun)
	require.NoError(t, err)
	require.Equal(t, "condition_met", joinedCurrent.Status)
	require.Equal(t, "flow_missing", joinedCurrent.Join.Reason)
	require.ErrorIs(t, s.RecordCompletion(ctx, "joined-implementation", joinedRun), storage.ErrReportFlowJoinUnsatisfied)
	joinedBranchTwo, joinedBranchTwoPackage, joinedBranchTwoTarget := makeBranchRun("joined-branch-two")
	secondFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "joined-second", CandidateAttemptID: joinedSecondID,
		Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("formal", joinedDeliveryTwo, plan)},
		Branches:   []storage.ArmReportBranchMember{{RunID: joinedBranchTwo, ConditionKey: "formal", When: "true"}},
	}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "joined-implementation", joinedRun), storage.ErrReportFlowJoinUnsatisfied)
	_, err = s.AuthorizeRunDispatch(ctx, joinedBranchTwo, human.ID, joinedBranchTwoPackage, joinedBranchTwoTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "joined-branch-two", joinedBranchTwo))
	require.NoError(t, s.RecordCompletion(ctx, "joined-implementation", joinedRun))
	joinedDone, err := s.ReadCandidateLoop(ctx, joinedRun)
	require.NoError(t, err)
	require.Equal(t, "completed", joinedDone.Status)
	require.Equal(t, secondFlow.ID, joinedDone.CompletedJoin.FlowID)
	require.Equal(t, joinedSecondID, joinedDone.CompletedJoin.SourceAttemptID)
	_, err = s.CancelReportBranchFlow(operator, secondFlow.ID, "Historical completion is not revoked", nil)
	require.NoError(t, err)
	joinedDone, err = s.ReadCandidateLoop(ctx, joinedRun)
	require.NoError(t, err)
	require.Equal(t, "completed", joinedDone.Status)
	require.Equal(t, "flow_cancelled", joinedDone.Join.Reason)
	require.Equal(t, secondFlow.ID, joinedDone.CompletedJoin.FlowID)

	makeSpec("cancel-implementation")
	cancelRun, cancelPackage, cancelTarget, cancelScope := prepareMore("cancel-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: cancelRun, MaxAttempts: 2, TerminationKind: "report",
		PlanSources: []storage.ReviewSource{plan}, Join: &storage.CandidateJoinConfig{Mode: "all"}}, nil)
	require.NoError(t, err)
	cancelAdmission, err := s.AuthorizeRunDispatch(ctx, cancelRun, human.ID, cancelPackage, cancelTarget)
	require.NoError(t, err)
	cancelAttemptID := *cancelAdmission.CandidateAttemptID
	cancelDelivery, err := s.CreateDelivery(ctx, cancelRun, candidateSnapshot(candidateOneCommit), human.ID, cancelAttemptID)
	require.NoError(t, err)
	_, err = reportFromTest(testOneScope, cancelDelivery, candidateOneCommit, "failed", plan)
	require.NoError(t, err)
	cancelBranch, _, _ := makeBranchRun("cancel-branch")
	cancelFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "cancel-candidate-flow", CandidateAttemptID: cancelAttemptID,
		Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("formal", cancelDelivery, plan)},
		Branches:   []storage.ArmReportBranchMember{{RunID: cancelBranch, ConditionKey: "formal", When: "false"}},
	}, nil)
	require.NoError(t, err)
	_, err = s.CancelReportBranchFlow(operator, cancelFlow.ID, "Withdraw this candidate join before admission", nil)
	require.NoError(t, err)
	_, err = s.NextOwnCandidateAttempt(operator, cancelScope, cancelAttemptID)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	cancelState, err := s.ReadCandidateLoop(ctx, cancelRun)
	require.NoError(t, err)
	require.Len(t, cancelState.Attempts, 1, "a cancelled join cannot spend the next attempt")
	require.Equal(t, "flow_cancelled", cancelState.Join.Reason)
	for _, assignedPlan := range []storage.ReviewSource{plan, additionalPlan} {
		_, err = reportFromTest(testOneScope, cancelDelivery, candidateOneCommit, "passed", assignedPlan)
		require.NoError(t, err)
	}
	require.ErrorIs(t, s.RecordCompletion(ctx, "cancel-implementation", cancelRun), storage.ErrReportFlowJoinUnsatisfied)

	makeSpec("judgment-implementation")
	judgedRun, judgedPackage, judgedTarget, _ := prepareMore("judgment-implementation")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: judgedRun, MaxAttempts: 1, TerminationKind: "report",
		PlanSources: []storage.ReviewSource{plan}, Join: &storage.CandidateJoinConfig{Mode: "all"}}, nil)
	require.NoError(t, err)
	judgedAdmission, err := s.AuthorizeRunDispatch(ctx, judgedRun, human.ID, judgedPackage, judgedTarget)
	require.NoError(t, err)
	judgedDelivery, err := s.CreateDelivery(ctx, judgedRun, candidateSnapshot(candidateOneCommit), human.ID, *judgedAdmission.CandidateAttemptID)
	require.NoError(t, err)
	for _, assignedPlan := range []storage.ReviewSource{plan, additionalPlan} {
		_, err = reportFromTest(testOneScope, judgedDelivery, candidateOneCommit, "passed", assignedPlan)
		require.NoError(t, err)
	}
	judgedBranchRun, judgedBranchPackage, judgedBranchTarget := makeBranchRun("candidate-judgment-branch")
	judgedCondition := storage.ArmReportBranchCondition{Key: "formal", Kind: "judgment", Judgment: &storage.ReportBranchJudgmentInput{
		Criterion: "Is this formal candidate acceptable?", InputSources: []storage.ReviewSource{plan}, DeliveryID: &judgedDelivery,
	}}
	badJudgedFlow := storage.ArmReportBranchFlowRequest{IdempotencyKey: "judged-no-delivery", CandidateAttemptID: *judgedAdmission.CandidateAttemptID,
		Conditions: []storage.ArmReportBranchCondition{{Key: "formal", Kind: "judgment", Judgment: &storage.ReportBranchJudgmentInput{
			Criterion: judgedCondition.Judgment.Criterion, InputSources: []storage.ReviewSource{plan},
		}}}, Branches: []storage.ArmReportBranchMember{{RunID: judgedBranchRun, ConditionKey: "formal", When: "true"}}}
	_, err = s.ArmReportBranchFlow(operator, badJudgedFlow, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchFlow, "unbound judgment cannot decorate a candidate attempt")
	badJudgedFlow.IdempotencyKey = "judged-empty-sources"
	badJudgedFlow.Conditions[0].Judgment.DeliveryID = &judgedDelivery
	badJudgedFlow.Conditions[0].Judgment.InputSources = []storage.ReviewSource{}
	_, err = s.ArmReportBranchFlow(operator, badJudgedFlow, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchFlow, "empty judgment sources cannot anchor the specified candidate plan")
	judgedFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "judged-formal", CandidateAttemptID: *judgedAdmission.CandidateAttemptID,
		Conditions: []storage.ArmReportBranchCondition{judgedCondition},
		Branches:   []storage.ArmReportBranchMember{{RunID: judgedBranchRun, ConditionKey: "formal", When: "true"}},
	}, nil)
	require.NoError(t, err)
	judgedFact, err := s.RecordReportBranchJudgment(operator, storage.RecordReportBranchJudgmentRequest{
		FlowID: judgedFlow.ID, ConditionKey: "formal", Value: "true", InputRefs: []storage.ReviewSource{plan},
		Reason: "Human evaluated this exact formal candidate", ExpectedJudgmentID: nil,
	}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "judgment-implementation", judgedRun), storage.ErrReportFlowJoinUnsatisfied)
	_, err = s.AuthorizeRunDispatch(ctx, judgedBranchRun, human.ID, judgedBranchPackage, judgedBranchTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "candidate-judgment-branch", judgedBranchRun))
	require.NoError(t, s.RecordCompletion(ctx, "judgment-implementation", judgedRun))
	judgedDone, err := s.ReadCandidateLoop(ctx, judgedRun)
	require.NoError(t, err)
	require.Equal(t, "completed", judgedDone.Status)
	require.Equal(t, judgedFact.Judgment.ID, judgedDone.CompletedJoin.Evaluation.Participants[0].Basis.Judgment.ID)
	require.Equal(t, plan, judgedDone.CompletedJoin.Evaluation.Participants[0].Basis.Judgment.InputRefs[0])
	makeSpec("replacement-implementation")
	_, _, err = s.LifecycleSupersedeSpec(ctx, "implementation", "replacement-implementation", "Replace the completed original implementation")
	require.NoError(t, err)
	replayedAfterSupersede, err := s.CreateDelivery(ctx, run, snapshot1, human.ID, firstID)
	require.NoError(t, err, "a fixed historical candidate replay keeps its exact original receipt")
	require.Equal(t, delivery1, replayedAfterSupersede)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", run), storage.ErrSpecTerminal)

	// Satisfaction judgments are independent of test reports. Either producer can
	// create a durable contradiction; later aligned facts require named human resolution.
	makeSpec("satisfaction-implementation")
	satisfactionBasis := basis
	satisfactionBasis.TestPlanSources = []storage.ReviewSource{plan}
	satisfactionTarget, err := json.Marshal(map[string]any{
		"workPurpose": "implementation", "purposeGuidance": "Produce formal candidates with a fixed satisfaction criterion.",
		"environmentId": "local", "threadId": "satisfaction-thread", "workspace": "C:/satisfaction-impl",
		"createCommandId": "create-satisfaction", "startCommandId": "start-satisfaction", "messageId": "message-satisfaction", "qaBasis": satisfactionBasis,
	})
	require.NoError(t, err)
	satisfactionRun, _, err := s.PrepareRunForOperator(ctx, "satisfaction-implementation", "C:/satisfaction-impl", human.ID, "prepare-satisfaction", satisfactionTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, satisfactionRun, "local", "satisfaction-thread"))
	satisfactionContext, err := s.ReadRunContext(ctx, satisfactionRun)
	require.NoError(t, err)
	criterion := "The implementation satisfies the named behavior, not just its test command"
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: satisfactionRun, MaxAttempts: 2, PlanSources: []storage.ReviewSource{plan}, TerminationKind: "satisfaction", SatisfactionCriterion: &criterion}, nil)
	require.NoError(t, err)
	satisfactionAdmission, err := s.AuthorizeRunDispatch(ctx, satisfactionRun, human.ID, satisfactionContext.PackageID, satisfactionTarget)
	require.NoError(t, err)
	firstSatisfactionID := *satisfactionAdmission.CandidateAttemptID
	satisfactionScope := storage.MailScope{EnvironmentID: "local", ThreadID: "satisfaction-thread", ProviderSessionID: "satisfaction-session", ProviderInstanceID: "codex"}
	firstCommit := strings.Repeat("d", 40)
	firstDelivery, err := s.CreateDelivery(ctx, satisfactionRun, candidateSnapshot(firstCommit), human.ID, firstSatisfactionID)
	require.NoError(t, err)
	recordSatisfaction := func(attemptID string, predecessor *string, value string) *storage.CandidateSatisfactionJudgmentResult {
		t.Helper()
		fact, recordErr := s.RecordOwnCandidateSatisfactionJudgment(operator, satisfactionScope, storage.RecordOwnCandidateSatisfactionJudgmentRequest{
			AttemptID: attemptID, ExpectedJudgmentID: predecessor, Value: value, Reason: "Synthetic named satisfaction judgment",
		})
		require.NoError(t, recordErr)
		return fact
	}
	recordCandidateReport := func(deliveryID, commit, status string) *storage.TestReport {
		t.Helper()
		report, recordErr := s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
			DeliveryID: deliveryID, CommitSHA: commit, PlanSources: []storage.ReviewSource{plan}, Status: status,
			Command: "synthetic assertion", Summary: "Synthetic " + status + " candidate", OutputRefs: []string{"fixture://satisfaction-" + status},
		}}, nil)
		require.NoError(t, recordErr)
		return report
	}
	firstJudgment := recordSatisfaction(firstSatisfactionID, nil, "false")
	recordCandidateReport(firstDelivery, firstCommit, "passed")
	firstHold, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "needs_human", firstHold.Status)
	require.Equal(t, "passed_false", firstHold.Satisfaction.Intervention.ConflictKind)
	assertSummary(satisfactionRun, firstHold)
	secondJudgment := recordSatisfaction(firstSatisfactionID, &firstJudgment.Judgment.ID, "true")
	require.ErrorIs(t, s.RecordCompletion(ctx, "satisfaction-implementation", satisfactionRun), storage.ErrCandidateInterventionHeld)
	thirdJudgment := recordSatisfaction(firstSatisfactionID, &secondJudgment.Judgment.ID, "false")
	firstFailed := recordCandidateReport(firstDelivery, firstCommit, "failed")
	_, err = s.ResolveCandidateIntervention(operator, storage.ResolveCandidateInterventionRequest{RunID: satisfactionRun, AttemptID: firstSatisfactionID,
		InterventionID: firstHold.Satisfaction.Intervention.ID, ExpectedJudgmentID: secondJudgment.Judgment.ID, ExpectedReportIDs: []string{firstFailed.ID}, Reason: "Stale judgment must not clear the hold"})
	require.ErrorIs(t, err, storage.ErrCandidateSatisfactionConflict)
	resolvedFirst, err := s.ResolveCandidateIntervention(operator, storage.ResolveCandidateInterventionRequest{RunID: satisfactionRun, AttemptID: firstSatisfactionID,
		InterventionID: firstHold.Satisfaction.Intervention.ID, ExpectedJudgmentID: thirdJudgment.Judgment.ID, ExpectedReportIDs: []string{firstFailed.ID}, Reason: "Reviewed aligned failed candidate"})
	require.NoError(t, err)
	require.NotNil(t, resolvedFirst.ResolvedAt)
	ready, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "ready_next", ready.Status)
	assertSummary(satisfactionRun, ready)
	nextSatisfaction, err := s.NextOwnCandidateAttempt(operator, satisfactionScope, firstSatisfactionID)
	require.NoError(t, err)
	require.Equal(t, thirdJudgment.Judgment.ID, nextSatisfaction.GrantedAttempt.ConsumedSatisfaction.JudgmentID)
	require.Equal(t, []string{firstFailed.ID}, nextSatisfaction.GrantedAttempt.ConsumedSatisfaction.ReportIDs)
	require.Equal(t, &resolvedFirst.ID, nextSatisfaction.GrantedAttempt.ConsumedSatisfaction.InterventionID)
	firstReplay := recordSatisfaction(firstSatisfactionID, nil, "false")
	require.True(t, firstReplay.Replayed)
	require.Equal(t, firstJudgment.Judgment.ID, firstReplay.Judgment.ID)
	_, err = s.RecordOwnCandidateSatisfactionJudgment(operator, satisfactionScope, storage.RecordOwnCandidateSatisfactionJudgmentRequest{
		AttemptID: firstSatisfactionID, ExpectedJudgmentID: &thirdJudgment.Judgment.ID, Value: "true", Reason: "Late old attempt",
	})
	require.ErrorIs(t, err, storage.ErrCandidateSatisfactionConflict)

	secondSatisfactionID := nextSatisfaction.GrantedAttempt.ID
	secondCommit := strings.Repeat("e", 40)
	secondDelivery, err := s.CreateDelivery(ctx, satisfactionRun, candidateSnapshot(secondCommit), human.ID, secondSatisfactionID)
	require.NoError(t, err)
	recordCandidateReport(secondDelivery, secondCommit, "failed")
	finalJudgment := recordSatisfaction(secondSatisfactionID, nil, "true")
	secondHold, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "failed_true", secondHold.Satisfaction.Intervention.ConflictKind)
	require.Equal(t, "needs_human", secondHold.Status)
	secondPassed := recordCandidateReport(secondDelivery, secondCommit, "passed")
	require.ErrorIs(t, s.RecordCompletion(ctx, "satisfaction-implementation", satisfactionRun), storage.ErrCandidateInterventionHeld)
	_, err = s.ResolveCandidateIntervention(operator, storage.ResolveCandidateInterventionRequest{RunID: satisfactionRun, AttemptID: secondSatisfactionID,
		InterventionID: secondHold.Satisfaction.Intervention.ID, ExpectedJudgmentID: finalJudgment.Judgment.ID, ExpectedReportIDs: []string{secondPassed.ID}, Reason: "Reviewed aligned passed candidate"})
	require.NoError(t, err)
	recordCandidateReport(secondDelivery, secondCommit, "failed")
	repeatedHold, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "needs_human", repeatedHold.Status)
	require.NotEqual(t, secondHold.Satisfaction.Intervention.ID, repeatedHold.Satisfaction.Intervention.ID)
	finalPassed := recordCandidateReport(secondDelivery, secondCommit, "passed")
	_, err = s.ResolveCandidateIntervention(operator, storage.ResolveCandidateInterventionRequest{RunID: satisfactionRun, AttemptID: secondSatisfactionID,
		InterventionID: repeatedHold.Satisfaction.Intervention.ID, ExpectedJudgmentID: finalJudgment.Judgment.ID, ExpectedReportIDs: []string{finalPassed.ID}, Reason: "Reviewed later aligned passed candidate"})
	require.NoError(t, err)
	conditionMet, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "condition_met", conditionMet.Status)
	require.NoError(t, s.RecordCompletion(ctx, "satisfaction-implementation", satisfactionRun))
	completed, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "completed", completed.Status)
	require.Equal(t, finalJudgment.Judgment.ID, completed.CompletedSatisfaction.JudgmentID)
	require.Equal(t, []string{finalPassed.ID}, completed.CompletedSatisfaction.ReportIDs)
	assertSummary(satisfactionRun, completed)
	lateJudgment := recordSatisfaction(secondSatisfactionID, &finalJudgment.Judgment.ID, "false")
	require.False(t, lateJudgment.Replayed)
	completedAfterCorrection, err := s.ReadCandidateLoop(ctx, satisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "completed", completedAfterCorrection.Status)
	require.Equal(t, completed.CompletedSatisfaction, completedAfterCorrection.CompletedSatisfaction)
	assertSummary(satisfactionRun, completedAfterCorrection)
	var completionCount int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion'`,
		"candidate-loop", "satisfaction-implementation", satisfactionRun).Scan(&completionCount))
	require.Equal(t, 1, completionCount)
	require.ErrorIs(t, s.RecordCompletion(ctx, "satisfaction-implementation", satisfactionRun), storage.ErrAgentNotClaimOwner)
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion'`,
		"candidate-loop", "satisfaction-implementation", satisfactionRun).Scan(&completionCount))
	require.Equal(t, 1, completionCount)
	history, err := s.ReadCandidateSatisfactionHistory(ctx, satisfactionRun, firstSatisfactionID, "", "")
	require.NoError(t, err)
	require.Len(t, history.Judgments, 3)
	require.Len(t, history.Interventions, 1)
	secondHistory, err := s.ReadCandidateSatisfactionHistory(ctx, satisfactionRun, secondSatisfactionID, "", "")
	require.NoError(t, err)
	require.Len(t, secondHistory.Judgments, 2, "the correction remains an auditable fact")
	require.Len(t, secondHistory.Interventions, 2, "completed work has no pending intervention consumer")

	makeSpec("satisfaction-joined")
	satJoinedRun, satJoinedPackage, satJoinedTarget, satJoinedScope := prepareMore("satisfaction-joined")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: satJoinedRun, MaxAttempts: 2,
		PlanSources: []storage.ReviewSource{plan, additionalPlan}, TerminationKind: "satisfaction", SatisfactionCriterion: &criterion,
		Join: &storage.CandidateJoinConfig{Mode: "all"}}, nil)
	require.NoError(t, err)
	satJoinedAdmission, err := s.AuthorizeRunDispatch(ctx, satJoinedRun, human.ID, satJoinedPackage, satJoinedTarget)
	require.NoError(t, err)
	satJoinedFirstID := *satJoinedAdmission.CandidateAttemptID
	satJoinedFirstDelivery, err := s.CreateDelivery(ctx, satJoinedRun, candidateSnapshot(candidateOneCommit), human.ID, satJoinedFirstID)
	require.NoError(t, err)
	recordCandidateReport(satJoinedFirstDelivery, candidateOneCommit, "failed")
	satJoinedFalse, err := s.RecordCandidateSatisfactionJudgment(operator, storage.RecordCandidateSatisfactionJudgmentRequest{
		RunID: satJoinedRun, AttemptID: satJoinedFirstID, Value: "false", Reason: "Human reviewed the failed first candidate",
	}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "satisfaction-joined", satJoinedRun), storage.ErrCandidateConditionNotMet)
	_, err = s.NextOwnCandidateAttempt(operator, satJoinedScope, satJoinedFirstID)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	satJoinedBranchOne, satJoinedBranchOnePackage, satJoinedBranchOneTarget := makeBranchRun("satisfaction-joined-branch-one")
	satJoinedFirstFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "satisfaction-joined-first", CandidateAttemptID: satJoinedFirstID,
		Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("formal", satJoinedFirstDelivery, plan)},
		Branches:   []storage.ArmReportBranchMember{{RunID: satJoinedBranchOne, ConditionKey: "formal", When: "false"}},
	}, nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, satJoinedBranchOne, human.ID, satJoinedBranchOnePackage, satJoinedBranchOneTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "satisfaction-joined-branch-one", satJoinedBranchOne))
	satJoinedNext, err := s.NextOwnCandidateAttempt(operator, satJoinedScope, satJoinedFirstID)
	require.NoError(t, err)
	require.Equal(t, satJoinedFalse.Judgment.ID, satJoinedNext.GrantedAttempt.ConsumedSatisfaction.JudgmentID)
	require.Equal(t, satJoinedFirstFlow.ID, satJoinedNext.GrantedAttempt.ConsumedJoin.FlowID)
	satJoinedSecondID := satJoinedNext.GrantedAttempt.ID
	satJoinedSecondDelivery, err := s.CreateDelivery(ctx, satJoinedRun, candidateSnapshot(candidateTwoCommit), human.ID, satJoinedSecondID)
	require.NoError(t, err)
	recordCandidateReport(satJoinedSecondDelivery, candidateTwoCommit, "passed")
	_, err = s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
		DeliveryID: satJoinedSecondDelivery, CommitSHA: candidateTwoCommit, PlanSources: []storage.ReviewSource{additionalPlan}, Status: "passed",
		Command: "synthetic assertion", Summary: "Additional QA passed", OutputRefs: []string{"fixture://satisfaction-joined-extra"},
	}}, nil)
	require.NoError(t, err)
	satJoinedTrue, err := s.RecordCandidateSatisfactionJudgment(operator, storage.RecordCandidateSatisfactionJudgmentRequest{
		RunID: satJoinedRun, AttemptID: satJoinedSecondID, Value: "true", Reason: "Human reviewed the passed second candidate",
	}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "satisfaction-joined", satJoinedRun), storage.ErrReportFlowJoinUnsatisfied)
	satJoinedBranchTwo, satJoinedBranchTwoPackage, satJoinedBranchTwoTarget := makeBranchRun("satisfaction-joined-branch-two")
	satJoinedSecondFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "satisfaction-joined-second", CandidateAttemptID: satJoinedSecondID,
		Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("formal", satJoinedSecondDelivery, plan)},
		Branches:   []storage.ArmReportBranchMember{{RunID: satJoinedBranchTwo, ConditionKey: "formal", When: "true"}},
	}, nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, satJoinedBranchTwo, human.ID, satJoinedBranchTwoPackage, satJoinedBranchTwoTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "satisfaction-joined-branch-two", satJoinedBranchTwo))
	require.NoError(t, s.RecordCompletion(ctx, "satisfaction-joined", satJoinedRun))
	satJoinedDone, err := s.ReadCandidateLoop(ctx, satJoinedRun)
	require.NoError(t, err)
	require.Equal(t, "completed", satJoinedDone.Status)
	require.Equal(t, satJoinedTrue.Judgment.ID, satJoinedDone.CompletedSatisfaction.JudgmentID)
	require.Equal(t, satJoinedSecondFlow.ID, satJoinedDone.CompletedJoin.FlowID)
	assertSummary(satJoinedRun, satJoinedDone)

	makeSpec("satisfaction-max-one")
	maxSatisfactionRun, maxSatisfactionPackage, maxSatisfactionTarget, _ := prepareMore("satisfaction-max-one")
	_, err = s.ArmCandidateLoop(operator, storage.ArmCandidateLoopRequest{RunID: maxSatisfactionRun, MaxAttempts: 1,
		PlanSources: []storage.ReviewSource{plan}, TerminationKind: "satisfaction", SatisfactionCriterion: &criterion}, nil)
	require.NoError(t, err)
	maxSatisfactionAdmission, err := s.AuthorizeRunDispatch(ctx, maxSatisfactionRun, human.ID, maxSatisfactionPackage, maxSatisfactionTarget)
	require.NoError(t, err)
	maxSatisfactionAttempt := *maxSatisfactionAdmission.CandidateAttemptID
	maxSatisfactionCurrent, err := s.ReadCandidateLoop(ctx, maxSatisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "candidate", maxSatisfactionCurrent.Status, "the final allowed attempt is still active before formal delivery")
	maxSatisfactionDelivery, err := s.CreateDelivery(ctx, maxSatisfactionRun, candidateSnapshot(candidateOneCommit), human.ID, maxSatisfactionAttempt)
	require.NoError(t, err)
	recordCandidateReport(maxSatisfactionDelivery, candidateOneCommit, "environment_blocked")
	unknownJudgment, err := s.RecordCandidateSatisfactionJudgment(operator, storage.RecordCandidateSatisfactionJudgmentRequest{
		RunID: maxSatisfactionRun, AttemptID: maxSatisfactionAttempt, Value: "unknown", Reason: "No dependable satisfaction result",
	}, nil)
	require.NoError(t, err)
	maxSatisfactionCurrent, err = s.ReadCandidateLoop(ctx, maxSatisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "needs_human", maxSatisfactionCurrent.Status)
	assertSummary(maxSatisfactionRun, maxSatisfactionCurrent)
	recordCandidateReport(maxSatisfactionDelivery, candidateOneCommit, "passed")
	_, err = s.RecordCandidateSatisfactionJudgment(operator, storage.RecordCandidateSatisfactionJudgmentRequest{
		RunID: maxSatisfactionRun, AttemptID: maxSatisfactionAttempt, ExpectedJudgmentID: &unknownJudgment.Judgment.ID,
		Value: "true", Reason: "Named satisfaction now verified",
	}, nil)
	require.NoError(t, err)
	maxSatisfactionCurrent, err = s.ReadCandidateLoop(ctx, maxSatisfactionRun)
	require.NoError(t, err)
	require.Equal(t, "condition_met", maxSatisfactionCurrent.Status, "aligned true takes precedence over max-attempt exhaustion")
	assertSummary(maxSatisfactionRun, maxSatisfactionCurrent)
}
