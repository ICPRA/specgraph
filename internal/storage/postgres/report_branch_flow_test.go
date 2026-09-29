// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func reportFlowCondition(key, delivery string, plan storage.ReviewSource, inputs ...storage.ReviewSource) storage.ArmReportBranchCondition {
	return storage.ArmReportBranchCondition{Key: key, Kind: "report", Report: &storage.ReportBranchReportInput{
		DeliveryID: delivery, PlanSource: plan, InputSources: append([]storage.ReviewSource{}, inputs...),
	}}
}

// Reports in this test are registered synthetic assertions, not executed commands.
func TestReportBranchFlowConsumesOnlySelectedCurrentCondition(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("report-branches"))
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Flow operator", Role: "admin"}, nil)
	require.NoError(t, err)
	operator := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	makeSpec := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Original "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	plan1, input1 := makeSpec("plan-one"), makeSpec("input-one")
	plan2, input2 := makeSpec("plan-two"), makeSpec("input-two")
	makeSpec("notes-source")
	notesA, notesEmpty := "A", ""
	notesIntentA, notesIntentEmpty := "notes step A", "notes step empty"
	_, err = s.UpdateSpec(ctx, "notes-source", &notesIntentA, nil, nil, nil, &notesA)
	require.NoError(t, err)
	_, err = s.UpdateSpec(ctx, "notes-source", &notesIntentEmpty, nil, nil, nil, &notesEmpty)
	require.NoError(t, err)
	noteChanges, err := s.ListChanges(ctx, "notes-source", storage.ChangeLogFilter{})
	require.NoError(t, err)
	var noteSource storage.ReviewSource
	for _, entry := range noteChanges {
		for _, change := range entry.Changes {
			if change.Field == "notes" && change.OldValue == "A" && change.NewValue == "" {
				noteSource = storage.ReviewSource{Kind: "specgraph", SpecSlug: "notes-source", Field: "notes", ChangeID: entry.ID}
			}
		}
	}
	require.NotEmpty(t, noteSource.ChangeID)
	noteChange, err := s.ReadWorkbenchKnowledgeRecord(ctx, "change", noteSource.ChangeID)
	require.NoError(t, err)
	require.Contains(t, noteChange.Record.(*storage.ChangeLogEntry).Changes, storage.FieldChange{Field: "notes", OldValue: "A", NewValue: ""})
	makeRun := func(slug string) (string, string, json.RawMessage) {
		t.Helper()
		makeSpec(slug)
		target, err := json.Marshal(map[string]any{
			"workPurpose": "coordination", "purposeGuidance": "Synthetic fixed work for flow admission.",
			"environmentId": "local", "threadId": "thread-" + slug, "workspace": "C:/" + slug,
			"createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug,
		})
		require.NoError(t, err)
		run, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, human.ID, "prepare-"+slug, target)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "thread-"+slug))
		pkg, err := s.ReadRunContext(ctx, run)
		require.NoError(t, err)
		return run, pkg.PackageID, target
	}
	implementation1, _, _ := makeRun("implementation-one")
	implementation2, _, _ := makeRun("implementation-two")
	commit := strings.Repeat("b", 40)
	makeDelivery := func(run string, plan storage.ReviewSource) string {
		t.Helper()
		target, err := json.Marshal(map[string]any{
			"workPurpose": "implementation",
			"qaBasis":     storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{plan}, Applicability: "Synthetic plan binding"},
		})
		require.NoError(t, err)
		_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}',$2::jsonb)
 WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, run, target)
		require.NoError(t, err)
		snapshot := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + commit + `"}}}`)
		delivery, err := s.CreateDelivery(ctx, run, snapshot, human.ID)
		require.NoError(t, err)
		return delivery
	}
	delivery1, delivery2 := makeDelivery(implementation1, plan1), makeDelivery(implementation2, plan2)
	trueRun, truePackage, trueTarget := makeRun("one-true")
	notesRun, notesPackage, notesTarget := makeRun("one-notes")
	falseRun, falsePackage, falseTarget := makeRun("one-false")
	otherRun, otherPackage, otherTarget := makeRun("two-true")
	spareRun, sparePackage, spareTarget := makeRun("two-spare")
	hardRun, hardPackage, hardTarget := makeRun("two-hard")
	req := storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "flow-one",
		Conditions: []storage.ArmReportBranchCondition{
			reportFlowCondition("C1", delivery1, plan1, input1, noteSource),
			reportFlowCondition("C2", delivery2, plan2, input2),
		},
		Branches: []storage.ArmReportBranchMember{
			{RunID: trueRun, ConditionKey: "C1", When: "true"},
			{RunID: notesRun, ConditionKey: "C1", When: "true"},
			{RunID: falseRun, ConditionKey: "C1", When: "false"},
			{RunID: otherRun, ConditionKey: "C2", When: "true"},
			{RunID: spareRun, ConditionKey: "C2", When: "true"},
			{RunID: hardRun, ConditionKey: "C2", When: "true"},
		},
	}
	flow, err := s.ArmReportBranchFlow(operator, req, nil)
	require.NoError(t, err)
	branch := func(view *storage.ReportBranchFlow, runID string) storage.ReportBranchMember {
		t.Helper()
		for _, member := range view.Branches {
			if member.RunID == runID {
				return member
			}
		}
		t.Fatalf("missing branch %s", runID)
		return storage.ReportBranchMember{}
	}
	require.Equal(t, "unknown", flow.Conditions[0].Evaluation.Value)
	require.Equal(t, "report_missing", flow.Conditions[0].Evaluation.Reason)
	_, err = s.AuthorizeRunDispatch(ctx, trueRun, human.ID, truePackage, trueTarget)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotSelected, "ordinary authorize must consume the branch condition")
	_, err = s.AuthorizeRunDispatch(ctx, spareRun, human.ID, truePackage, spareTarget)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict, "a branch cannot substitute another prepared package")
	report := func(delivery, status string, plan storage.ReviewSource) *storage.TestReport {
		t.Helper()
		result, err := s.RecordTestReport(operator, &storage.RecordTestReportRequest{TestReportFields: storage.TestReportFields{
			DeliveryID: delivery, CommitSHA: commit, PlanSources: []storage.ReviewSource{plan}, Status: status,
			Command: "synthetic fixture assertion", Summary: "synthetic fixture assertion", OutputRefs: []string{"fixture://declared"},
		}}, nil)
		require.NoError(t, err)
		return result
	}
	passed1 := report(delivery1, "passed", plan1)
	report(delivery2, "passed", plan2)
	current, err := s.ReadReportBranchFlowByRun(ctx, trueRun)
	require.NoError(t, err)
	require.Equal(t, flow.ID, current.ID)
	require.Equal(t, "true", current.Conditions[0].Evaluation.Value)
	require.Equal(t, "selected", branch(current, trueRun).Eligibility)
	require.Equal(t, "unselected", branch(current, falseRun).Eligibility)
	notesB := "B"
	_, err = s.UpdateSpec(ctx, "notes-source", nil, nil, nil, nil, &notesB)
	require.NoError(t, err)
	current, err = s.ReadReportBranchFlow(ctx, flow.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", current.Conditions[0].Evaluation.Value, "empty-string source is still a substantive binding")
	require.Equal(t, "true", current.Conditions[1].Evaluation.Value, "unrelated C2 is unaffected")
	_, err = s.AuthorizeRunDispatch(ctx, notesRun, human.ID, notesPackage, notesTarget)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotSelected)
	_, err = s.UpdateSpec(ctx, "notes-source", nil, nil, nil, nil, &notesEmpty)
	require.NoError(t, err)
	notesAdmission, err := s.AuthorizeRunDispatch(ctx, notesRun, human.ID, notesPackage, notesTarget)
	require.NoError(t, err, "restoring empty content revalidates without rearm")
	require.Equal(t, flow.ID, notesAdmission.ReportFlowBasis.FlowID)
	changed := "Changed input-one"
	_, err = s.UpdateSpec(ctx, "input-one", &changed, nil, nil, nil, nil)
	require.NoError(t, err)
	current, err = s.ReadReportBranchFlow(ctx, flow.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", current.Conditions[0].Evaluation.Value)
	require.Equal(t, "true", current.Conditions[1].Evaluation.Value)
	_, err = s.AuthorizeRunDispatch(ctx, trueRun, human.ID, truePackage, trueTarget)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotSelected)
	otherAdmission, err := s.AuthorizeRunDispatch(ctx, otherRun, human.ID, otherPackage, otherTarget)
	require.NoError(t, err, "C1 drift cannot freeze independently bound C2")
	require.Equal(t, flow.ID, otherAdmission.ReportFlowBasis.FlowID)
	require.Equal(t, "C2", otherAdmission.ReportFlowBasis.ConditionKey)
	original := "Original input-one"
	_, err = s.UpdateSpec(ctx, "input-one", &original, nil, nil, nil, nil)
	require.NoError(t, err)
	priority := "p1"
	_, err = s.UpdateSpec(ctx, "input-one", nil, nil, &priority, nil, nil)
	require.NoError(t, err)
	admission, err := s.AuthorizeRunDispatch(ctx, trueRun, human.ID, truePackage, trueTarget)
	require.NoError(t, err, "same original content restores applicability despite unrelated version/priority changes")
	require.Equal(t, passed1.ID, admission.ReportFlowBasis.Report.ID)
	failed := report(delivery1, "failed", plan1)
	current, err = s.ReadReportBranchFlow(ctx, flow.ID)
	require.NoError(t, err)
	require.Equal(t, "false", current.Conditions[0].Evaluation.Value)
	require.Equal(t, "admitted", branch(current, trueRun).Eligibility)
	require.Equal(t, "selected", branch(current, falseRun).Eligibility)
	falseAdmission, err := s.AuthorizeRunDispatch(ctx, falseRun, human.ID, falsePackage, falseTarget)
	require.NoError(t, err)
	require.Equal(t, failed.ID, falseAdmission.ReportFlowBasis.Report.ID)
	report(delivery1, "environment_blocked", plan1)
	current, err = s.ReadReportBranchFlow(ctx, flow.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", current.Conditions[0].Evaluation.Value)
	require.Equal(t, "environment_blocked", current.Conditions[0].Evaluation.Reason)
	_, err = s.AddEdge(ctx, "two-hard", "plan-one", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, hardRun, human.ID, hardPackage, hardTarget)
	require.ErrorIs(t, err, storage.ErrExecutionDependenciesChanged, "selected branch still obeys the original hard dependencies")
	_, err = s.CancelReportBranchFlow(operator, flow.ID, "Stop only future admissions", nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, spareRun, human.ID, sparePackage, spareTarget)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotSelected)
	replayed, err := s.AuthorizeRunDispatch(ctx, trueRun, human.ID, truePackage, trueTarget)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, admission.ID, replayed.ID)
	firstProof, err := json.Marshal(admission.ReportFlowBasis)
	require.NoError(t, err)
	replayProof, err := json.Marshal(replayed.ReportFlowBasis)
	require.NoError(t, err)
	require.JSONEq(t, string(firstProof), string(replayProof))
	current, err = s.ReadReportBranchFlow(ctx, flow.ID)
	require.NoError(t, err)
	require.Equal(t, "flow_cancelled", current.Conditions[0].Evaluation.Reason)
	require.Equal(t, "admitted", branch(current, trueRun).Eligibility)
	require.Equal(t, "cancelled", branch(current, spareRun).Eligibility)
	view, err := s.ArmReportBranchFlow(operator, req, nil)
	require.NoError(t, err, "idempotent recovery must survive prior admission and cancellation")
	require.Equal(t, flow.ID, view.ID)
	reordered := req
	reordered.Conditions = slices.Clone(req.Conditions)
	reordered.Branches = slices.Clone(req.Branches)
	slices.Reverse(reordered.Conditions)
	slices.Reverse(reordered.Branches)
	view, err = s.ArmReportBranchFlow(operator, reordered, nil)
	require.NoError(t, err, "named definition order cannot change the idempotent occurrence")
	require.Equal(t, flow.ID, view.ID)
	_, err = s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "other-flow", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("C2", delivery2, plan2)},
		Branches: []storage.ArmReportBranchMember{{RunID: spareRun, ConditionKey: "C2", When: "true"}},
	}, nil)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowConflict, "a still-unadmitted run cannot bind another occurrence")
	newDelivery := makeDelivery(implementation1, plan1)
	newRun, _, _ := makeRun("new-delivery-branch")
	newFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "new-delivery-flow", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("new", newDelivery, plan1)},
		Branches: []storage.ArmReportBranchMember{{RunID: newRun, ConditionKey: "new", When: "true"}},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "report_missing", newFlow.Conditions[0].Evaluation.Reason, "old delivery assertions must not feed a new delivery")
	otherProject := newStore(t, postgres.WithProject("report-branches-other"))
	_, err = otherProject.ReadReportBranchFlowByRun(ctx, trueRun)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotFound, "run lookup must be scoped to its project")
	_, err = otherProject.ReadReportBranchFlow(ctx, flow.ID)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotFound)
	_, err = otherProject.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "foreign", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("C2", delivery2, plan2)},
		Branches: []storage.ArmReportBranchMember{{RunID: newRun, ConditionKey: "C2", When: "true"}},
	}, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchFlow, "a foreign delivery cannot bind a local flow")
	alreadyRun, alreadyPackage, alreadyTarget := makeRun("already-admitted")
	_, err = s.AuthorizeRunDispatch(ctx, alreadyRun, human.ID, alreadyPackage, alreadyTarget)
	require.NoError(t, err)
	_, err = s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "already-admitted-flow", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("C2", delivery2, plan2)},
		Branches: []storage.ArmReportBranchMember{{RunID: alreadyRun, ConditionKey: "C2", When: "true"}},
	}, nil)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowConflict)
	req.Conditions[0].Report.InputSources = []storage.ReviewSource{}
	_, err = s.ArmReportBranchFlow(operator, req, nil)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowConflict)

	// A later false selection joins the already admitted true branch; another condition stays unknown.
	report(delivery1, "passed", plan1)
	joinA, joinAPackage, joinATarget := makeRun("join-a")
	joinB, joinBPackage, joinBTarget := makeRun("join-b")
	joinU, joinUPackage, joinUTarget := makeRun("join-unknown")
	joinedFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "joined-flow",
		Conditions: []storage.ArmReportBranchCondition{
			reportFlowCondition("known", delivery1, plan1),
			reportFlowCondition("pending", newDelivery, plan1),
		},
		Branches: []storage.ArmReportBranchMember{
			{RunID: joinA, ConditionKey: "known", When: "true"},
			{RunID: joinB, ConditionKey: "known", When: "false"},
			{RunID: joinU, ConditionKey: "pending", When: "true"},
		},
	}, nil)
	require.NoError(t, err)
	allRun, allPackage, allTarget := makeRun("join-all-target")
	anyRun, anyPackage, anyTarget := makeRun("join-any-target")
	allJoin, err := s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: allRun, Mode: "all"}, nil)
	require.NoError(t, err)
	anyJoin, err := s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: anyRun, Mode: "any"}, nil)
	require.NoError(t, err)
	require.False(t, allJoin.Evaluation.Satisfied, "selected but unadmitted A is not completion")
	require.False(t, anyJoin.Evaluation.Satisfied)
	require.Len(t, allJoin.Evaluation.Participants, 1)
	require.Len(t, allJoin.Evaluation.Unknown, 1)
	joinedView, err := s.ReadReportBranchFlow(ctx, joinedFlow.ID)
	require.NoError(t, err)
	require.Len(t, joinedView.Joins, 2, "flow discovery contains only join configuration")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: joinA, Mode: "all"}, nil)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinConflict, "join target cannot be its own flow member")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: alreadyRun, Mode: "all"}, nil)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinConflict, "already admitted target cannot be rebound")
	_, err = otherProject.ReadReportFlowJoin(ctx, allRun)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinNotFound)
	_, err = s.AuthorizeRunDispatch(ctx, allRun, human.ID, allPackage, allTarget)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	_, err = s.AuthorizeRunDispatch(ctx, anyRun, human.ID, anyPackage, anyTarget)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	_, err = s.AuthorizeRunDispatch(ctx, joinA, human.ID, joinAPackage, joinATarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "join-a", joinA))
	anyJoin, err = s.ReadReportFlowJoin(ctx, anyRun)
	require.NoError(t, err)
	require.True(t, anyJoin.Evaluation.Satisfied, "a valid A witness suffices despite unrelated unknown U")
	require.Equal(t, joinA, anyJoin.Evaluation.Witness.RunID)
	require.Len(t, anyJoin.Evaluation.Unknown, 1)
	anyAdmission, err := s.AuthorizeRunDispatch(ctx, anyRun, human.ID, anyPackage, anyTarget)
	require.NoError(t, err)
	require.Equal(t, joinA, anyAdmission.ReportJoinBasis.Evaluation.Witness.RunID)
	joinedFailed := report(delivery1, "failed", plan1)
	allJoin, err = s.ReadReportFlowJoin(ctx, allRun)
	require.NoError(t, err)
	require.False(t, allJoin.Evaluation.Satisfied)
	require.Len(t, allJoin.Evaluation.Participants, 2, "admitted A remains in P when B becomes selected")
	require.Len(t, allJoin.Evaluation.Unknown, 1)
	_, err = s.AuthorizeRunDispatch(ctx, joinB, human.ID, joinBPackage, joinBTarget)
	require.NoError(t, err, "admitting the any target does not close later selected branches")
	require.NoError(t, s.RecordCompletion(ctx, "join-b", joinB))
	report(newDelivery, "failed", plan1)
	allJoin, err = s.ReadReportFlowJoin(ctx, allRun)
	require.NoError(t, err)
	require.True(t, allJoin.Evaluation.Satisfied)
	require.Empty(t, allJoin.Evaluation.Unknown)
	require.Len(t, allJoin.Evaluation.Participants, 2)
	for _, member := range allJoin.Evaluation.Participants {
		require.NotNil(t, member.Completion)
	}
	allAdmission, err := s.AuthorizeRunDispatch(ctx, allRun, human.ID, allPackage, allTarget)
	require.NoError(t, err)
	require.Len(t, allAdmission.ReportJoinBasis.Evaluation.Participants, 2)
	report(newDelivery, "passed", plan1)
	_, err = s.AuthorizeRunDispatch(ctx, joinU, human.ID, joinUPackage, joinUTarget)
	require.NoError(t, err, "all target admission does not seal the flow")
	replayedAll, err := s.AuthorizeRunDispatch(ctx, allRun, human.ID, allPackage, allTarget)
	require.NoError(t, err)
	require.True(t, replayedAll.Replayed)
	require.Equal(t, allAdmission.ID, replayedAll.ID)
	require.Len(t, replayedAll.ReportJoinBasis.Evaluation.Participants, 2, "replay retains prior proof instead of including later U")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: allRun, Mode: "all"}, nil)
	require.NoError(t, err, "same configuration recovers the original join after admission")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: allRun, Mode: "any"}, nil)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinConflict)

	// Existing hard dependencies and an independent branch gate still compose with the join.
	hardJoinRun, hardJoinPackage, hardJoinTarget := makeRun("join-hard-target")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: hardJoinRun, Mode: "any"}, nil)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, "join-hard-target", "plan-one", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, hardJoinRun, human.ID, hardJoinPackage, hardJoinTarget)
	require.ErrorIs(t, err, storage.ErrExecutionDependenciesChanged)
	combinedRun, combinedPackage, combinedTarget := makeRun("join-combined-target")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: combinedRun, Mode: "any"}, nil)
	require.NoError(t, err)
	_, err = s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "combined-gate", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("gate", delivery2, plan2)},
		Branches: []storage.ArmReportBranchMember{{RunID: combinedRun, ConditionKey: "gate", When: "false"}},
	}, nil)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, combinedRun, human.ID, combinedPackage, combinedTarget)
	require.ErrorIs(t, err, storage.ErrReportBranchFlowNotSelected)
	report(delivery2, "failed", plan2)
	combinedAdmission, err := s.AuthorizeRunDispatch(ctx, combinedRun, human.ID, combinedPackage, combinedTarget)
	require.NoError(t, err)
	require.NotNil(t, combinedAdmission.ReportFlowBasis)
	require.NotNil(t, combinedAdmission.ReportJoinBasis)

	// A changed inherited scope makes old completion facts unusable for new targets, not old admissions.
	makeSpec("join-parent")
	_, err = s.AddEdge(ctx, "join-parent", "join-a", storage.EdgeTypeComposes)
	require.NoError(t, err)
	futureRun, _, _ := makeRun("join-future-target")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: joinedFlow.ID, RunID: futureRun, Mode: "all"}, nil)
	require.NoError(t, err)
	future, err := s.ReadReportFlowJoin(ctx, futureRun)
	require.NoError(t, err)
	require.False(t, future.Evaluation.Satisfied, "scope change invalidates A's exact completion")
	replayedAll, err = s.AuthorizeRunDispatch(ctx, allRun, human.ID, allPackage, allTarget)
	require.NoError(t, err)
	require.Equal(t, allAdmission.ID, replayedAll.ID)
	approvedAgain := "approved"
	_, err = s.UpdateSpec(ctx, "join-b", nil, &approvedAgain, nil, nil, nil)
	require.NoError(t, err)
	future, err = s.ReadReportFlowJoin(ctx, futureRun)
	require.NoError(t, err)
	require.False(t, future.Evaluation.Satisfied, "reopened B cannot count its old completion")

	// Known empty paths may skip only when explicitly configured; unknown never becomes empty.
	emptyRun, _, _ := makeRun("join-empty-branch")
	emptyFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "empty-path", Conditions: []storage.ArmReportBranchCondition{reportFlowCondition("only", delivery1, plan1)},
		Branches: []storage.ArmReportBranchMember{{RunID: emptyRun, ConditionKey: "only", When: "true"}},
	}, nil)
	require.NoError(t, err)
	skipRun, skipPackage, skipTarget := makeRun("join-skip-target")
	strictRun, strictPackage, strictTarget := makeRun("join-strict-target")
	skipJoin, err := s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: emptyFlow.ID, RunID: skipRun, Mode: "all", AllowEmptySkip: true}, nil)
	require.NoError(t, err)
	strictJoin, err := s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: emptyFlow.ID, RunID: strictRun, Mode: "any"}, nil)
	require.NoError(t, err)
	require.True(t, skipJoin.Evaluation.Satisfied)
	require.True(t, skipJoin.Evaluation.ExplicitSkip)
	require.Len(t, skipJoin.Evaluation.Conditions, 1)
	require.Equal(t, joinedFailed.ID, skipJoin.Evaluation.Conditions[0].Evaluation.Report.ID, "unselected R1 remains the explicit skip basis")
	require.False(t, strictJoin.Evaluation.Satisfied)
	require.Equal(t, "empty_path", strictJoin.Evaluation.Reason)
	skipAdmission, err := s.AuthorizeRunDispatch(ctx, skipRun, human.ID, skipPackage, skipTarget)
	require.NoError(t, err)
	require.True(t, skipAdmission.ReportJoinBasis.Evaluation.ExplicitSkip)
	require.Equal(t, joinedFailed.ID, skipAdmission.ReportJoinBasis.Evaluation.Conditions[0].Evaluation.Report.ID)
	blockedReport := report(delivery1, "environment_blocked", plan1)
	skipJoin, err = s.ReadReportFlowJoin(ctx, skipRun)
	require.NoError(t, err)
	require.Equal(t, blockedReport.ID, skipJoin.Evaluation.Conditions[0].Evaluation.Report.ID, "current R2 is separate from consumed R1")
	require.Equal(t, joinedFailed.ID, skipJoin.Admission.Proof.Evaluation.Conditions[0].Evaluation.Report.ID)
	strictJoin, err = s.ReadReportFlowJoin(ctx, strictRun)
	require.NoError(t, err)
	require.Len(t, strictJoin.Evaluation.Unknown, 1)
	_, err = s.AuthorizeRunDispatch(ctx, strictRun, human.ID, strictPackage, strictTarget)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	_, err = s.CancelReportBranchFlow(operator, emptyFlow.ID, "Stop pending joins", nil)
	require.NoError(t, err)
	changedPlan := "Changed plan-one after cancellation"
	_, err = s.UpdateSpec(ctx, "plan-one", &changedPlan, nil, nil, nil, nil)
	require.NoError(t, err)
	skipJoin, err = s.ReadReportFlowJoin(ctx, skipRun)
	require.NoError(t, err)
	require.Equal(t, "flow_cancelled", skipJoin.Evaluation.Reason)
	require.Empty(t, skipJoin.Evaluation.Conditions)
	require.Equal(t, skipAdmission.ID, skipJoin.Admission.ID)
	require.Equal(t, joinedFailed.ID, skipJoin.Admission.Proof.Evaluation.Conditions[0].Evaluation.Report.ID)
	_, err = s.AuthorizeRunDispatch(ctx, strictRun, human.ID, strictPackage, strictTarget)
	require.ErrorIs(t, err, storage.ErrReportFlowJoinUnsatisfied)
	replayedSkip, err := s.AuthorizeRunDispatch(ctx, skipRun, human.ID, skipPackage, skipTarget)
	require.NoError(t, err)
	require.True(t, replayedSkip.Replayed)
	require.True(t, replayedSkip.ReportJoinBasis.Evaluation.ExplicitSkip)

	judgmentSource := makeSpec("judgment-source")
	independentSource := makeSpec("judgment-independent")
	judgeTrueRun, judgeTruePackage, judgeTrueTarget := makeRun("judgment-true")
	judgeFalseRun, judgeFalsePackage, judgeFalseTarget := makeRun("judgment-false")
	independentRun, independentPackage, independentTarget := makeRun("judgment-independent-branch")
	judgmentFlow, err := s.ArmReportBranchFlow(operator, storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "judgment-flow",
		Conditions: []storage.ArmReportBranchCondition{
			{Key: "J", Kind: "judgment", Judgment: &storage.ReportBranchJudgmentInput{Criterion: "Is the recorded candidate suitable?", InputSources: []storage.ReviewSource{judgmentSource}}},
			{Key: "I", Kind: "judgment", Judgment: &storage.ReportBranchJudgmentInput{Criterion: "Is the independent requirement satisfied?", InputSources: []storage.ReviewSource{independentSource}}},
		},
		Branches: []storage.ArmReportBranchMember{
			{RunID: judgeTrueRun, ConditionKey: "J", When: "true"},
			{RunID: judgeFalseRun, ConditionKey: "J", When: "false"},
			{RunID: independentRun, ConditionKey: "I", When: "true"},
		},
	}, nil)
	require.NoError(t, err)
	newJudgment := func(key, value, reason string, refs []storage.ReviewSource, predecessor *string) (*storage.ReportBranchJudgmentResult, error) {
		return s.RecordReportBranchJudgment(operator, storage.RecordReportBranchJudgmentRequest{
			FlowID: judgmentFlow.ID, ConditionKey: key, ExpectedJudgmentID: predecessor,
			Value: value, InputRefs: refs, Reason: reason,
		}, nil)
	}
	_, err = newJudgment("J", "true", "Wrong source position", []storage.ReviewSource{independentSource}, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchJudgment)
	j1, err := newJudgment("J", "true", "Original explicit judgment", []storage.ReviewSource{judgmentSource}, nil)
	require.NoError(t, err)
	require.False(t, j1.Replayed)
	judgmentView, err := s.ReadReportBranchFlow(ctx, judgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "true", judgmentView.Conditions[1].Evaluation.Value)
	require.Equal(t, j1.Judgment.ID, judgmentView.Conditions[1].Evaluation.Judgment.ID)
	trueAdmission, err := s.AuthorizeRunDispatch(ctx, judgeTrueRun, human.ID, judgeTruePackage, judgeTrueTarget)
	require.NoError(t, err)
	require.Equal(t, j1.Judgment.ID, trueAdmission.ReportFlowBasis.Judgment.ID)
	require.NoError(t, s.RecordCompletion(ctx, "judgment-true", judgeTrueRun))
	j2, err := newJudgment("J", "false", "Corrected value without changing original inputs", []storage.ReviewSource{judgmentSource}, &j1.Judgment.ID)
	require.NoError(t, err)
	joinJudgeRun, joinJudgePackage, joinJudgeTarget := makeRun("judgment-any-target")
	_, err = s.ArmReportFlowJoin(operator, storage.ArmReportFlowJoinRequest{FlowID: judgmentFlow.ID, RunID: joinJudgeRun, Mode: "any"}, nil)
	require.NoError(t, err)
	joinJudgment, err := s.ReadReportFlowJoin(ctx, joinJudgeRun)
	require.NoError(t, err)
	require.True(t, joinJudgment.Evaluation.Satisfied, "admitted J1 is a valid witness despite unknown independent condition")
	require.Len(t, joinJudgment.Evaluation.Unknown, 1)
	require.Equal(t, j2.Judgment.ID, joinJudgment.Evaluation.Conditions[0].Evaluation.Judgment.ID)
	joinJudgeAdmission, err := s.AuthorizeRunDispatch(ctx, joinJudgeRun, human.ID, joinJudgePackage, joinJudgeTarget)
	require.NoError(t, err)
	require.Equal(t, j2.Judgment.ID, joinJudgeAdmission.ReportJoinBasis.Evaluation.Conditions[0].Evaluation.Judgment.ID)
	judgeFalseAdmission, err := s.AuthorizeRunDispatch(ctx, judgeFalseRun, human.ID, judgeFalsePackage, judgeFalseTarget)
	require.NoError(t, err)
	require.Equal(t, j2.Judgment.ID, judgeFalseAdmission.ReportFlowBasis.Judgment.ID)
	j2Replay, err := newJudgment("J", "false", "Corrected value without changing original inputs", []storage.ReviewSource{judgmentSource}, &j1.Judgment.ID)
	require.NoError(t, err)
	require.True(t, j2Replay.Replayed)
	require.Equal(t, j2.Judgment.ID, j2Replay.Judgment.ID)
	_, err = newJudgment("J", "true", "Different successor", []storage.ReviewSource{judgmentSource}, &j1.Judgment.ID)
	require.ErrorIs(t, err, storage.ErrReportBranchJudgmentConflict)
	otherHuman, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Other judge", Role: "admin"}, nil)
	require.NoError(t, err)
	otherActor := auth.WithIdentity(ctx, &auth.Identity{UserID: otherHuman.ID, UserKind: storage.KindHuman})
	_, err = s.RecordReportBranchJudgment(otherActor, storage.RecordReportBranchJudgmentRequest{
		FlowID: judgmentFlow.ID, ConditionKey: "J", ExpectedJudgmentID: &j1.Judgment.ID,
		Value: "false", InputRefs: []storage.ReviewSource{judgmentSource}, Reason: "Corrected value without changing original inputs",
	}, nil)
	require.ErrorIs(t, err, storage.ErrReportBranchJudgmentConflict, "another actor cannot replay the predecessor's successor")
	i1, err := newJudgment("I", "true", "Independent content still applies", []storage.ReviewSource{independentSource}, nil)
	require.NoError(t, err)
	changedJudgment := "Changed judgment source"
	_, err = s.UpdateSpec(ctx, "judgment-source", &changedJudgment, nil, nil, nil, nil)
	require.NoError(t, err)
	judgmentView, err = s.ReadReportBranchFlow(ctx, judgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "true", judgmentView.Conditions[0].Evaluation.Value, "independent C2 is not frozen by J source change")
	require.Equal(t, "unknown", judgmentView.Conditions[1].Evaluation.Value)
	require.Equal(t, "input_changed", judgmentView.Conditions[1].Evaluation.Reason)
	independentAdmission, err := s.AuthorizeRunDispatch(ctx, independentRun, human.ID, independentPackage, independentTarget)
	require.NoError(t, err)
	require.Equal(t, i1.Judgment.ID, independentAdmission.ReportFlowBasis.Judgment.ID)
	judgmentRefs, err := s.ReadSpecSourceRefs(ctx, "judgment-source")
	require.NoError(t, err)
	changedRef := judgmentSource
	changedRef.ChangeID = judgmentRefs["intent"]
	j3, err := newJudgment("J", "true", "Re-evaluated changed source", []storage.ReviewSource{changedRef}, &j2.Judgment.ID)
	require.NoError(t, err)
	judgmentView, err = s.ReadReportBranchFlow(ctx, judgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "true", judgmentView.Conditions[1].Evaluation.Value)
	judgmentOriginal := "Original judgment-source"
	_, err = s.UpdateSpec(ctx, "judgment-source", &judgmentOriginal, nil, nil, nil, nil)
	require.NoError(t, err)
	judgmentView, err = s.ReadReportBranchFlow(ctx, judgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown", judgmentView.Conditions[1].Evaluation.Value, "old J1 is not revived behind a stale latest J3")
	judgmentRefs, err = s.ReadSpecSourceRefs(ctx, "judgment-source")
	require.NoError(t, err)
	restoredRef := judgmentSource
	restoredRef.ChangeID = judgmentRefs["intent"]
	j4, err := newJudgment("J", "false", "Explicit judgment on restored source", []storage.ReviewSource{restoredRef}, &j3.Judgment.ID)
	require.NoError(t, err)
	require.Equal(t, restoredRef.ChangeID, j4.Judgment.InputRefs[0].ChangeID)
	judgmentView, err = s.ReadReportBranchFlow(ctx, judgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "false", judgmentView.Conditions[1].Evaluation.Value)
	require.Equal(t, j1.Judgment.ID, trueAdmission.ReportFlowBasis.Judgment.ID)
	require.Equal(t, j2.Judgment.ID, joinJudgeAdmission.ReportJoinBasis.Evaluation.Conditions[0].Evaluation.Judgment.ID)
	latest := j4.Judgment.ID
	for i := range 51 {
		value := "true"
		if i%2 == 0 {
			value = "false"
		}
		result, err := newJudgment("J", value, fmt.Sprintf("History revision %d", i), []storage.ReviewSource{restoredRef}, &latest)
		require.NoError(t, err)
		latest = result.Judgment.ID
	}
	page, err := s.ReadReportBranchJudgmentHistory(ctx, judgmentFlow.ID, "J", "")
	require.NoError(t, err)
	require.Len(t, page.Judgments, 50)
	require.True(t, page.HasMore)
	require.NotNil(t, page.NextCursor)
	older, err := s.ReadReportBranchJudgmentHistory(ctx, judgmentFlow.ID, "J", *page.NextCursor)
	require.NoError(t, err)
	require.False(t, older.HasMore)
	require.Equal(t, j1.Judgment.ID, older.Judgments[len(older.Judgments)-1].ID)
	historyBody, err := json.Marshal(map[string]string{"flowId": judgmentFlow.ID, "conditionKey": "J"})
	require.NoError(t, err)
	historyReply, err := server.ExecuteHumanReportJudgmentHistory(operator, s, "report-branches", bytes.NewReader(historyBody))
	require.NoError(t, err)
	var humanHistory storage.ReportBranchJudgmentPage
	require.NoError(t, json.Unmarshal(historyReply, &humanHistory))
	require.Len(t, humanHistory.Judgments, 50)
	emptyJudgeRun, emptyJudgePackage, emptyJudgeTarget := makeRun("judgment-empty")
	emptyJudgmentRequest := storage.ArmReportBranchFlowRequest{
		IdempotencyKey: "judgment-empty-flow",
		Conditions: []storage.ArmReportBranchCondition{{Key: "E", Kind: "judgment", Judgment: &storage.ReportBranchJudgmentInput{
			Criterion: "Explicit operator choice without declared source", InputSources: nil,
		}}},
		Branches: []storage.ArmReportBranchMember{{RunID: emptyJudgeRun, ConditionKey: "E", When: "true"}},
	}
	_, err = s.ArmReportBranchFlow(operator, emptyJudgmentRequest, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchFlow, "omitted/null source array is not an explicit empty set")
	emptyJudgmentRequest.Conditions[0].Judgment.InputSources = []storage.ReviewSource{}
	emptyJudgmentFlow, err := s.ArmReportBranchFlow(operator, emptyJudgmentRequest, nil)
	require.NoError(t, err)
	_, err = s.RecordReportBranchJudgment(operator, storage.RecordReportBranchJudgmentRequest{
		FlowID: emptyJudgmentFlow.ID, ConditionKey: "E", Value: "true", Reason: "Human selected this branch", InputRefs: nil,
	}, nil)
	require.ErrorIs(t, err, storage.ErrInvalidReportBranchJudgment)
	emptyBody, err := json.Marshal(map[string]any{"flowId": emptyJudgmentFlow.ID, "conditionKey": "E", "expectedJudgmentId": nil,
		"value": "true", "inputRefs": []storage.ReviewSource{}, "reason": "Human selected this branch"})
	require.NoError(t, err)
	emptyReply, err := server.ExecuteReportBranchJudgmentRecord(operator, s, "report-branches", "report-judgment-record", bytes.NewReader(emptyBody))
	require.NoError(t, err)
	var emptyFact storage.ReportBranchJudgmentResult
	require.NoError(t, json.Unmarshal(emptyReply, &emptyFact))
	require.Empty(t, emptyFact.Judgment.InputRefs)
	emptyView, err := s.ReadReportBranchFlow(ctx, emptyJudgmentFlow.ID)
	require.NoError(t, err)
	require.Equal(t, "true", emptyView.Conditions[0].Evaluation.Value)
	_, err = s.AuthorizeRunDispatch(ctx, emptyJudgeRun, human.ID, emptyJudgePackage, emptyJudgeTarget)
	require.NoError(t, err)
}
