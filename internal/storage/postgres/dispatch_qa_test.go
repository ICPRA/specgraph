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

func TestDispatchQABasis(t *testing.T) {
	s := newStore(t, postgres.WithProject("dispatch-qa"))
	users, err := postgres.NewAuth(context.Background(), s.Pool())
	require.NoError(t, err)
	operator, err := users.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: "QA manager", Role: "admin"}, nil)
	require.NoError(t, err)
	ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: operator.ID, UserKind: storage.KindHuman})
	require.NoError(t, s.SetProjectManaged(ctx, "dispatch-qa", true))
	create := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Explicit source and acceptance expectation for "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	create("reviewer")
	reviewer, err := s.PrepareRun(ctx, "reviewer", "review-workspace")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, reviewer, "local", "review-thread"))
	approve := func(slug, kind string, source storage.ReviewSource, requirements []string) storage.SourceReviewResult {
		t.Helper()
		state, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: slug, Kind: kind, Sources: []storage.ReviewSource{source}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: operator.ID}, RequirementDecisionIDs: requirements, ReviewerRunID: reviewer}, nil)
		require.NoError(t, err)
		index := 0
		if kind == "design" {
			index = 1
		}
		result, err := s.ReviewSource(ctx, slug, state.Reviews[index].Request.ID, "accepted", "Approved declared source for these tasks")
		require.NoError(t, err)
		return *result
	}
	requirementsSource := create("requirements-source")
	requirement := approve("requirements-source", "requirements", requirementsSource, nil)
	designSource := create("design-source")
	design := approve("design-source", "design", designSource, []string{requirement.Decision.ID})
	otherSource := create("other-requirements")
	otherRequirement := approve("other-requirements", "requirements", otherSource, nil)
	planSource := create("test-plan-source")
	unreviewedDesign := create("unreviewed-design")
	originalRefs, err := s.ReadSpecSourceRefs(ctx, "test-plan-source")
	require.NoError(t, err)
	full := storage.DispatchQABasis{RequirementDecisionIDs: []string{requirement.Decision.ID}, DesignDecisionIDs: []string{design.Decision.ID}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{planSource}, Applicability: "Shared approved behavior and test plan apply to both implementation nodes."}
	empty := storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{}, Applicability: "Applies to this task."}
	target := func(slug, purpose string, basis *storage.DispatchQABasis) json.RawMessage {
		t.Helper()
		fields := map[string]any{"environmentId": "local", "threadId": "thread-" + slug, "workspace": "workspace-" + slug, "createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug, "workPurpose": purpose, "purposeGuidance": "Carry out the declared purpose using the saved original references."}
		if basis != nil {
			fields["qaBasis"] = basis
		}
		body, err := json.Marshal(fields)
		require.NoError(t, err)
		return body
	}
	prepare := func(slug string, body json.RawMessage) (string, string) {
		t.Helper()
		run, replayed, err := s.PrepareRunForOperator(ctx, slug, "workspace-"+slug, operator.ID, "prepare-"+slug, body)
		require.NoError(t, err)
		require.False(t, replayed)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "thread-"+slug))
		pkg, err := s.ReadRunContext(ctx, run)
		require.NoError(t, err)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(pkg.Body, &fields))
		require.JSONEq(t, string(body), string(fields["dispatch_target"]))
		return run, pkg.PackageID
	}
	for _, purpose := range []string{"requirements", "requirements_review", "design_review", "investigation", "coordination", "knowledge"} {
		slug := "nonrecursive-" + purpose
		source := create(slug)
		if purpose == "knowledge" {
			require.NoError(t, s.StoreShapeOutput(ctx, slug, &storage.ShapeOutput{ChosenApproach: "Original input"}))
		}
		if purpose == "investigation" {
			create("ordinary-prerequisite")
			done := "done"
			_, err = s.UpdateSpec(ctx, "ordinary-prerequisite", nil, &done, nil, nil, nil)
			require.NoError(t, err)
			_, err = s.AddEdge(ctx, slug, "ordinary-prerequisite", storage.EdgeTypeDependsOn)
			require.NoError(t, err)
		}
		body := target(slug, purpose, nil)
		run, pkg := prepare(slug, body)
		_, err := s.AuthorizeRunDispatch(ctx, run, operator.ID, pkg, body)
		require.NoError(t, err, "authoring and review worker creation must not need its own future output")
		if purpose == "requirements" {
			continue
		} // Authoring still requires its explicit approved submission.
		if purpose == "coordination" {
			_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 hour' WHERE project_slug='dispatch-qa' AND agent=$1`, run)
			require.NoError(t, err)
			require.ErrorIs(t, s.RecordCompletion(ctx, slug, run), storage.ErrAgentNotClaimOwner)
			_, err = s.ClaimSpec(ctx, slug, run, 30*time.Minute)
			require.NoError(t, err)
		}
		if purpose == "investigation" {
			approved := "approved"
			_, err = s.UpdateSpec(ctx, "ordinary-prerequisite", nil, &approved, nil, nil, nil)
			require.NoError(t, err)
			require.ErrorIs(t, s.RecordCompletion(ctx, slug, run), storage.ErrDependenciesNotReady)
			done := "done"
			_, err = s.UpdateSpec(ctx, "ordinary-prerequisite", nil, &done, nil, nil, nil)
			require.NoError(t, err)
		}
		if purpose == "knowledge" {
			maximum := int32(1)
			hold, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: slug, Kind: "requirements", Sources: []storage.ReviewSource{source}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: operator.ID}, ReviewerRunID: reviewer, MaxReviewRounds: &maximum}, nil)
			require.NoError(t, err)
			id := hold.Reviews[0].Request.ID
			_, err = s.SubmitReview(ctx, storage.MailScope{EnvironmentID: "local", ThreadID: "review-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}, id, "rejected", "Synthetic human intervention required")
			require.NoError(t, err)
			require.ErrorIs(t, s.RecordCompletion(ctx, slug, run), storage.ErrReviewHumanHold)
			_, err = s.ReviewSource(ctx, slug, id, "rejected", "Human resolves hold without accepting a delivery")
			require.NoError(t, err)
			require.NoError(t, s.StoreShapeOutput(ctx, slug, &storage.ShapeOutput{ChosenApproach: "Changed unsubmitted input"}))
			require.ErrorIs(t, s.RecordCompletion(ctx, slug, run), storage.ErrCompletionRequiresRequirementReview)
			require.NoError(t, s.StoreShapeOutput(ctx, slug, &storage.ShapeOutput{ChosenApproach: "Original input"}))
		}
		require.NoError(t, s.RecordCompletion(ctx, slug, run), "ordinary work must not need another human delivery opinion")
	}
	for _, purpose := range []string{"design", "test_design", "test_execution"} {
		slug := "valid-" + purpose
		create(slug)
		basis := empty
		if purpose != "test_execution" {
			basis.RequirementDecisionIDs = []string{requirement.Decision.ID}
		}
		if purpose == "test_design" {
			basis.DesignSources = []storage.ReviewSource{unreviewedDesign}
		}
		if purpose == "test_execution" {
			basis.TestPlanSources = []storage.ReviewSource{planSource}
		}
		body := target(slug, purpose, &basis)
		run, pkg := prepare(slug, body)
		_, err := s.AuthorizeRunDispatch(ctx, run, operator.ID, pkg, body)
		require.NoError(t, err)
		if purpose != "design" {
			require.NoError(t, s.RecordCompletion(ctx, slug, run), "finishing test design/execution does not assert tested implementation passed")
		}
	}
	for _, missing := range []string{"requirements", "design", "plan", "applicability", "wrong-kind", "mismatched-design-requirements", "unknown-source", "design-without-requirements", "test-design-without-design", "test-execution-without-plan", "nonrecursive-invalid-extra"} {
		slug := "missing-" + missing
		create(slug)
		basis := full
		purpose := "implementation"
		switch missing {
		case "requirements":
			basis.RequirementDecisionIDs = []string{}
		case "design":
			basis.DesignDecisionIDs = []string{}
		case "plan":
			basis.TestPlanSources = []storage.ReviewSource{}
		case "applicability":
			basis.Applicability = " "
		case "wrong-kind":
			basis.DesignDecisionIDs = []string{requirement.Decision.ID}
		case "nonrecursive-invalid-extra":
			purpose = "requirements"
			basis.RequirementDecisionIDs = []string{design.Decision.ID}
		case "mismatched-design-requirements":
			basis.RequirementDecisionIDs = []string{otherRequirement.Decision.ID}
		case "unknown-source":
			basis.TestPlanSources = []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "test-plan-source", Field: "intent", ChangeID: "missing-change"}}
		case "design-without-requirements":
			purpose = "design"
			basis = empty
		case "test-design-without-design":
			purpose = "test_design"
			basis = empty
			basis.RequirementDecisionIDs = []string{requirement.Decision.ID}
		case "test-execution-without-plan":
			purpose = "test_execution"
			basis = empty
		}
		_, _, err := s.PrepareRunForOperator(ctx, slug, "workspace-"+slug, operator.ID, "prepare-"+slug, target(slug, purpose, &basis))
		require.Error(t, err, missing)
		require.Contains(t, err.Error(), "qaBasis", "prerequisite diagnostics must identify QA rather than suggest authentication repair")
		var count int
		require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM run_bindings WHERE project_slug=$1 AND task_spec_slug=$2)+(SELECT count(*) FROM claims WHERE project_slug=$1 AND spec_slug=$2)+(SELECT count(*) FROM context_packages WHERE project_slug=$1 AND task_spec_slug=$2)+(SELECT count(*) FROM run_preparations WHERE project_slug=$1 AND task_slug=$2)`, "dispatch-qa", slug).Scan(&count))
		require.Zero(t, count, "failed prerequisite check must publish no run, claim, package or success receipt")
	}
	// Moving the current field pointer is not a semantic veto on explicitly referenced original content.
	revised := "New plan draft, not automatically a replacement for the selected original plan."
	_, err = s.UpdateSpec(ctx, "test-plan-source", &revised, nil, nil, nil, nil)
	require.NoError(t, err)
	revisedRefs, err := s.ReadSpecSourceRefs(ctx, "test-plan-source")
	require.NoError(t, err)
	require.NotEqual(t, originalRefs, revisedRefs)
	create("implementation-one")
	create("implementation-two")
	firstBody := target("implementation-one", "implementation", &full)
	secondBody := target("implementation-two", "implementation", &full)
	first, firstPkg := prepare("implementation-one", firstBody)
	second, secondPkg := prepare("implementation-two", secondBody)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation-one", first), storage.ErrImplementationTestsRequired, "implementation still requires matching test reports")
	currentRefs, err := s.ReadSpecSourceRefs(ctx, "test-plan-source")
	require.NoError(t, err)
	require.Equal(t, revisedRefs, currentRefs, "preparation cannot rewrite source refs")
	_, err = s.AuthorizeRunDispatch(ctx, first, operator.ID, firstPkg, firstBody)
	require.NoError(t, err)
	changed := target("implementation-one", "investigation", nil)
	_, _, err = s.PrepareRunForOperator(ctx, "implementation-one", "workspace-implementation-one", operator.ID, "prepare-implementation-one", changed)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.AuthorizeRunDispatch(ctx, first, operator.ID, firstPkg, changed)
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	changedBasis := full
	changedBasis.Applicability = "Another declaration"
	_, err = s.AuthorizeRunDispatch(ctx, first, operator.ID, firstPkg, target("implementation-one", "implementation", &changedBasis))
	require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	_, err = s.ReviewSource(ctx, "design-source", design.Decision.RequestID, "rejected", "Human revokes this design approval")
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, second, operator.ID, secondPkg, secondBody)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	_, err = s.AuthorizeRunDispatch(ctx, first, operator.ID, firstPkg, firstBody)
	require.ErrorIs(t, err, storage.ErrInvalidReview, "authorization replay must recheck current explicit decisions")
	status, err := s.ReadRunDispatch(ctx, second)
	require.NoError(t, err)
	require.Nil(t, status.Admission)
	firstStatus, err := s.ReadRunDispatch(ctx, first)
	require.NoError(t, err)
	require.NotNil(t, firstStatus.Admission)
	require.Nil(t, firstStatus.Resolution, "revocation does not fabricate native stop/release")
	create("revoked-requirement")
	reqBasis := empty
	reqBasis.RequirementDecisionIDs = []string{requirement.Decision.ID}
	reqBody := target("revoked-requirement", "design", &reqBasis)
	reqRun, reqPkg := prepare("revoked-requirement", reqBody)
	_, err = s.ReviewSource(ctx, "requirements-source", requirement.Decision.RequestID, "rejected", "Human revokes this requirement approval")
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, reqRun, operator.ID, reqPkg, reqBody)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	// Historical unknown purpose remains readable but cannot gain native admission.
	create("legacy-unknown")
	legacy, err := s.PrepareRun(ctx, "legacy-unknown", "workspace-legacy-unknown")
	require.NoError(t, err)
	legacyBody := json.RawMessage(`{"environmentId":"local","threadId":"thread-legacy-unknown","workspace":"workspace-legacy-unknown","createCommandId":"create","startCommandId":"start","messageId":"message"}`)
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=body||jsonb_build_object('dispatch_target',$2::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, legacy, legacyBody)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, legacy, "local", "thread-legacy-unknown"))
	legacyPackage, err := s.ReadRunContext(ctx, legacy)
	require.NoError(t, err)
	_, err = s.AuthorizeRunDispatch(ctx, legacy, operator.ID, legacyPackage.PackageID, legacyBody)
	require.ErrorIs(t, err, storage.ErrInvalidRunPreparation)
	require.ErrorIs(t, s.RecordCompletion(ctx, "legacy-unknown", legacy), storage.ErrManagedCompletionRequiresAcceptance, "unknown legacy purpose retains its original behavior")
}
