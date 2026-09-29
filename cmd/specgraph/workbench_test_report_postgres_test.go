// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

// All reports here are synthetic isolated fixtures, not claims that these commands ran.
func TestWorkbenchTestReportsLocalPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("test-reports"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Fixture operator", Role: "admin"}, nil)
	require.NoError(t, err)
	reader, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Fixture Agent credential", Role: "reader"}, nil)
	require.NoError(t, err)
	secret, hash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	key, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: reader.ID, PHCHash: hash})
	require.NoError(t, err)
	credential := auth.FormatAPIKeyToken(key.Prefix, secret)
	readerCredential := credential
	adminSecret, adminHash, err := auth.GenerateAPIKeySecret()
	require.NoError(t, err)
	adminKey, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: human.ID, PHCHash: adminHash})
	require.NoError(t, err)
	adminCredential := auth.FormatAPIKeyToken(adminKey.Prefix, adminSecret)
	operator := auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	create := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Synthetic explicit plan/source for "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	create("reviewer")
	reviewer, err := s.PrepareRun(ctx, "reviewer", "C:/fixture-reviewer")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, reviewer, "local", "reviewer-thread"))
	approve := func(slug, kind string, requirements []string) string {
		t.Helper()
		source := create(slug)
		state, err := s.AssignReview(operator, &storage.AssignReviewRequest{TaskSlug: slug, Kind: kind, Sources: []storage.ReviewSource{source}, RequirementDecisionIDs: requirements, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer}, nil)
		require.NoError(t, err)
		index := 0
		if kind == "design" {
			index = 1
		}
		decision, err := s.ReviewSource(operator, slug, state.Reviews[index].Request.ID, "accepted", "Synthetic source approval")
		require.NoError(t, err)
		return decision.Decision.ID
	}
	requirementID := approve("requirements", "requirements", nil)
	designID := approve("design", "design", []string{requirementID})
	plans := []storage.ReviewSource{create("plan-one"), create("plan-two")}
	otherPlan := create("other-plan")
	commit := strings.Repeat("b", 40)
	prepare := func(slug, purpose, baseline string) string {
		t.Helper()
		create(slug)
		target := map[string]any{"environmentId": "local", "threadId": "thread-" + slug, "workspace": "C:/" + slug, "createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug, "workPurpose": purpose, "purposeGuidance": "Report actual results for the selected fixed commit and plans.", "gitBaseline": map[string]any{"isRepo": true, "commitSha": baseline}}
		if purpose == "implementation" || purpose == "test_execution" {
			basis := storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: plans, Applicability: "Both synthetic cases apply"}
			if purpose == "implementation" {
				basis.RequirementDecisionIDs = []string{requirementID}
				basis.DesignDecisionIDs = []string{designID}
			}
			target["qaBasis"] = basis
		}
		body, err := json.Marshal(target)
		require.NoError(t, err)
		run, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, human.ID, "prepare-"+slug, body)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "thread-"+slug))
		pkg, err := s.ReadRunContext(ctx, run)
		require.NoError(t, err)
		_, err = s.AuthorizeRunDispatch(ctx, run, human.ID, pkg.PackageID, body)
		require.NoError(t, err)
		return run
	}
	implementation := prepare("implementation", "implementation", strings.Repeat("a", 40))
	tester := prepare("tester", "test_execution", commit)
	prepare("wrong-commit", "test_execution", strings.Repeat("c", 40))
	prepare("wrong-purpose", "coordination", commit)
	snapshot, _ := json.Marshal(map[string]any{"git": map[string]any{"head": map[string]any{"isRepo": true, "commitSha": commit}}})
	var delivery string
	project := "test-reports"
	commandSlug := ""
	call := func(operation string, body any) workbenchCommandResponse {
		t.Helper()
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		input, err := json.Marshal(workbenchCommandRequest{ID: "test-report", Operation: operation, Project: project, Slug: commandSlug, Credential: credential, Body: encoded})
		require.NoError(t, err)
		var output bytes.Buffer
		err = runWorkbenchCommandStdio(ctx, bytes.NewReader(append(input, '\n')), &output, func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, procedure)
			if err != nil {
				return nil, err
			}
			callCtx = auth.WithIdentity(callCtx, identity)
			if req.Operation == "delivery-self-context" || req.Operation == "delivery-submit-self" {
				action, ok := auth.ActionForProcedure(procedure)
				require.True(t, ok)
				if req.Operation == "delivery-self-context" {
					require.Equal(t, "execution.read", action)
				} else {
					require.Equal(t, "delivery-submission.write", action)
				}
				result, err := server.ExecuteAgentDelivery(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(result)
			}
			if req.Operation == "run-complete-self" {
				result, err := server.ExecuteAgentRunCompletion(callCtx, s, req.Project, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(result)
			}
			if req.Operation == "test-result-submit" {
				report, err := server.ExecuteAgentTestReport(callCtx, s, req.Project, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(report)
			}
			if req.Operation == "record-test-result" || req.Operation == "complete-run" {
				return server.ExecuteWorkbenchCommand(callCtx, s, req.Operation, req.Project, req.Slug, identity.UserID, req.Body)
			}
			result, err := server.ExecuteLocalWorkbenchKnowledge(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
			if err != nil {
				return nil, err
			}
			return json.Marshal(result)
		})
		require.NoError(t, err)
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(output.Bytes(), &response))
		return response
	}
	deliveryScope := storage.MailScope{EnvironmentID: "local", ThreadID: "thread-implementation", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	project = ""
	contextResponse := call("delivery-self-context", map[string]any{"scope": deliveryScope})
	require.Nil(t, contextResponse.Error)
	var deliveryContext storage.DeliverySelfContext
	require.NoError(t, json.Unmarshal(contextResponse.Data, &deliveryContext))
	require.Equal(t, implementation, deliveryContext.RunID)
	require.Equal(t, "implementation", deliveryContext.TaskSlug)
	require.Equal(t, "C:/implementation", deliveryContext.Workspace)
	project = "test-reports"
	beforeSubmission, err := s.GetSpec(ctx, "implementation")
	require.NoError(t, err)
	beforeSubmissionClaim, err := s.GetActiveClaim(ctx, "implementation")
	require.NoError(t, err)
	deliveryBody := map[string]any{"scope": deliveryScope, "expectedRunId": implementation, "summary": "Synthetic host-observed self delivery", "head": storage.DeliveryHead{IsRepo: true, CommitSHA: &commit}}
	submitted := call("delivery-submit-self", deliveryBody)
	require.Nil(t, submitted.Error)
	var deliveryReceipt storage.DeliverySelfReceipt
	require.NoError(t, json.Unmarshal(submitted.Data, &deliveryReceipt))
	delivery = deliveryReceipt.DeliveryID
	require.Equal(t, implementation, deliveryReceipt.RunID)
	require.Equal(t, "implementation", deliveryReceipt.TaskSlug)
	originalDelivery, err := s.ReadWorkbenchDelivery(ctx, delivery)
	require.NoError(t, err)
	require.Equal(t, implementation, originalDelivery.SubmittedBy)
	var submittedSnapshot struct {
		Summary string `json:"summary"`
		Git     struct {
			Workspace   string               `json:"workspace"`
			Baseline    storage.DeliveryHead `json:"baseline"`
			Head        storage.DeliveryHead `json:"head"`
			Uncommitted bool                 `json:"uncommittedContentIncluded"`
		} `json:"git"`
	}
	require.NoError(t, json.Unmarshal(originalDelivery.Snapshot, &submittedSnapshot))
	require.Equal(t, "C:/implementation", submittedSnapshot.Git.Workspace)
	require.Equal(t, strings.Repeat("a", 40), *submittedSnapshot.Git.Baseline.CommitSHA)
	require.Equal(t, commit, *submittedSnapshot.Git.Head.CommitSHA)
	require.False(t, submittedSnapshot.Git.Uncommitted)
	afterSubmission, err := s.GetSpec(ctx, "implementation")
	require.NoError(t, err)
	require.Equal(t, beforeSubmission, afterSubmission)
	afterSubmissionClaim, err := s.GetActiveClaim(ctx, "implementation")
	require.NoError(t, err)
	require.Equal(t, beforeSubmissionClaim, afterSubmissionClaim)
	for _, selector := range []string{"runId", "taskSlug", "workspace", "baseline", "actor", "project"} {
		forged := map[string]any{"scope": deliveryScope, "expectedRunId": implementation, "summary": "Invalid override", "head": storage.DeliveryHead{IsRepo: true, CommitSHA: &commit}, selector: "model-chosen"}
		response := call("delivery-submit-self", forged)
		require.NotNil(t, response.Error)
		require.Equal(t, "invalid_argument", response.Error.Code)
	}
	forgedRead := call("delivery-self-context", map[string]any{"scope": deliveryScope, "runId": tester})
	require.NotNil(t, forgedRead.Error)
	generalSubmission := call("submit-delivery", map[string]any{"run_binding_id": implementation, "snapshot": map[string]string{"summary": "Not granted"}})
	require.NotNil(t, generalSubmission.Error)
	require.Equal(t, "forbidden", generalSubmission.Error.Code)
	// Simulate a genuine ownership handoff between the context read and the host's submission.
	oldBinding := prepare("delivery-rebinding", "coordination", commit)
	rebindScope := storage.MailScope{EnvironmentID: "local", ThreadID: "thread-delivery-rebinding", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	oldContext := call("delivery-self-context", map[string]any{"scope": rebindScope})
	require.Nil(t, oldContext.Error)
	oldDispatch, err := s.ReadRunDispatch(ctx, oldBinding)
	require.NoError(t, err)
	require.NoError(t, s.ResolveRunDispatch(ctx, oldBinding, oldDispatch.Admission.ID, human.ID, "stopped_writing", "Synthetic handoff after HEAD read"))
	create("delivery-replacement")
	replacement, err := s.PrepareRun(ctx, "delivery-replacement", "C:/original-replacement-workspace")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, replacement, "local", rebindScope.ThreadID))
	staleSubmission := call("delivery-submit-self", map[string]any{"scope": rebindScope, "expectedRunId": oldBinding, "summary": "Must not attach the old observation to a new run", "head": storage.DeliveryHead{IsRepo: true, CommitSHA: &commit}})
	require.NotNil(t, staleSubmission.Error)
	require.Equal(t, "conflict", staleSubmission.Error.Code)
	for _, isRepo := range []bool{false, true} {
		unknownHead := call("delivery-submit-self", map[string]any{"scope": rebindScope, "expectedRunId": replacement, "summary": "Synthetic nonrepo or unborn HEAD", "head": storage.DeliveryHead{IsRepo: isRepo}})
		require.Nil(t, unknownHead.Error)
		var receipt storage.DeliverySelfReceipt
		require.NoError(t, json.Unmarshal(unknownHead.Data, &receipt))
		stored, err := s.ReadWorkbenchDelivery(ctx, receipt.DeliveryID)
		require.NoError(t, err)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(stored.Snapshot, &fields))
		var git map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(fields["git"], &git))
		require.Equal(t, "null", string(git["baseline"]))
		require.Contains(t, string(git["head"]), `"commitSha": null`)
		var head storage.DeliveryHead
		require.NoError(t, json.Unmarshal(git["head"], &head))
		require.Equal(t, isRepo, head.IsRepo)
		require.Nil(t, head.CommitSHA)
	}
	for _, head := range []any{map[string]any{}, map[string]any{"isRepo": true}, map[string]any{"commitSha": nil}, storage.DeliveryHead{IsRepo: false, CommitSHA: &commit}} {
		invalidHead := call("delivery-submit-self", map[string]any{"scope": rebindScope, "expectedRunId": replacement, "summary": "Malformed native metadata", "head": head})
		require.NotNil(t, invalidHead.Error)
	}
	fields := storage.TestReportFields{DeliveryID: delivery, CommitSHA: commit, PlanSources: plans, Status: "passed", Command: "synthetic-check", Summary: "Synthetic execution fixture", OutputRefs: []string{"fixture://original-output"}}
	reportBody := func(slug string, fields storage.TestReportFields) any {
		return struct {
			Scope storage.MailScope `json:"scope"`
			storage.TestReportFields
		}{Scope: storage.MailScope{EnvironmentID: "local", ThreadID: "thread-" + slug, ProviderSessionID: "session", ProviderInstanceID: "codex"}, TestReportFields: fields}
	}
	selfScope := storage.MailScope{EnvironmentID: "local", ThreadID: "thread-implementation", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	selfBody := map[string]any{"scope": selfScope}
	blockedSelf := call("run-complete-self", selfBody)
	require.NotNil(t, blockedSelf.Error)
	require.Equal(t, "failed_precondition", blockedSelf.Error.Code)
	require.Contains(t, blockedSelf.Error.Message, storage.ErrImplementationTestsRequired.Error())
	for _, selector := range []string{"runId", "taskSlug", "actor", "project"} {
		forged := call("run-complete-self", map[string]any{"scope": selfScope, selector: "model-chosen"})
		require.NotNil(t, forged.Error)
		require.Equal(t, "invalid_argument", forged.Error.Code)
	}
	unknownScope := selfScope
	unknownScope.ThreadID = "not-a-bound-thread"
	unknownSelf := call("run-complete-self", map[string]any{"scope": unknownScope})
	require.NotNil(t, unknownSelf.Error)
	require.Equal(t, "forbidden", unknownSelf.Error.Code)
	successful := call("test-result-submit", reportBody("implementation", fields))
	require.Nil(t, successful.Error, "implementation baseline is NOT the later tested commit")
	var report storage.TestReport
	require.NoError(t, json.Unmarshal(successful.Data, &report))
	require.Nil(t, report.ExitCode)
	require.Equal(t, implementation, *report.TestRunID)
	require.Equal(t, implementation, report.Reporter)
	require.Contains(t, string(successful.Data), `"exitCode":null`)
	tested := call("test-result-submit", reportBody("tester", fields))
	require.Nil(t, tested.Error)
	require.NoError(t, json.Unmarshal(tested.Data, &report))
	require.Equal(t, tester, *report.TestRunID)
	for _, slug := range []string{"wrong-commit", "wrong-purpose"} {
		response := call("test-result-submit", reportBody(slug, fields))
		require.NotNil(t, response.Error)
	}
	invalid := fields
	invalid.CommitSHA = strings.Repeat("d", 40)
	require.NotNil(t, call("test-result-submit", reportBody("tester", invalid)).Error)
	invalid = fields
	invalid.PlanSources = []storage.ReviewSource{otherPlan}
	require.NotNil(t, call("test-result-submit", reportBody("tester", invalid)).Error)
	crash := int64(3221225477)
	invalid = fields
	invalid.ExitCode = &crash
	require.NotNil(t, call("test-result-submit", reportBody("tester", invalid)).Error)
	invalid.Status = "failed"
	invalid.Summary = "Synthetic Windows access-violation exit"
	failed := call("test-result-submit", reportBody("tester", invalid))
	require.Nil(t, failed.Error)
	require.NoError(t, json.Unmarshal(failed.Data, &report))
	require.Equal(t, crash, *report.ExitCode)
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, crash, *metadata.Evidence[len(metadata.Evidence)-1].ExitCode)
	encoded, _ := json.Marshal(reportBody("tester", fields))
	var forged map[string]any
	require.NoError(t, json.Unmarshal(encoded, &forged))
	forged["testRunId"] = implementation
	require.NotNil(t, call("test-result-submit", forged).Error, "Agent cannot choose its run")
	_, err = s.EnsureProject(ctx, "test-reports-other")
	require.NoError(t, err)
	project = "test-reports-other"
	foreignContext := call("delivery-self-context", map[string]any{"scope": deliveryScope})
	require.NotNil(t, foreignContext.Error)
	foreignSubmission := call("delivery-submit-self", deliveryBody)
	require.NotNil(t, foreignSubmission.Error)
	require.NotNil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	foreignSelf := call("run-complete-self", selfBody)
	require.NotNil(t, foreignSelf.Error)
	require.Equal(t, "forbidden", foreignSelf.Error.Code)
	project = "test-reports"
	// Paging uses recorded database order; no command output is copied into these reports.
	for i := 0; i < 49; i++ {
		fields.Status = "not_run"
		fields.Command = ""
		fields.Summary = "Synthetic pending fixture"
		fields.OutputRefs = []string{}
		require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	}
	readBody := map[string]string{"environment_id": "local", "native_project_id": "native-project", "deliveryId": delivery}
	pageResponse := call("test-results", readBody)
	require.Nil(t, pageResponse.Error)
	var page storage.TestReportPage
	require.NoError(t, json.Unmarshal(pageResponse.Data, &page))
	require.Len(t, page.Reports, 50)
	require.True(t, page.HasMore)
	require.NotNil(t, page.NextCursor)
	readBody["cursor"] = *page.NextCursor
	next := call("test-results", readBody)
	require.Nil(t, next.Error)
	require.NoError(t, json.Unmarshal(next.Data, &page))
	require.Len(t, page.Reports, 2)
	require.False(t, page.HasMore)
	require.Nil(t, page.NextCursor)
	require.Equal(t, "passed", page.Reports[0].Status)
	state, err := s.GetSpec(ctx, "implementation")
	require.NoError(t, err)
	require.NotEqual(t, storage.SpecStageDone, state.Stage, "recording reports never completes work")
	// Neither the old accepted-delivery opinion nor a source approval substitutes for tests.
	_, err = s.ReviewDelivery(operator, delivery, "", "accepted", "Synthetic opinion, not a test result")
	require.NoError(t, err)
	opinion, err := s.AssignReview(operator, &storage.AssignReviewRequest{TaskSlug: "implementation", Kind: "requirements", Sources: plans, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer}, nil)
	require.NoError(t, err)
	_, err = s.ReviewSource(operator, "implementation", opinion.Reviews[0].Request.ID, "accepted", "Source opinion is separate from execution reports")
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrImplementationTestsRequired)
	fields.Status = "passed"
	fields.Command = "synthetic-check"
	fields.Summary = "Synthetic explicit pass"
	fields.OutputRefs = []string{"fixture://updated-output"}
	fields.PlanSources = plans[:1]
	require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrImplementationTestsRequired, "one plan still has its latest not_run report")
	fields.PlanSources = plans[1:]
	require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	for _, status := range []string{"failed", "not_run", "environment_blocked"} {
		fields.PlanSources = plans[:1]
		fields.Status = status
		fields.Summary = "Synthetic later nonpass outcome"
		require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
		require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrImplementationTestsRequired, "old pass must not hide "+status)
	}
	fields.Status = "passed"
	fields.PlanSources = plans
	require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	newDelivery, err := s.CreateDelivery(ctx, implementation, snapshot, human.ID)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrImplementationTestsRequired, "a new delivery cannot borrow older delivery reports")
	fields.DeliveryID = newDelivery
	fields.PlanSources = plans[:1]
	require.Nil(t, call("test-result-submit", reportBody("tester", fields)).Error)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrImplementationTestsRequired, "new delivery lacks its second plan report")
	manualFields := fields
	manualFields.PlanSources = plans[1:]
	manualBody := storage.RecordTestReportRequest{TestReportFields: manualFields}
	commandSlug = newDelivery
	denied := call("record-test-result", manualBody)
	require.NotNil(t, denied.Error)
	require.Equal(t, "forbidden", denied.Error.Code, "narrow Agent report grant must not grant operator writes")
	credential = adminCredential
	commandSlug = "wrong-delivery"
	mismatch := call("record-test-result", manualBody)
	require.NotNil(t, mismatch.Error)
	require.Equal(t, "invalid_argument", mismatch.Error.Code)
	commandSlug = newDelivery
	manual := call("record-test-result", manualBody)
	require.Nil(t, manual.Error)
	var manualReport storage.TestReport
	require.NoError(t, json.Unmarshal(manual.Data, &manualReport))
	require.Nil(t, manualReport.TestRunID)
	require.Nil(t, manualReport.ExitCode)
	require.Equal(t, human.ID, manualReport.Reporter)
	manualBody.TestRunID = &tester
	associated := call("record-test-result", manualBody)
	require.Nil(t, associated.Error)
	require.NoError(t, json.Unmarshal(associated.Data, &manualReport))
	require.Equal(t, tester, *manualReport.TestRunID)
	require.Equal(t, human.ID, manualReport.Reporter, "operator reporter is distinct from execution association")
	// Reporting never silently renews the implementation owner's lease.
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 hour' WHERE project_slug='test-reports' AND agent=$1`, implementation)
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrAgentNotClaimOwner)
	credential = readerCredential
	commandSlug = ""
	expiredSelf := call("run-complete-self", selfBody)
	require.NotNil(t, expiredSelf.Error)
	require.Equal(t, "conflict", expiredSelf.Error.Code)
	require.Contains(t, expiredSelf.Error.Message, storage.ErrAgentNotClaimOwner.Error())
	_, err = s.ClaimSpec(ctx, "implementation", implementation, 30*time.Minute)
	require.NoError(t, err, "explicit same-owner renewal is separate from reporting")
	maximum := int32(1)
	hold, err := s.AssignReview(operator, &storage.AssignReviewRequest{TaskSlug: "implementation", Kind: "requirements", Sources: plans, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: human.ID}, ReviewerRunID: reviewer, MaxReviewRounds: &maximum}, nil)
	require.NoError(t, err)
	holdID := hold.Reviews[0].Request.ID
	_, err = s.SubmitReview(operator, storage.MailScope{EnvironmentID: "local", ThreadID: "reviewer-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}, holdID, "rejected", "Synthetic hold requires human resolution")
	require.NoError(t, err)
	require.ErrorIs(t, s.RecordCompletion(ctx, "implementation", implementation), storage.ErrReviewHumanHold)
	heldSelf := call("run-complete-self", selfBody)
	require.NotNil(t, heldSelf.Error)
	require.Equal(t, "failed_precondition", heldSelf.Error.Code)
	require.Contains(t, heldSelf.Error.Message, storage.ErrReviewHumanHold.Error())
	_, err = s.ReviewSource(operator, "implementation", holdID, "rejected", "Human resolves the hold without fabricating test success")
	require.NoError(t, err)
	commandSlug = implementation
	operatorDenied := call("complete-run", map[string]any{})
	require.NotNil(t, operatorDenied.Error)
	require.Equal(t, "forbidden", operatorDenied.Error.Code, "self completion grants no general completion selector")
	commandSlug = ""
	completed := call("run-complete-self", selfBody)
	require.Nil(t, completed.Error)
	expected, _ := json.Marshal(map[string]string{"runId": implementation, "completed": "implementation"})
	require.JSONEq(t, string(expected), string(completed.Data))
	state, err = s.GetSpec(ctx, "implementation")
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageDone, state.Stage)
	credential = readerCredential
	commandSlug = ""
}
