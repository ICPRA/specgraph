// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestAuthoringSourceCompletion(t *testing.T) {
	s := newStore(t, postgres.WithProject("authoring-completion"))
	users, err := postgres.NewAuth(context.Background(), s.Pool())
	require.NoError(t, err)
	user, err := users.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: "Authoring operator", Role: "admin"}, nil)
	require.NoError(t, err)
	ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: user.ID, UserKind: storage.KindHuman})
	create := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Initial input for "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	create("reviewer")
	reviewer, err := s.PrepareRun(ctx, "reviewer", "C:/authoring-reviewer")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, reviewer, "local", "reviewer-thread"))
	scope := storage.MailScope{EnvironmentID: "local", ThreadID: "reviewer-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	requirementSource := create("approved-requirements")
	requirements, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: "approved-requirements", Kind: "requirements", Sources: []storage.ReviewSource{requirementSource}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: user.ID}, ReviewerRunID: reviewer}, nil)
	require.NoError(t, err)
	requirement, err := s.ReviewSource(ctx, "approved-requirements", requirements.Reviews[0].Request.ID, "accepted", "Approved input requirements")
	require.NoError(t, err)
	designSource := create("approved-design")
	designs, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: "approved-design", Kind: "design", Sources: []storage.ReviewSource{designSource}, RequirementDecisionIDs: []string{requirement.Decision.ID}, AuthorResponsibility: storage.ReviewAuthor{Kind: "human", UserID: user.ID}, ReviewerRunID: reviewer}, nil)
	require.NoError(t, err)
	design, err := s.ReviewSource(ctx, "approved-design", designs.Reviews[1].Request.ID, "accepted", "Approved design")
	require.NoError(t, err)
	plan := create("test-plan")
	type author struct {
		slug, run, admission string
		request              storage.AssignReviewRequest
	}
	prepare := func(slug, purpose string) author {
		t.Helper()
		create(slug)
		create("review-" + slug)
		target := map[string]any{"environmentId": "local", "threadId": "thread-" + slug, "workspace": "C:/" + slug, "createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug, "workPurpose": purpose, "purposeGuidance": "Write the declared final sources without assuming review approval."}
		if purpose == "design" || purpose == "implementation" {
			basis := storage.DispatchQABasis{RequirementDecisionIDs: []string{requirement.Decision.ID}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{}, Applicability: "Applies to this authoring fixture"}
			if purpose == "implementation" {
				basis.DesignDecisionIDs = []string{design.Decision.ID}
				basis.TestPlanSources = []storage.ReviewSource{plan}
			}
			target["qaBasis"] = basis
		}
		body, err := json.Marshal(target)
		require.NoError(t, err)
		run, _, err := s.PrepareRunForOperator(ctx, slug, "C:/"+slug, user.ID, "prepare-"+slug, body)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "thread-"+slug))
		pkg, err := s.ReadRunContext(ctx, run)
		require.NoError(t, err)
		admission, err := s.AuthorizeRunDispatch(ctx, run, user.ID, pkg.PackageID, body)
		require.NoError(t, err)
		output := "Final authored content for " + slug
		_, err = s.UpdateSpec(ctx, slug, &output, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NoError(t, s.StoreSpecifyOutput(ctx, slug, &storage.SpecifyOutput{Invariants: []string{"Observable reviewed behavior"}}))
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		kind := purpose
		if kind == "implementation" {
			kind = "requirements"
		}
		req := storage.AssignReviewRequest{TaskSlug: "review-" + slug, Kind: kind, Sources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}, {Kind: "specgraph", SpecSlug: slug, Field: "specify_output", ChangeID: refs["specify_output"]}}, AuthorResponsibility: storage.ReviewAuthor{Kind: "agent", RunID: run}, ReviewerRunID: reviewer, CompletionRunID: &run}
		if kind == "design" {
			req.RequirementDecisionIDs = []string{requirement.Decision.ID}
		}
		return author{slug: slug, run: run, admission: admission.ID, request: req}
	}
	assign := func(a author) string {
		t.Helper()
		state, err := s.AssignReview(ctx, &a.request, nil)
		require.NoError(t, err)
		index := 0
		if a.request.Kind == "design" {
			index = 1
		}
		require.Equal(t, a.run, *state.Reviews[index].Request.CompletionRunID)
		spec, err := s.GetSpec(ctx, a.slug)
		require.NoError(t, err)
		require.Equal(t, storage.SpecStageReview, spec.Stage)
		return state.Reviews[index].Request.ID
	}
	assertPending := func(a author) {
		t.Helper()
		spec, err := s.GetSpec(ctx, a.slug)
		require.NoError(t, err)
		require.Equal(t, storage.SpecStageReview, spec.Stage)
		events, err := s.GetExecutionEvents(ctx, a.slug, 0)
		require.NoError(t, err)
		require.Empty(t, events)
	}
	for _, kind := range []string{"requirements", "design"} {
		t.Run(kind, func(t *testing.T) {
			a := prepare("success-"+kind, kind)
			_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 hour' WHERE project_slug='authoring-completion' AND agent=$1`, a.run)
			require.NoError(t, err)
			id := assign(a)
			if kind == "design" {
				require.NoError(t, s.ConfirmRunStopped(ctx, a.run, a.admission, "local", "thread-"+a.slug, user.ID, "Observed author stopped after submission"))
			}
			result, err := s.SubmitReview(ctx, scope, id, "accepted", "Whole source set reviewed")
			require.NoError(t, err)
			require.Equal(t, "completed", result.AuthoringCompletion.Status)
			require.Equal(t, a.slug, result.AuthoringCompletion.TaskSlug)
			spec, err := s.GetSpec(ctx, a.slug)
			require.NoError(t, err)
			require.Equal(t, storage.SpecStageDone, spec.Stage)
			reviewTask, err := s.GetSpec(ctx, a.request.TaskSlug)
			require.NoError(t, err)
			require.NotEqual(t, storage.SpecStageDone, reviewTask.Stage)
			events, err := s.GetExecutionEvents(ctx, a.slug, 0)
			require.NoError(t, err)
			require.Len(t, events, 1)
			repeated, err := s.ReviewSource(ctx, a.request.TaskSlug, id, "accepted", "Human confirms the same completed submission")
			require.NoError(t, err)
			require.Equal(t, "completed", repeated.AuthoringCompletion.Status)
			events, err = s.GetExecutionEvents(ctx, a.slug, 0)
			require.NoError(t, err)
			require.Len(t, events, 1, "same completed source submission must not create another event")
			claim, err := s.GetActiveClaim(ctx, a.slug)
			require.NoError(t, err)
			require.Nil(t, claim)
			status, err := s.ReadRunDispatch(ctx, a.run)
			require.NoError(t, err)
			if kind == "requirements" {
				require.Nil(t, status.Resolution)
				require.Nil(t, status.StopConfirmation)
			}
			_, err = s.AssignReview(ctx, &a.request, nil)
			require.Error(t, err, "terminal authoring node cannot be overwritten")
		})
	}
	t.Run("fact-only", func(t *testing.T) {
		a := prepare("fact-only", "requirements")
		a.request.CompletionRunID = nil
		state, err := s.AssignReview(ctx, &a.request, nil)
		require.NoError(t, err)
		result, err := s.SubmitReview(ctx, scope, state.Reviews[0].Request.ID, "accepted", "Opinion only without complete submission declaration")
		require.NoError(t, err)
		require.Nil(t, result.AuthoringCompletion)
		spec, err := s.GetSpec(ctx, a.slug)
		require.NoError(t, err)
		require.NotEqual(t, storage.SpecStageDone, spec.Stage)
		require.Error(t, s.RecordCompletion(ctx, a.slug, a.run))
	})
	t.Run("invalid-owner", func(t *testing.T) {
		implementation := prepare("implementation", "implementation")
		_, err = s.AssignReview(ctx, &implementation.request, nil)
		require.ErrorIs(t, err, storage.ErrInvalidReview)
		a := prepare("wrong-author", "requirements")
		a.request.AuthorResponsibility.RunID = reviewer
		_, err = s.AssignReview(ctx, &a.request, nil)
		require.Error(t, err)
		a = prepare("already-released", "requirements")
		require.NoError(t, s.ResolveRunDispatch(ctx, a.run, a.admission, user.ID, "stopped_writing", "Released before any submission"))
		_, err = s.AssignReview(ctx, &a.request, nil)
		require.ErrorIs(t, err, storage.ErrRunBindingConflict)
	})
	t.Run("new-submission", func(t *testing.T) {
		a := prepare("reassigned", "requirements")
		old := assign(a)
		current := assign(a)
		require.NotEqual(t, old, current)
		historical, err := s.ReviewSource(ctx, a.request.TaskSlug, old, "accepted", "Historic original request opinion")
		require.NoError(t, err)
		require.Equal(t, "failed", historical.AuthoringCompletion.Status)
		assertPending(a)
		accepted, err := s.SubmitReview(ctx, scope, current, "accepted", "Current full submission")
		require.NoError(t, err)
		require.Equal(t, "completed", accepted.AuthoringCompletion.Status)
	})
	t.Run("explicit-replacement-run", func(t *testing.T) {
		a := prepare("replacement", "requirements")
		old := assign(a)
		require.NoError(t, s.ResolveRunDispatch(ctx, a.run, a.admission, user.ID, "stopped_writing", "Explicitly hand off rework"))
		approved := "approved"
		_, err = s.UpdateSpec(ctx, a.slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		oldPackage, err := s.ReadRunContext(ctx, a.run)
		require.NoError(t, err)
		var packageFields struct {
			Target map[string]any `json:"dispatch_target"`
		}
		require.NoError(t, json.Unmarshal(oldPackage.Body, &packageFields))
		for _, key := range []string{"threadId", "createCommandId", "startCommandId", "messageId"} {
			packageFields.Target[key] = "replacement-" + key
		}
		packageFields.Target["workspace"] = "C:/replacement-next"
		body, err := json.Marshal(packageFields.Target)
		require.NoError(t, err)
		next, _, err := s.PrepareRunForOperator(ctx, a.slug, "C:/replacement-next", user.ID, "replacement-next", body)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, next, "local", "replacement-threadId"))
		pkg, err := s.ReadRunContext(ctx, next)
		require.NoError(t, err)
		_, err = s.AuthorizeRunDispatch(ctx, next, user.ID, pkg.PackageID, body)
		require.NoError(t, err)
		output := "Final output from the explicitly assigned replacement"
		_, err = s.UpdateSpec(ctx, a.slug, &output, nil, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, a.slug)
		require.NoError(t, err)
		a.run = next
		a.request.CompletionRunID = &next
		a.request.AuthorResponsibility.RunID = next
		a.request.Sources[0].ChangeID = refs["intent"]
		current := assign(a)
		history, err := s.ReviewSource(ctx, a.request.TaskSlug, old, "accepted", "Historical source approval cannot close replacement")
		require.NoError(t, err)
		require.Equal(t, "failed", history.AuthoringCompletion.Status)
		assertPending(a)
		result, err := s.SubmitReview(ctx, scope, current, "accepted", "Replacement's full source set reviewed")
		require.NoError(t, err)
		require.Equal(t, "completed", result.AuthoringCompletion.Status)
		require.Equal(t, next, result.AuthoringCompletion.RunID)
	})
	for _, failure := range []string{"unlisted-input", "stale-source", "git-only", "dependency", "competing-owner"} {
		t.Run(failure, func(t *testing.T) {
			a := prepare(failure, "requirements")
			if failure == "git-only" {
				a.request.Sources = []storage.ReviewSource{{Kind: "git", EnvironmentID: "local", RepositoryRoot: "C:/" + a.slug, CommitSHA: strings.Repeat("a", 40), Path: "requirements.md"}}
			}
			id := assign(a)
			switch failure {
			case "unlisted-input":
				require.NoError(t, s.StoreShapeOutput(ctx, a.slug, &storage.ShapeOutput{ChosenApproach: "Changed unsubmitted input"}))
			case "stale-source":
				changed := "Unreviewed later output"
				_, err = s.UpdateSpec(ctx, a.slug, &changed, nil, nil, nil, nil)
				require.NoError(t, err)
			case "dependency":
				create("new-dependency")
				_, err = s.AddEdge(ctx, a.slug, "new-dependency", storage.EdgeTypeDependsOn)
				require.NoError(t, err)
			case "competing-owner":
				require.NoError(t, s.ResolveRunDispatch(ctx, a.run, a.admission, user.ID, "stopped_writing", "Author stopped"))
				_, err = s.ClaimSpec(ctx, a.slug, "another-writer", time.Hour)
				require.NoError(t, err)
			}
			result, err := s.SubmitReview(ctx, scope, id, "accepted", "Source opinion recorded separately from closure")
			require.NoError(t, err)
			require.True(t, result.Recorded)
			require.Equal(t, "failed", result.AuthoringCompletion.Status)
			require.NotEmpty(t, result.AuthoringCompletion.Message)
			assertPending(a)
			original, err := s.ReadReviewRequest(ctx, id)
			require.NoError(t, err)
			require.Len(t, original.Decisions, 1)
			require.Equal(t, "accepted", original.Decisions[0].Verdict)
			require.Error(t, s.RecordCompletion(ctx, a.slug, a.run))
		})
	}
	t.Run("target-hold-and-retry", func(t *testing.T) {
		a := prepare("held-author", "requirements")
		id := assign(a)
		maximum := int32(1)
		hold, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: a.slug, Kind: "requirements", Sources: a.request.Sources, AuthorResponsibility: a.request.AuthorResponsibility, ReviewerRunID: reviewer, MaxReviewRounds: &maximum}, nil)
		require.NoError(t, err)
		holdID := hold.Reviews[0].Request.ID
		rejected, err := s.SubmitReview(ctx, scope, holdID, "rejected", "Owner node needs human intervention")
		require.NoError(t, err)
		require.True(t, rejected.Status.Reviews[0].HumanHold)
		accepted, err := s.SubmitReview(ctx, scope, id, "accepted", "The separate review node accepted its submission")
		require.NoError(t, err)
		require.Equal(t, "failed", accepted.AuthoringCompletion.Status)
		assertPending(a)
		_, err = s.ReviewSource(ctx, a.slug, holdID, "rejected", "Human resolves hold, retaining findings")
		require.NoError(t, err)
		// The original operator completion action retries closure; it does not mint another approval.
		response, err := server.ExecuteWorkbenchCommand(ctx, s, "complete-run", "authoring-completion", a.run, user.ID, json.RawMessage(`{}`))
		require.NoError(t, err)
		require.JSONEq(t, `{"completed":"held-author"}`, string(response))
		original, err := s.ReadReviewRequest(ctx, id)
		require.NoError(t, err)
		require.Len(t, original.Decisions, 1)
	})
}
