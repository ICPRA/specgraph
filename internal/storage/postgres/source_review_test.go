// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

// Q7/Q8: source review precedes delivery, preserves independent identity and unresolved rounds.
func TestSourceReviewLifecycle(t *testing.T) {
	s := newStore(t, postgres.WithProject("source-review"))
	identityStore, err := postgres.NewAuth(context.Background(), s.Pool())
	require.NoError(t, err)
	human, err := identityStore.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: "Source review operator", Role: "admin"}, nil)
	require.NoError(t, err)
	ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	runs := map[string]string{}
	for _, slug := range []string{"author", "reviewer", "manager", "other-reviewer"} {
		_, err = s.CreateSpec(ctx, slug, "Review fixture requirement", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		approved := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		runs[slug], err = s.PrepareRun(ctx, slug, "C:/review-fixture/"+slug)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, runs[slug], "local", "thread-"+slug))
	}
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}','{"assignmentRole":"manager"}'::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, runs["manager"])
	require.NoError(t, err)
	scope := func(name string) storage.MailScope {
		return storage.MailScope{EnvironmentID: "local", ThreadID: "thread-" + name, ProviderSessionID: "session", ProviderInstanceID: "codex"}
	}
	manager := scope("manager")
	refs, err := s.ReadSpecSourceRefs(ctx, "author")
	require.NoError(t, err)
	req := storage.AssignReviewRequest{TaskSlug: "author", Kind: "requirements", Sources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "author", Field: "intent", ChangeID: refs["intent"]}}, AuthorResponsibility: storage.ReviewAuthor{Kind: "agent", RunID: runs["author"]}, ReviewerRunID: runs["reviewer"]}
	invalid := req
	invalid.ReviewerRunID = runs["author"]
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrReviewSelfReview)
	reviewerScope := scope("reviewer")
	_, err = s.AssignReview(ctx, &req, &reviewerScope)
	require.ErrorIs(t, err, storage.ErrReviewForbidden)
	invalid = req
	invalid.AuthorResponsibility = storage.ReviewAuthor{}
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	maximum := int32(1)
	invalid = req
	invalid.MaxReviewRounds = &maximum
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	invalid = req
	invalid.Kind = "design"
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	initial, err := s.GetSpec(ctx, "author")
	require.NoError(t, err)
	claim, err := s.GetActiveClaim(ctx, "author")
	require.NoError(t, err)
	state, err := s.AssignReview(ctx, &req, &manager)
	require.NoError(t, err)
	require.Nil(t, req.RequirementDecisionIDs)
	require.Len(t, state.Reviews, 2)
	require.Nil(t, state.Reviews[1].Request)
	requestID := state.Reviews[0].Request.ID
	require.Empty(t, state.Reviews[0].Request.Decisions)
	require.Equal(t, human.ID, state.Reviews[0].Request.CreatedBy)
	_, err = s.SubmitReview(ctx, scope("author"), requestID, "accepted", "self review")
	require.ErrorIs(t, err, storage.ErrReviewForbidden)
	accepted, err := s.SubmitReview(ctx, scope("reviewer"), requestID, "accepted", "Requirement and acceptance criteria reviewed")
	require.NoError(t, err)
	require.True(t, accepted.Recorded)
	require.Equal(t, "agent", accepted.Decision.ActorKind)
	require.Equal(t, runs["reviewer"], accepted.Decision.Actor)
	_, err = s.SubmitReview(ctx, scope("reviewer"), requestID, "accepted", "duplicate")
	require.ErrorIs(t, err, storage.ErrReviewAlreadyDecided)
	var count int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE project_slug='source-review'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM acceptances WHERE project_slug='source-review'`).Scan(&count))
	require.Zero(t, count)
	after, err := s.GetSpec(ctx, "author")
	require.NoError(t, err)
	require.Equal(t, initial, after)
	afterClaim, err := s.GetActiveClaim(ctx, "author")
	require.NoError(t, err)
	require.Equal(t, claim, afterClaim)
	events, err := s.GetExecutionEvents(ctx, "author", 0)
	require.NoError(t, err)
	require.Empty(t, events)
	design := req
	_, err = s.ReviewSource(ctx, "author", requestID, "accepted", "Human reaffirms the same immutable requirements")
	require.NoError(t, err)
	design.Kind = "design"
	design.RequirementDecisionIDs = []string{accepted.Decision.ID}
	state, err = s.AssignReview(ctx, &design, &manager)
	require.NoError(t, err)
	require.Equal(t, requestID, state.Reviews[0].Request.ID)
	require.Equal(t, design.RequirementDecisionIDs, state.Reviews[1].Request.RequirementDecisionIDs)
	rejected, err := s.SubmitReview(ctx, scope("reviewer"), state.Reviews[1].Request.ID, "rejected", "Design omits a required boundary")
	require.NoError(t, err)
	require.EqualValues(t, 1, rejected.Status.Reviews[1].AgentRejections)
	require.Zero(t, rejected.Status.Reviews[0].AgentRejections)
	invalid = design
	invalid.RequirementDecisionIDs = []string{rejected.Decision.ID}
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	for round := 2; round <= 3; round++ {
		design.ReviewerRunID = runs["other-reviewer"]
		state, err = s.AssignReview(ctx, &design, &manager)
		require.NoError(t, err)
		result, err := s.SubmitReview(ctx, scope("other-reviewer"), state.Reviews[1].Request.ID, "rejected", "Still unresolved")
		require.NoError(t, err)
		require.EqualValues(t, round, result.Status.Reviews[1].AgentRejections)
	}
	state, err = s.AssignReview(ctx, &design, &manager)
	require.NoError(t, err)
	require.True(t, state.Reviews[1].HumanHold)
	heldID := state.Reviews[1].Request.ID
	_, err = s.SubmitReview(ctx, scope("other-reviewer"), heldID, "accepted", "Cannot override hold")
	require.ErrorIs(t, err, storage.ErrReviewHumanHold)
	// The human-owned credential on the Agent route did not become human intervention.
	require.True(t, state.Reviews[1].HumanHold)
	_, err = s.ReviewSource(ctx, "reviewer", heldID, "accepted", "Wrong node")
	require.ErrorIs(t, err, storage.ErrReviewRequestNotFound)
	intervened, err := s.ReviewSource(ctx, "author", heldID, "rejected", "Human decides the tradeoff before another review")
	require.NoError(t, err)
	require.False(t, intervened.Status.Reviews[1].HumanHold)
	require.Zero(t, intervened.Status.Reviews[1].AgentRejections)
	require.Equal(t, "human", intervened.Decision.ActorKind)
	require.Nil(t, intervened.Decision.ReviewerRunID)
	_, err = s.ReviewSource(ctx, "author", heldID, "accepted", "Revised human determination")
	require.NoError(t, err)
	history, err := s.ReadReviewRequest(ctx, heldID)
	require.NoError(t, err)
	require.Len(t, history.Decisions, 2)
	require.Equal(t, "rejected", history.Decisions[0].Verdict)
	require.Equal(t, "accepted", history.Decisions[1].Verdict)
	prior, err := s.ReadReviewRequest(ctx, rejected.Decision.RequestID)
	require.NoError(t, err)
	require.Len(t, prior.Decisions, 1)
	require.Equal(t, "agent", prior.Decisions[0].ActorKind)
	_, err = s.EnsureProject(ctx, "source-review-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "source-review-other")
	require.NoError(t, err)
	_, err = other.ReadReviewRequest(ctx, heldID)
	require.ErrorIs(t, err, storage.ErrReviewRequestNotFound)
	_, err = other.CreateSpec(ctx, "foreign", "Outside project", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	foreignRefs, err := other.ReadSpecSourceRefs(ctx, "foreign")
	require.NoError(t, err)
	invalid = req
	invalid.Sources = []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "foreign", Field: "intent", ChangeID: foreignRefs["intent"]}}
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	invalid = req
	invalid.Sources = []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "author", Field: "missing", ChangeID: refs["intent"]}}
	_, err = s.AssignReview(ctx, &invalid, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	gitReq := req
	gitReq.Sources = []storage.ReviewSource{{Kind: "git", EnvironmentID: "local", RepositoryRoot: "C:/review-fixture/author", CommitSHA: strings.Repeat("a", 40), Path: "docs/requirements.md"}}
	_, err = s.AssignReview(ctx, &gitReq, &manager)
	require.NoError(t, err, "valid Git metadata is not a content-read assertion")
	gitReq.Sources[0].Path = "../outside.md"
	_, err = s.AssignReview(ctx, &gitReq, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	gitReq.Sources[0].Path = "docs/requirements.md"
	gitReq.Sources[0].RepositoryRoot = "C:/unregistered"
	_, err = s.AssignReview(ctx, &gitReq, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	humanReq := req
	pending, err := s.AssignReview(ctx, &design, &manager)
	require.NoError(t, err)
	pendingID := pending.Reviews[1].Request.ID
	_, err = s.ReviewSource(ctx, "author", requestID, "rejected", "Human overrides the earlier requirements approval")
	require.NoError(t, err)
	_, err = s.SubmitReview(ctx, scope("other-reviewer"), pendingID, "accepted", "Cannot approve against overridden requirements")
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	_, err = s.ReviewSource(ctx, "author", pendingID, "accepted", "Human also needs valid requirements")
	require.ErrorIs(t, err, storage.ErrInvalidReview)
	_, err = s.SubmitReview(ctx, scope("other-reviewer"), pendingID, "rejected", "Requirements need reconsideration")
	require.NoError(t, err, "rejection findings remain recordable")
	_, err = s.AssignReview(ctx, &design, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview, "an overridden accepted decision cannot authorize a design review")
	reaccepted, err := s.ReviewSource(ctx, "author", requestID, "accepted", "New explicit approval after rejection")
	require.NoError(t, err)
	_, err = s.AssignReview(ctx, &design, &manager)
	require.ErrorIs(t, err, storage.ErrInvalidReview, "new approval must not resurrect the explicitly revoked older ID")
	newDesign := design
	newDesign.RequirementDecisionIDs = []string{reaccepted.Decision.ID}
	_, err = s.AssignReview(ctx, &newDesign, &manager)
	require.NoError(t, err, "new approval can be explicitly selected")
	humanReq.AuthorResponsibility = storage.ReviewAuthor{Kind: "human", UserID: human.ID}
	_, err = s.AssignReview(ctx, &humanReq, &manager)
	require.NoError(t, err)
	// Different runs in the same native conversation are still the same reviewer identity.
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='stopped',thread_ref='thread-reviewer' WHERE id=$1`, runs["author"])
	require.NoError(t, err)
	_, err = s.AssignReview(ctx, &req, &manager)
	require.ErrorIs(t, err, storage.ErrReviewSelfReview)
}
