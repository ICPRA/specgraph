// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

// Q4/Q7: an opinion is not test execution and must not complete code work.
func TestReviewOpinionDoesNotCompleteWork(t *testing.T) {
	for _, caller := range []string{"human", "agent"} {
		t.Run(caller, func(t *testing.T) {
			project := "review-no-completion-" + caller
			s := newStore(t, postgres.WithProject(project))
			identityStore, err := postgres.NewAuth(context.Background(), s.Pool())
			require.NoError(t, err)
			operator, err := identityStore.CreateHuman(context.Background(), &storage.User{Kind: storage.KindHuman, DisplayName: "review operator", Role: "admin"}, nil)
			require.NoError(t, err)
			ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: operator.ID, UserKind: storage.KindHuman})
			runs := make(map[string]string)
			for _, slug := range []string{"implementation", "reviewer"} {
				_, err := s.CreateSpec(ctx, slug, "Isolated QA fixture", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
				require.NoError(t, err)
				approved := "approved"
				_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
				require.NoError(t, err)
				runs[slug], err = s.PrepareRun(ctx, slug, "workspace-"+slug)
				require.NoError(t, err)
				require.NoError(t, s.BindRunThreadInEnvironment(ctx, runs[slug], "local", "thread-"+slug))
			}
			delivery, err := s.CreateDelivery(ctx, runs["implementation"], json.RawMessage(`{"summary":"No executed tests"}`), "writer")
			require.NoError(t, err)
			refs, err := s.ReadSpecSourceRefs(ctx, "implementation")
			require.NoError(t, err)
			state, err := s.AssignReview(ctx, &storage.AssignReviewRequest{TaskSlug: "implementation", Kind: "requirements", Sources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "implementation", Field: "intent", ChangeID: refs["intent"]}}, AuthorResponsibility: storage.ReviewAuthor{Kind: "agent", RunID: runs["implementation"]}, ReviewerRunID: runs["reviewer"]}, nil)
			require.NoError(t, err)
			before, err := s.GetSpec(ctx, "implementation")
			require.NoError(t, err)
			claim, err := s.GetActiveClaim(ctx, "implementation")
			require.NoError(t, err)
			if caller == "human" {
				result, err := s.ReviewDelivery(ctx, delivery, "fixture", "accepted", "Opinion only; no test results")
				require.NoError(t, err)
				require.True(t, result.Recorded)
				require.Equal(t, "not_requested", result.Completion.Status)
			} else {
				result, err := s.SubmitReview(ctx, storage.MailScope{EnvironmentID: "local", ThreadID: "thread-reviewer", ProviderSessionID: "session", ProviderInstanceID: "codex"}, state.Reviews[0].Request.ID, "accepted", "Opinion only; no test results")
				require.NoError(t, err)
				require.True(t, result.Recorded)
			}
			after, err := s.GetSpec(ctx, "implementation")
			require.NoError(t, err)
			require.Equal(t, before, after)
			afterClaim, err := s.GetActiveClaim(ctx, "implementation")
			require.NoError(t, err)
			require.Equal(t, claim, afterClaim)
			events, err := s.GetExecutionEvents(ctx, "implementation", 0)
			require.NoError(t, err)
			require.Empty(t, events)
		})
	}
}
