// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestCompletionRequiresCurrentRunLatestDelivery(t *testing.T) {
	store := newStore(t, postgres.WithProject("acceptance-owner"))
	ctx := context.Background()
	require.NoError(t, store.SetProjectManaged(ctx, "acceptance-owner", true))
	spec, err := store.CreateSpec(ctx, "acceptance-task", "Acceptance fixture", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	spec, err = store.UpdateSpec(ctx, spec.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	oldRun, err := store.PrepareRun(ctx, spec.Slug, "old")
	require.NoError(t, err)
	oldDelivery, err := store.CreateDelivery(ctx, oldRun, json.RawMessage(`{}`), "writer")
	require.NoError(t, err)
	require.NoError(t, store.InsertAcceptance(ctx, oldDelivery, "fixture", "accepted", "operator", nil))
	require.NoError(t, store.UnclaimSpec(ctx, spec.Slug, oldRun))
	currentRun, err := store.PrepareRun(ctx, spec.Slug, "current")
	require.NoError(t, err)
	require.ErrorIs(t, store.RecordCompletion(ctx, spec.Slug, currentRun), storage.ErrManagedCompletionRequiresAcceptance)
	first, err := store.CreateDelivery(ctx, currentRun, json.RawMessage(`{"candidate":1}`), "writer")
	require.NoError(t, err)
	require.NoError(t, store.InsertAcceptance(ctx, first, "fixture", "accepted", "operator", nil))
	latest, err := store.CreateDelivery(ctx, currentRun, json.RawMessage(`{"candidate":2}`), "writer")
	require.NoError(t, err)
	require.ErrorIs(t, store.RecordCompletion(ctx, spec.Slug, currentRun), storage.ErrManagedCompletionRequiresAcceptance)
	require.NoError(t, store.InsertAcceptance(ctx, latest, "fixture", "accepted", "operator", nil))
	require.NoError(t, store.InsertAcceptance(ctx, latest, "fixture", "rejected", "operator", nil))
	require.ErrorIs(t, store.RecordCompletion(ctx, spec.Slug, currentRun), storage.ErrManagedCompletionRequiresAcceptance)
	unchanged, err := store.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, spec.Version, unchanged.Version)
	claim, err := store.GetActiveClaim(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, currentRun, claim.Agent)
	events, err := store.GetExecutionEvents(ctx, spec.Slug, 0)
	require.NoError(t, err)
	require.Empty(t, events)
	require.NoError(t, store.InsertAcceptance(ctx, latest, "fixture", "accepted", "operator", nil))
	require.NoError(t, store.RecordCompletion(ctx, spec.Slug, currentRun))
	completed, err := store.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, storage.SpecStage("done"), completed.Stage)
	require.Equal(t, spec.Version+1, completed.Version)
	metadata, err := store.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	states := make(map[string]string)
	for _, run := range metadata.Runs {
		states[run.ID] = run.State
	}
	require.Equal(t, "completed", states[currentRun])
	require.Equal(t, "prepared", states[oldRun], "completion must not rewrite previous attempts")
}

func TestSupersededRunCannotAddDeliveryOrRecomplete(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("superseded-common-owner"))
	_, err := s.CreateSpec(ctx, "source", "Original source", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.CreateSpec(ctx, "replacement", "Replacement", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	approved := "approved"
	_, err = s.UpdateSpec(ctx, "source", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	target := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Synthetic original work","environmentId":"local","threadId":"source-thread","workspace":"C:/source","createCommandId":"create-source","startCommandId":"start-source","messageId":"message-source"}`)
	run, _, err := s.PrepareRunForOperator(ctx, "source", "C:/source", "operator", "prepare-source", target)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, run, "local", "source-thread"))
	first, err := s.CreateDelivery(ctx, run, json.RawMessage(`{"candidate":1}`), "operator")
	require.NoError(t, err)
	require.NoError(t, s.RecordCompletion(ctx, "source", run))
	events, err := s.GetExecutionEvents(ctx, "source", 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	_, _, err = s.LifecycleSupersedeSpec(ctx, "source", "replacement", "New source supersedes the completed original")
	require.NoError(t, err)
	_, err = s.CreateDelivery(ctx, run, json.RawMessage(`{"candidate":2}`), "operator")
	require.ErrorIs(t, err, storage.ErrSpecTerminal)
	require.ErrorIs(t, s.RecordCompletion(ctx, "source", run), storage.ErrSpecTerminal)
	current, err := s.GetSpec(ctx, "source")
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageSuperseded, current.Stage)
	after, err := s.GetExecutionEvents(ctx, "source", 0)
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, events[0].ID, after[0].ID)
	page, err := s.ReadNodeDeliveries(ctx, "source", "")
	require.NoError(t, err)
	require.Len(t, page.Deliveries, 1)
	require.Equal(t, first, page.Deliveries[0].ID)
}

func TestCompletionRequiresUnchangedRequirementContent(t *testing.T) {
	for _, name := range []string{"stage-only", "empty-array", "changed-intent", "changed-criteria"} {
		t.Run(name, func(t *testing.T) {
			store := newStore(t, postgres.WithProject("acceptance-"+name))
			ctx := context.Background()
			require.NoError(t, store.SetProjectManaged(ctx, "acceptance-"+name, true))
			spec, err := store.CreateSpec(ctx, "task", "Original requirement", "p1", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			require.NoError(t, store.StoreSpecifyOutput(ctx, spec.Slug, &storage.SpecifyOutput{
				VerifyCriteria: []storage.VerifyCriterion{{Category: "functional", Description: "Original criterion"}},
			}))
			if name == "empty-array" {
				fixture, err := pgx.Connect(ctx, connString)
				require.NoError(t, err)
				defer func() { require.NoError(t, fixture.Close(ctx)) }()
				_, err = fixture.Exec(ctx, `UPDATE specs SET specify_output = '{"verify_criteria":[]}'::jsonb
					WHERE project_slug = $1 AND slug = $2`, "acceptance-"+name, spec.Slug)
				require.NoError(t, err)
			}
			approved := "approved"
			_, err = store.UpdateSpec(ctx, spec.Slug, nil, &approved, nil, nil, nil)
			require.NoError(t, err)
			run, err := store.PrepareRun(ctx, spec.Slug, "workspace")
			require.NoError(t, err)
			delivery, err := store.CreateDelivery(ctx, run, json.RawMessage(`{}`), "writer")
			require.NoError(t, err)
			require.NoError(t, store.InsertAcceptance(ctx, delivery, "fixture", "accepted", "operator", nil))
			review := "review"
			var intent *string
			if name == "changed-intent" {
				value := "Changed requirement"
				intent = &value
			}
			before, err := store.UpdateSpec(ctx, spec.Slug, intent, &review, nil, nil, nil)
			require.NoError(t, err)
			if name == "changed-criteria" {
				require.NoError(t, store.StoreSpecifyOutput(ctx, spec.Slug, &storage.SpecifyOutput{
					VerifyCriteria: []storage.VerifyCriterion{{Category: "functional", Description: "New criterion"}},
				}))
				updated, err := store.GetSpec(ctx, spec.Slug)
				require.NoError(t, err)
				require.Equal(t, before.Version, updated.Version, "authoring output changes must be detected without a version bump")
			}
			err = store.RecordCompletion(ctx, spec.Slug, run)
			if name == "stage-only" || name == "empty-array" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, storage.ErrCompletionRequiresRequirementReview)
			after, err := store.GetSpec(ctx, spec.Slug)
			require.NoError(t, err)
			require.Equal(t, before.Version, after.Version)
			require.Equal(t, storage.SpecStage("review"), after.Stage)
			claim, err := store.GetActiveClaim(ctx, spec.Slug)
			require.NoError(t, err)
			require.Equal(t, run, claim.Agent)
			metadata, err := store.ReadWorkbenchMetadata(ctx)
			require.NoError(t, err)
			require.Len(t, metadata.Runs, 1)
			require.Equal(t, "prepared", metadata.Runs[0].State, "failed completion must not end the execution binding")
		})
	}
}
