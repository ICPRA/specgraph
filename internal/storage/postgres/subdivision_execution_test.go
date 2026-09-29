// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestSubdivisionLineageDoesNotReplaceExecutionChecks(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("subdivision-execution"))
	require.NoError(t, s.SetProjectManaged(ctx, "subdivision-execution", true))
	parent, err := s.CreateSpec(ctx, "parent", "Parent goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, s.StoreSpecifyOutput(ctx, parent.Slug, &storage.SpecifyOutput{Invariants: []string{"Inherited constraint"}}))
	approved := "approved"
	parent, err = s.UpdateSpec(ctx, parent.Slug, nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	// Ordinary coordination exercises lineage and execution guards, not authoring review.
	parentTarget := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this lifecycle fixture.","environmentId":"env","threadId":"parent-thread","workspace":"parent-workspace","createCommandId":"parent-create","startCommandId":"parent-start","messageId":"parent-message"}`)
	parentRun, _, err := s.PrepareRunForOperator(ctx, parent.Slug, "parent-workspace", "operator", "parent-prepare", parentTarget)
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, parentRun, "env", "parent-thread"))
	parentContext, err := s.ReadRunContext(ctx, parentRun)
	require.NoError(t, err)
	parentAdmission, err := s.AuthorizeRunDispatch(ctx, parentRun, "operator", parentContext.PackageID, parentTarget)
	require.NoError(t, err)
	require.NoError(t, s.RecordProgress(ctx, parent.Slug, parentRun, "Actual historical parent event"))
	_, err = s.SubdivideSpec(ctx, parent.Slug, "operator", storage.SubdivideRequest{
		ExpectedVersion: parent.Version, Reason: "Separate work",
		Children: []storage.ChildSpecDraft{{Slug: "child", Intent: "Child goal", Priority: "p2", Complexity: "low"}},
	})
	require.NoError(t, err)
	parentClaim, err := s.GetActiveClaim(ctx, parent.Slug)
	require.NoError(t, err)
	require.Equal(t, parentRun, parentClaim.Agent)
	parentDispatch, err := s.ReadRunDispatch(ctx, parentRun)
	require.NoError(t, err)
	require.Equal(t, parentAdmission.ID, parentDispatch.Admission.ID)
	require.Nil(t, parentDispatch.Resolution)
	preserved, err := s.ReadRunContext(ctx, parentRun)
	require.NoError(t, err)
	require.Equal(t, parentContext, preserved)
	_, err = s.PrepareRun(ctx, parent.Slug, "new-parent-workspace")
	require.ErrorIs(t, err, storage.ErrSummaryNotExecutable)
	_, err = s.PrepareRun(ctx, "child", "child-workspace")
	require.ErrorIs(t, err, storage.ErrSpecNotApproved)
	child, err := s.GetSpec(ctx, "child")
	require.NoError(t, err)
	_, err = s.ApproveWorkbenchNode(ctx, child.Slug, "operator", child.Version, "Approved concrete child work")
	require.NoError(t, err, "lineage is not an approval or handoff gate")
	_, err = s.CreateSpec(ctx, "prerequisite", "Required work", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, child.Slug, "prerequisite", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	_, err = s.PrepareRun(ctx, child.Slug, "child-workspace")
	require.ErrorIs(t, err, storage.ErrDependenciesNotReady)
	ready, err := s.GetReady(ctx)
	require.NoError(t, err)
	require.Empty(t, ready)
	done := "done"
	_, err = s.UpdateSpec(ctx, "prerequisite", nil, &done, nil, nil, nil)
	require.NoError(t, err)
	ready, err = s.GetReady(ctx)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	require.Equal(t, child.Slug, ready[0].Slug)
	childTarget := json.RawMessage(`{"workPurpose":"coordination","purposeGuidance":"Coordinate work for this lifecycle fixture.","environmentId":"env","threadId":"child-thread","workspace":"child-workspace","createCommandId":"child-create","startCommandId":"child-start","messageId":"child-message"}`)
	childRun, _, err := s.PrepareRunForOperator(ctx, child.Slug, "child-workspace", "operator", "child-prepare", childTarget)
	require.NoError(t, err, "the parent run and history do not block approved child work")
	_, err = s.PrepareRun(ctx, child.Slug, "duplicate")
	require.ErrorIs(t, err, storage.ErrSpecAlreadyClaimed)
	childContext, err := s.ReadRunContext(ctx, childRun)
	require.NoError(t, err)
	var contents struct {
		Scope storage.ScopeContext `json:"scope_context"`
	}
	require.NoError(t, json.Unmarshal(childContext.Body, &contents))
	require.Len(t, contents.Scope.Sources, 2)
	require.Contains(t, string(childContext.Body), "Inherited constraint")
	require.NotContains(t, string(childContext.Body), "scope_reviews")
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, childRun, "env", "child-thread"))
	_, err = s.AuthorizeRunDispatch(ctx, childRun, "operator", childContext.PackageID, childTarget)
	require.NoError(t, err)
	require.NoError(t, s.StoreSpecifyOutput(ctx, child.Slug, &storage.SpecifyOutput{Invariants: []string{"Changed after preparation"}}))
	require.ErrorIs(t, s.RecordCompletion(ctx, child.Slug, childRun), storage.ErrCompletionRequiresRequirementReview)
	_, err = s.Pool().Exec(ctx, `UPDATE claims SET lease_expires=now()-interval '1 minute' WHERE project_slug='subdivision-execution' AND spec_slug='child'`)
	require.NoError(t, err)
	_, err = s.PrepareRun(ctx, child.Slug, "duplicate-after-expiry")
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
}
