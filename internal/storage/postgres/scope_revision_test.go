// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestScopeRevisionTracksContractNotStageOrGenericVersion(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("scope-revision"))
	spec, err := s.CreateSpec(ctx, "task", "Original goal", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	readRevision := func() int64 {
		t.Helper()
		var revision int64
		require.NoError(t, s.Pool().QueryRow(ctx, `SELECT scope_revision FROM specs WHERE project_slug='scope-revision' AND slug='task'`).Scan(&revision))
		return revision
	}
	require.EqualValues(t, 1, readRevision())
	stage := "approved"
	spec, err = s.UpdateSpec(ctx, spec.Slug, nil, &stage, nil, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, readRevision(), "stage is not an approved-scope change")
	version := spec.Version
	output := &storage.SpecifyOutput{Invariants: []string{"Keep original history"}}
	require.NoError(t, s.StoreSpecifyOutput(ctx, spec.Slug, output))
	after, err := s.GetSpec(ctx, spec.Slug)
	require.NoError(t, err)
	require.Equal(t, version, after.Version, "native authoring does not advance generic version")
	require.EqualValues(t, 2, readRevision())
	require.NoError(t, s.StoreSpecifyOutput(ctx, spec.Slug, output))
	require.EqualValues(t, 2, readRevision(), "same content is not a change")
	output.Invariants = []string{"Keep history and provenance"}
	require.NoError(t, s.StoreSpecifyOutput(ctx, spec.Slug, output))
	require.EqualValues(t, 3, readRevision())
	output.Invariants = []string{"Keep original history"}
	require.NoError(t, s.StoreSpecifyOutput(ctx, spec.Slug, output))
	require.EqualValues(t, 4, readRevision(), "restoring old text still records another content change")

	// Exercise imported empty representations, not just the typed writer.
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET specify_output='{"invariants":["Keep original history"],"verify_criteria":[]}' WHERE project_slug='scope-revision' AND slug='task'`)
	require.NoError(t, err)
	require.EqualValues(t, 4, readRevision())
	require.NoError(t, s.StoreSpecifyOutput(ctx, spec.Slug, output))
	require.EqualValues(t, 4, readRevision())
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET scope_revision=1 WHERE project_slug='scope-revision' AND slug='task'`)
	require.NoError(t, err)
	require.EqualValues(t, 4, readRevision(), "ordinary writes cannot rewind the counter")
	intent := "Revised goal"
	_, err = s.UpdateSpec(ctx, spec.Slug, &intent, nil, nil, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 5, readRevision())
}

func TestSubdivisionReadsActualScopeContentSeparatelyFromHistory(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("scope-read"))
	parent, err := s.CreateSpec(ctx, "parent", "Original parent", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	division, err := s.SubdivideSpec(ctx, parent.Slug, "operator", storage.SubdivideRequest{
		ExpectedVersion: parent.Version, Reason: "Separate work",
		Children: []storage.ChildSpecDraft{
			{Slug: "a", Intent: "Task A", Priority: "p2", Complexity: "low"},
			{Slug: "b", Intent: "Task B", Priority: "p2", Complexity: "low"},
		},
	})
	require.NoError(t, err)
	before, err := s.ReadSubdivision(ctx, division.ID)
	require.NoError(t, err)
	require.Len(t, before.ParentScope.Sources, 1)
	require.Equal(t, "1", before.ParentScope.Sources[0].Revision)
	require.JSONEq(t, `{"intent":"Original parent","shape":{},"specify":{}}`, string(before.ParentScope.Sources[0].Contract))
	require.Len(t, before.Children, 2)
	require.NoError(t, s.StoreSpecifyOutput(ctx, "a", &storage.SpecifyOutput{Invariants: []string{"Preserve history"}}))
	require.NoError(t, s.StoreShapeOutput(ctx, "parent", &storage.ShapeOutput{ScopeOut: []string{"No production deletion"}}))
	after, err := s.ReadSubdivision(ctx, division.ID)
	require.NoError(t, err)
	require.Equal(t, "2", after.ParentScope.Sources[0].Revision)
	require.JSONEq(t, `{"intent":"Original parent","shape":{"scope_out":["No production deletion"]},"specify":{}}`, string(after.ParentScope.Sources[0].Contract))
	require.Equal(t, before.SourceReference, after.SourceReference)
	require.Equal(t, parent.Version, after.SourceReference.Version)
	require.Equal(t, "a", after.Children[0].Spec.Slug)
	require.Equal(t, "2", after.Children[0].Scope.Sources[0].Revision)
	require.JSONEq(t, `{"intent":"Task A","shape":{},"specify":{"invariants":["Preserve history"]}}`, string(after.Children[0].Scope.Sources[0].Contract))
	require.Equal(t, before.Children[1].Scope.Sources[0], after.Children[1].Scope.Sources[0], "B's own content did not change")
	require.Equal(t, after.ParentScope.Sources[0], after.Children[1].Scope.Sources[1], "B observes the current parent constraint separately")
	require.Equal(t, storage.SpecStageSpark, after.Children[0].Spec.Stage)
	require.Equal(t, storage.SpecStageSpark, after.Children[1].Spec.Stage, "observing content does not approve any child")

	for _, slug := range []string{"root", "left", "right"} {
		_, err = s.CreateSpec(ctx, slug, slug+" constraint", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	for _, pair := range [][2]string{{"root", "left"}, {"root", "right"}, {"left", "parent"}, {"right", "parent"}} {
		_, err = s.AddEdge(ctx, pair[0], pair[1], storage.EdgeTypeComposes)
		require.NoError(t, err)
	}
	_, err = s.StoreDecomposeOutput(ctx, "root", &storage.DecomposeOutput{Strategy: storage.StrategyVerticalSlice, Slices: []storage.DecomposeSlice{{ID: "native", Intent: "Native slice"}}})
	require.NoError(t, err)
	require.NoError(t, s.StoreShapeOutput(ctx, "root", &storage.ShapeOutput{Decisions: []storage.DecisionInput{{Slug: "architecture", Title: "Storage", Body: "Original choice", Rationale: "Original reason"}}}))
	_, err = s.Pool().Exec(ctx, `UPDATE decisions SET body='Current authoritative choice',version=version+1 WHERE project_slug='scope-read' AND slug='architecture'`)
	require.NoError(t, err)
	inherited, err := s.ReadSubdivision(ctx, division.ID)
	require.NoError(t, err)
	require.Len(t, inherited.ParentScope.Sources, 4, "diamond ancestor appears once; native slice is not an ancestor")
	require.Len(t, inherited.Children[0].Scope.Sources, 5)
	require.Len(t, inherited.Children[0].Scope.Relations, 6, "four ancestry edges, one child edge and one decision link")
	require.Len(t, inherited.Children[0].Scope.Decisions, 1)
	require.Equal(t, "Current authoritative choice", inherited.Children[0].Scope.Decisions[0].Body, "do not use stale embedded Shape decision")
	_, err = s.AddEdge(ctx, "parent", "root", storage.EdgeTypeComposes)
	require.NoError(t, err)
	cycled, err := s.ReadSubdivision(ctx, division.ID)
	require.NoError(t, err)
	require.Len(t, cycled.Children[0].Scope.Sources, 5, "a cycle must terminate without enumerating paths")
	require.Len(t, cycled.Children[0].Scope.Relations, 7, "preserve the cycle for review; do not claim executable order")
	require.NoError(t, s.RemoveEdge(ctx, "parent", "a", storage.EdgeTypeComposes))
	detached, err := s.ReadSubdivision(ctx, division.ID)
	require.NoError(t, err)
	require.Len(t, detached.Children[0].Scope.Sources, 1, "current edges, not immutable lineage, determine inherited context")
	require.Empty(t, detached.Children[0].Scope.Decisions)
	require.False(t, detached.Children[0].Attached)
	_, err = s.Pool().Exec(ctx, `INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES ('scope-read','root','missing-decision','DECIDED_IN')`)
	require.NoError(t, err)
	_, err = s.ReadSubdivision(ctx, division.ID)
	require.ErrorIs(t, err, storage.ErrDecisionNotFound, "missing linked constraints must not silently disappear")
}
