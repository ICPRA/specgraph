// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestRequirementChangePreview_StructuralCandidatesAndSharedParents(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("change-preview"))
	for _, slug := range []string{"ancestor", "a", "b", "c", "downstream", "subtask", "dependent", "leaf", "prerequisite", "blocker", "unrelated"} {
		_, err := s.CreateSpec(ctx, slug, slug+" intent", "p2", "medium", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	for _, edge := range []storage.ScopeRelation{
		{From: "ancestor", To: "a", Type: "COMPOSES"},
		{From: "a", To: "c", Type: "COMPOSES"},
		{From: "b", To: "c", Type: "COMPOSES"},
		{From: "c", To: "downstream", Type: "BLOCKS"},
		{From: "downstream", To: "subtask", Type: "COMPOSES"},
		{From: "dependent", To: "subtask", Type: "DEPENDS_ON"},
		{From: "dependent", To: "leaf", Type: "COMPOSES"},
		{From: "leaf", To: "a", Type: "BLOCKS"},
		{From: "c", To: "prerequisite", Type: "DEPENDS_ON"},
		{From: "blocker", To: "c", Type: "BLOCKS"},
		{From: "a", To: "unrelated", Type: "RELATES_TO"},
		{From: "a", To: "leaf", Type: "RELATES_TO"},
		{From: "c", To: "subtask", Type: "SUPERSEDES"},
	} {
		_, err := s.AddEdge(ctx, edge.From, edge.To, storage.EdgeType(edge.Type))
		require.NoError(t, err)
	}
	_, err := s.StoreDecomposeOutput(ctx, "a", &storage.DecomposeOutput{
		Strategy: storage.StrategyVerticalSlice,
		Slices:   []storage.DecomposeSlice{{ID: "native", Intent: "Native slice, not a Spec child"}},
	})
	require.NoError(t, err)
	require.NoError(t, s.StoreShapeOutput(ctx, "a", &storage.ShapeOutput{ScopeOut: []string{"Do not remove shared goals"}}))
	require.NoError(t, s.StoreShapeOutput(ctx, "ancestor", &storage.ShapeOutput{
		Decisions: []storage.DecisionInput{{Slug: "choice", Title: "Choice", Body: "Original decision", Rationale: "Keep provenance"}},
	}))
	require.NoError(t, s.StoreSpecifyOutput(ctx, "ancestor", &storage.SpecifyOutput{Invariants: []string{"Preserve existing history"}}))
	_, err = s.Pool().Exec(ctx, `UPDATE decisions SET body='Current decision',version=version+1 WHERE project_slug='change-preview' AND slug='choice'`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET role='summary',stage='done',version=7 WHERE project_slug='change-preview' AND slug='a'`)
	require.NoError(t, err)
	beforeSpecs, err := s.ListSpecs(ctx, "", "", 0)
	require.NoError(t, err)
	readState := func() string {
		t.Helper()
		var state string
		require.NoError(t, s.Pool().QueryRow(ctx, `SELECT jsonb_build_object(
			'specs',(SELECT jsonb_agg(s ORDER BY to_jsonb(s)::text) FROM specs s WHERE project_slug='change-preview'),
			'edges',(SELECT jsonb_agg(e ORDER BY to_jsonb(e)::text) FROM edges e WHERE project_slug='change-preview'),
			'decisions',(SELECT jsonb_agg(d ORDER BY to_jsonb(d)::text) FROM decisions d WHERE project_slug='change-preview'),
			'slices',(SELECT jsonb_agg(s ORDER BY to_jsonb(s)::text) FROM slices s WHERE project_slug='change-preview'),
			'changes',(SELECT jsonb_agg(c ORDER BY to_jsonb(c)::text) FROM changelog_entries c WHERE project_slug='change-preview')
		)::text`).Scan(&state))
		return state
	}
	before := readState()
	previewCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	preview, err := s.ReadRequirementChangePreview(previewCtx, "a")
	require.NoError(t, err, "mixed-edge cycle must terminate")
	require.Equal(t, "a", preview.SpecSlug)
	var slugs []string
	for _, node := range preview.Nodes {
		slugs = append(slugs, node.Slug)
		for _, spec := range beforeSpecs {
			if spec.Slug == node.Slug {
				require.Equal(t, spec.ID, node.ID)
				require.Equal(t, spec.Intent, node.Intent)
				require.Equal(t, string(spec.Stage), node.Stage)
				require.Equal(t, string(spec.Role), node.Role)
				require.Equal(t, spec.Version, node.Version)
			}
		}
		require.NotNil(t, node.ParentSlugs)
	}
	require.Equal(t, []string{"a", "c", "dependent", "downstream", "leaf", "subtask"}, slugs)
	require.Equal(t, []string{"ancestor"}, preview.Nodes[0].ParentSlugs)
	require.Equal(t, []string{"a", "b"}, preview.Nodes[1].ParentSlugs, "shared B is visible but not automatically affected")
	require.Empty(t, preview.Nodes[2].ParentSlugs)
	require.Equal(t, []storage.ScopeRelation{
		{From: "c", To: "downstream", Type: "BLOCKS"},
		{From: "leaf", To: "a", Type: "BLOCKS"},
		{From: "a", To: "c", Type: "COMPOSES"},
		{From: "ancestor", To: "a", Type: "COMPOSES"},
		{From: "b", To: "c", Type: "COMPOSES"},
		{From: "dependent", To: "leaf", Type: "COMPOSES"},
		{From: "downstream", To: "subtask", Type: "COMPOSES"},
		{From: "dependent", To: "subtask", Type: "DEPENDS_ON"},
	}, preview.Relations)
	require.Len(t, preview.Scope.Sources, 2)
	require.Equal(t, "a", preview.Scope.Sources[0].Slug)
	require.JSONEq(t, `{"intent":"a intent","shape":{"scope_out":["Do not remove shared goals"]},"specify":{}}`, string(preview.Scope.Sources[0].Contract))
	require.Equal(t, "ancestor", preview.Scope.Sources[1].Slug)
	require.Contains(t, string(preview.Scope.Sources[1].Contract), "Preserve existing history")
	require.Len(t, preview.Scope.Decisions, 1)
	require.Equal(t, "Current decision", preview.Scope.Decisions[0].Body)
	require.Equal(t, []storage.ScopeRelation{
		{From: "ancestor", To: "a", Type: "COMPOSES"},
		{From: "ancestor", To: "choice", Type: "DECIDED_IN"},
	}, preview.Scope.Relations)
	repeated, err := s.ReadRequirementChangePreview(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, preview, repeated, "ordering is stable")
	require.JSONEq(t, before, readState(), "preview cannot change authored state or graph/history")
	// Native authoring does not bump the generic version, but must change the
	// baseline used to review a candidate's actual requirement content.
	previousChild := preview.Nodes[1]
	require.NoError(t, s.StoreSpecifyOutput(ctx, "c", &storage.SpecifyOutput{Invariants: []string{"New shared constraint"}}))
	changed, err := s.ReadRequirementChangePreview(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, previousChild.Version, changed.Nodes[1].Version)
	require.NotEqual(t, previousChild.ScopeRevision, changed.Nodes[1].ScopeRevision)
}

func TestRequirementChangePreview_ProjectIsolationAndEmptyArrays(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("change-preview-isolation"))
	other := newStore(t, postgres.WithProject("change-preview-other"))
	_, err := s.CreateSpec(ctx, "solo", "This project", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	for _, slug := range []string{"solo", "foreign"} {
		_, err = other.CreateSpec(ctx, slug, "Other project", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
	}
	_, err = other.AddEdge(ctx, "solo", "foreign", storage.EdgeTypeComposes)
	require.NoError(t, err)
	preview, err := s.ReadRequirementChangePreview(ctx, "solo")
	require.NoError(t, err)
	require.Len(t, preview.Nodes, 1)
	require.Equal(t, "This project", preview.Nodes[0].Intent)
	require.NotNil(t, preview.Relations)
	require.Empty(t, preview.Relations)
	encoded, err := json.Marshal(preview)
	require.NoError(t, err)
	for _, array := range []string{`"parentSlugs":[]`, `"relations":[]`, `"decisions":[]`} {
		require.Contains(t, string(encoded), array)
	}
	for _, slug := range []string{"foreign", "missing"} {
		_, err = s.ReadRequirementChangePreview(ctx, slug)
		require.ErrorIs(t, err, storage.ErrSpecNotFound)
	}
}

func TestRequirementChangePreview_NoDepthTruncation(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("change-preview-depth"))
	for i := range 55 {
		slug := fmt.Sprintf("node-%02d", i)
		_, err := s.CreateSpec(ctx, slug, "Chain fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		if i > 0 {
			_, err = s.AddEdge(ctx, fmt.Sprintf("node-%02d", i-1), slug, storage.EdgeTypeComposes)
			require.NoError(t, err)
		}
	}
	_, err := s.AddEdge(ctx, "node-54", "node-00", storage.EdgeTypeComposes)
	require.NoError(t, err)
	previewCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	preview, err := s.ReadRequirementChangePreview(previewCtx, "node-00")
	require.NoError(t, err)
	require.Len(t, preview.Nodes, 55)
	require.Len(t, preview.Relations, 55)
	require.Len(t, preview.Scope.Sources, 55)
	require.Equal(t, "node-54", preview.Nodes[54].Slug)
}
