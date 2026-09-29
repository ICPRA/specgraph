// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/stretchr/testify/require"
)

func mergeRequest(t *testing.T, f *summaryFixture, sources []string, target string, contexts ...string) storage.MergeRequest {
	t.Helper()
	preview, err := f.s.PreviewNodeMerge(f.ctx, sources, contexts)
	require.NoError(t, err)
	actions := make([]storage.MergeRelationDisposition, 0, len(preview.Relations))
	for _, edge := range preview.Relations {
		actions = append(actions, storage.MergeRelationDisposition{Before: edge, Action: "retain"})
	}
	return storage.MergeRequest{Expected: *preview, Target: storage.MergeTarget{Slug: target, Intent: "Combined explicit requirement for " + target,
		Role: storage.SpecRoleWork, Priority: storage.SpecPriorityP2, Complexity: storage.SpecComplexityLow}, Relations: actions,
		Reason: "Human reviewed source content and every affected relation", IdempotencyKey: "merge-" + target}
}

func TestNodeMergeAtomicDraftBaselineAndReplay(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-simple")
	f.create("a", false, "approved")
	f.create("b", false, "in_progress")
	req := mergeRequest(t, f, []string{"a", "b"}, "c")
	first, err := f.s.MergeNodes(f.ctx, req, nil)
	require.NoError(t, err)
	require.False(t, first.Replayed)
	require.Equal(t, "c", first.Target.Slug)
	require.Len(t, first.Sources, 2)
	for _, source := range first.Sources {
		spec, err := f.s.GetSpec(f.ctx, source.Slug)
		require.NoError(t, err)
		require.Equal(t, storage.SpecStageSuperseded, spec.Stage)
		require.Equal(t, "c", spec.SupersededBy)
	}
	created, err := f.s.GetSpec(f.ctx, "c")
	require.NoError(t, err)
	require.Equal(t, storage.SpecStageSpark, created.Stage)
	require.Equal(t, storage.SpecProvenanceAuthored, created.Provenance)
	require.Equal(t, storage.SpecRoleWork, created.Role)
	prior, err := f.s.MergeNodes(f.ctx, req, nil)
	require.NoError(t, err)
	require.True(t, prior.Replayed)
	require.Equal(t, first.ID, prior.ID)
	req.Target.Intent = "Different content with same key"
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrNodeMergeConflict)
	history, err := f.s.ReadNodeMergeHistory(f.ctx, "a", "")
	require.NoError(t, err)
	require.Len(t, history.Items, 1)
	require.Equal(t, first.ID, history.Items[0].ID)
}

func TestNodeMergeMissingRelationDispositionRollsBack(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-incomplete")
	f.create("a", false, "approved")
	f.create("b", false, "approved")
	f.create("other", false, "done")
	f.edge("a", "other", storage.EdgeTypeRelatesTo)
	req := mergeRequest(t, f, []string{"a", "b"}, "c")
	req.Relations = nil
	_, err := f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrInvalidNodeMerge)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	req = mergeRequest(t, f, []string{"a", "b"}, "c")
	f.edge("b", "other", storage.EdgeTypeInforms)
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrNodeMergeConflict)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
}

func TestNodeMergeExistingExternalEndpointBaseline(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-endpoint")
	for _, slug := range []string{"a", "b", "x"} {
		f.create(slug, false, "approved")
	}
	f.edge("a", "x", storage.EdgeTypeRelatesTo)
	req := mergeRequest(t, f, []string{"a", "b"}, "c")
	require.Len(t, req.Expected.Contexts, 1)
	require.Equal(t, "x", req.Expected.Contexts[0].Slug)
	changed := "X changed after A's relation preview"
	_, err := f.s.UpdateSpec(f.ctx, "x", &changed, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrNodeMergeConflict)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	edges, err := f.s.ListEdges(f.ctx, "a", storage.EdgeTypeRelatesTo)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	req = mergeRequest(t, f, []string{"a", "b"}, "c")
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.NoError(t, err, "unchanged source and external endpoint baseline must commit")
}

func TestNodeMergeSummarySourceKeepsItsHistoricalObligation(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-summary")
	f.create("p", true, "approved")
	f.create("s", true, "approved")
	f.create("t", false, "approved")
	f.create("a", false, "done")
	f.edge("p", "s", storage.EdgeTypeComposes)
	f.edge("s", "a", storage.EdgeTypeComposes)
	req := mergeRequest(t, f, []string{"s", "t"}, "c", "p", "a")
	req.Target.Role = storage.SpecRoleSummary
	for i := range req.Relations {
		if req.Relations[i].Before.FromSlug == "p" && req.Relations[i].Before.ToSlug == "s" {
			req.Relations[i].Action = "rewire"
			req.Relations[i].Replacements = []storage.MergeRelationRef{{FromSlug: "p", ToSlug: "c", Type: "COMPOSES"}}
		}
	}
	req.AddedRelations = []storage.MergeRelationRef{{FromSlug: "c", ToSlug: "a", Type: "COMPOSES"}}
	_, err := f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrSummaryNotAcceptable)
	require.Contains(t, err.Error(), `goal "p" affected "s"`)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	approval := f.approve(f.source("p"))
	req.Dispositions = []storage.SummaryDispositionRequest{f.withdrawal("p", "s", "merge-p-s", approval, f.source("p"))}
	receipt, err := f.s.MergeNodes(f.ctx, req, nil)
	require.NoError(t, err)
	require.Len(t, receipt.DispositionIDs, 1)
	merged, err := f.s.GetSpec(f.ctx, "c")
	require.NoError(t, err)
	require.Equal(t, storage.SpecRoleSummary, merged.Role)
	require.Equal(t, storage.SpecStageSpark, merged.Stage)
	old := f.state("s")
	require.NotContains(t, fmt.Sprint(old.Blockers), "unreviewed_removal")
	foundA := false
	for _, obligation := range old.Obligations {
		if obligation.Slug == "a" {
			foundA = true
		}
	}
	require.True(t, foundA, "old S->A remains as historical structure")
	parent := f.state("p")
	require.NotContains(t, fmt.Sprint(parent.Blockers), "unreviewed_removal")
	for _, obligation := range parent.Obligations {
		if obligation.Slug == "s" {
			require.False(t, obligation.Effective)
			require.False(t, obligation.PendingReview)
		}
	}
	require.True(t, strings.Contains(fmt.Sprint(receipt.InsertedRelations), "COMPOSES"))
}

func TestNodeMergeRawBoundSourceNeedsOriginalHumanHandoff(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-raw")
	f.create("a", false, "approved")
	f.create("b", false, "approved")
	run, err := f.s.PrepareRun(f.ctx, "a", "C:/merge-raw")
	require.NoError(t, err)
	require.NoError(t, f.s.BindRunThreadInEnvironment(f.ctx, run, "local", "merge-raw-thread"))
	context, err := f.s.ReadRunContext(f.ctx, run)
	require.NoError(t, err)
	req := mergeRequest(t, f, []string{"a", "b"}, "c")
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.NoError(t, err, "merging does not pretend the raw source stopped")
	approved := "approved"
	_, err = f.s.UpdateSpec(f.ctx, "c", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	_, err = f.s.PrepareRun(f.ctx, "c", "C:/merge-new")
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	require.ErrorIs(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Old raw thread stopped"), storage.ErrRunBindingConflict)
	handoff := &storage.RawPreparationHandoff{EnvironmentID: "wrong", ThreadID: "merge-raw-thread", Summary: storage.NodeOwnershipSummary{
		Kind: "human_substitute", HumanSubstitute: &storage.NodeHumanHandoffSummary{Statement: "Observed original raw thread stopped",
			SourceRefs: []string{"original native thread merge-raw-thread"}, MissingInfo: []string{"No admitted dispatch existed"}}}}
	require.ErrorIs(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Old raw thread stopped", handoff), storage.ErrNodeOwnershipHandoffRequired)
	handoff.EnvironmentID = "local"
	require.NoError(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Old raw thread stopped", handoff))
	status, err := f.s.ReadRunDispatch(f.ctx, run)
	require.NoError(t, err)
	require.Equal(t, handoff, status.Cancellation.RawHandoff)
	require.NoError(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Old raw thread stopped", handoff))
	different := *handoff
	different.ThreadID = "changed"
	require.ErrorIs(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Old raw thread stopped", &different), storage.ErrRunBindingConflict)
	_, err = f.s.PrepareRun(f.ctx, "c", "C:/merge-new")
	require.NoError(t, err, "only the original raw responsibility was sealed")
}

func TestNodeMergeFinalCyclesRollback(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-cycle")
	for _, slug := range []string{"a", "b", "x"} {
		f.create(slug, false, "approved")
	}
	req := mergeRequest(t, f, []string{"a", "b"}, "c", "x")
	req.AddedRelations = []storage.MergeRelationRef{{FromSlug: "c", ToSlug: "x", Type: "DEPENDS_ON"}, {FromSlug: "c", ToSlug: "x", Type: "BLOCKS"}}
	_, err := f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrDependencyCycle)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	req.AddedRelations = []storage.MergeRelationRef{{FromSlug: "c", ToSlug: "x", Type: "COMPOSES"}, {FromSlug: "x", ToSlug: "c", Type: "COMPOSES"}}
	_, err = f.s.MergeNodes(f.ctx, req, nil)
	require.ErrorIs(t, err, storage.ErrNodeMergeConflict)
	_, err = f.s.GetSpec(f.ctx, "c")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
}

func TestNodeMergeRecursiveSourceResponsibility(t *testing.T) {
	f := newSummaryFixture(t, "node-merge-recursive")
	for _, slug := range []string{"a", "b", "d"} {
		f.create(slug, false, "approved")
	}
	run, err := f.s.PrepareRun(f.ctx, "a", "C:/merge-ancestor")
	require.NoError(t, err)
	require.NoError(t, f.s.BindRunThreadInEnvironment(f.ctx, run, "local", "merge-ancestor-thread"))
	context, err := f.s.ReadRunContext(f.ctx, run)
	require.NoError(t, err)
	_, err = f.s.MergeNodes(f.ctx, mergeRequest(t, f, []string{"a", "b"}, "c"), nil)
	require.NoError(t, err)
	_, err = f.s.MergeNodes(f.ctx, mergeRequest(t, f, []string{"c", "d"}, "e"), nil)
	require.NoError(t, err)
	approved := "approved"
	_, err = f.s.UpdateSpec(f.ctx, "e", nil, &approved, nil, nil, nil)
	require.NoError(t, err)
	_, err = f.s.PrepareRun(f.ctx, "e", "C:/merge-descendant")
	require.ErrorIs(t, err, storage.ErrDispatchResponsibilityHeld)
	handoff := &storage.RawPreparationHandoff{EnvironmentID: "local", ThreadID: "merge-ancestor-thread", Summary: storage.NodeOwnershipSummary{
		Kind: "human_substitute", HumanSubstitute: &storage.NodeHumanHandoffSummary{Statement: "Observed original thread stopped",
			SourceRefs: []string{"original raw thread"}, MissingInfo: []string{"No dispatch was admitted"}}}}
	require.NoError(t, f.s.CancelRunPreparation(f.ctx, run, context.PackageID, f.user, "Original raw thread stopped", handoff))
	_, err = f.s.PrepareRun(f.ctx, "e", "C:/merge-descendant")
	require.NoError(t, err)
}
