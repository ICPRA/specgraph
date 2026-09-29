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

func TestSubdivisionPreservesParentAndStartsUnapprovedWork(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("subdivision-preserve"))
	parent, err := s.CreateSpec(ctx, "parent", "Original contract", "p1", "high",
		storage.SpecProvenanceDeclared, storage.SpecProvenanceDetail{Declared: &storage.DeclaredProvenance{DeclaredBy: "human", Note: "Historical assertion"}},
		&storage.SparkOutput{}, &storage.ShapeOutput{}, &storage.SpecifyOutput{}, &storage.DecomposeOutput{})
	require.NoError(t, err)
	_, err = s.RecordConversation(ctx, parent.Slug, storage.ConversationLogEntry{Stage: storage.SpecStageSpark,
		Exchanges: []storage.ConversationExchange{{Role: storage.ConversationRoleResponse, Content: "Original rationale", Stage: "spark", Sequence: 1}}, ExchangeCount: 1})
	require.NoError(t, err)
	parent, err = s.GetSpec(ctx, parent.Slug)
	require.NoError(t, err)
	changes, err := s.ListChanges(ctx, parent.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.NoError(t, s.CreateSlice(ctx, &storage.Slice{Slug: "native-slice", ParentSlug: parent.Slug, SliceID: "s1", Intent: "Native decomposition"}))
	_, err = s.AddEdge(ctx, parent.Slug, "native-slice", storage.EdgeTypeComposes)
	require.NoError(t, err)
	request := storage.SubdivideRequest{ExpectedVersion: parent.Version, Reason: "Separate concrete responsibilities",
		Children: []storage.ChildSpecDraft{{Slug: "child-a", Intent: "Implement A", Priority: "p1", Complexity: "low"}, {Slug: "child-b", Intent: "Implement B", Priority: "p2", Complexity: "medium"}}}
	result, err := s.SubdivideSpec(ctx, parent.Slug, "operator", request)
	require.NoError(t, err)
	require.Equal(t, parent.Version+1, result.ParentVersion)
	current, err := s.GetSpec(ctx, parent.Slug)
	require.NoError(t, err)
	expected := *parent
	expected.Role, expected.Version, expected.UpdatedAt = storage.SpecRoleSummary, result.ParentVersion, current.UpdatedAt
	require.Equal(t, &expected, current)
	afterChanges, err := s.ListChanges(ctx, parent.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Len(t, afterChanges, len(changes)+1)
	require.Equal(t, changes, afterChanges[:len(changes)])
	require.Equal(t, request.Reason, afterChanges[len(changes)].Reason)
	require.Contains(t, afterChanges[len(changes)].Summary, "operator")
	require.Contains(t, afterChanges[len(changes)].Summary, "child-a")
	for _, slug := range result.ChildSlugs {
		child, err := s.GetSpec(ctx, slug)
		require.NoError(t, err)
		require.Equal(t, storage.SpecStageSpark, child.Stage)
		require.Equal(t, storage.SpecRoleWork, child.Role)
		require.Equal(t, storage.SpecProvenanceAuthored, child.Provenance)
		require.Nil(t, child.SpecifyOutput)
		require.Empty(t, child.ConversationLogs)
	}
	metadata, err := s.ReadWorkbenchMetadata(ctx)
	require.NoError(t, err)
	require.Empty(t, metadata.Runs)
	require.Empty(t, metadata.Deliveries)
	require.Empty(t, metadata.Acceptances)
	var stored map[string]json.RawMessage
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT to_jsonb(d) FROM subdivisions d WHERE id=$1`, result.ID).Scan(&stored))
	require.Len(t, stored, 8, "lineage stores only identity, actor, reason, versions and time")
	var lineageCount int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM subdivision_children c JOIN specs s ON s.project_slug=c.project_slug AND s.slug=c.child_slug AND s.id=c.child_id WHERE c.operation_id=$1`, result.ID).Scan(&lineageCount))
	require.Equal(t, 2, lineageCount)
	require.NoError(t, s.RemoveEdge(ctx, parent.Slug, "child-a", storage.EdgeTypeComposes))
	changed := "Later parent clarification"
	_, err = s.UpdateSpec(ctx, parent.Slug, &changed, nil, nil, nil, nil)
	require.NoError(t, err)
	read, err := s.ReadSubdivision(ctx, result.ID)
	require.NoError(t, err)
	require.Equal(t, storage.SubdivisionSourceReference{ID: parent.ID, Slug: parent.Slug, Version: parent.Version}, read.SourceReference)
	require.Equal(t, changed, read.Parent.Intent)
	require.Equal(t, result, read.Receipt)
	require.False(t, read.Children[0].Attached)
	list, err := s.ListSpecSubdivisions(ctx, "child-a")
	require.NoError(t, err)
	require.Len(t, list.Items, 1, "detachment must preserve historical lineage")
	require.Equal(t, result.ID, list.Items[0].ID)
	_, err = s.SubdivideSpec(ctx, parent.Slug, "operator", request)
	require.ErrorIs(t, err, storage.ErrConcurrentModification, "a repeated stale creation is not replayed")
	other := newStore(t, postgres.WithProject("subdivision-other"))
	_, err = other.ReadSubdivision(ctx, result.ID)
	require.ErrorIs(t, err, storage.ErrSubdivisionNotFound)
}

func TestSubdivisionVersionAndAtomicCollision(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("subdivision-conflicts"))
	parent, err := s.CreateSpec(ctx, "parent", "Contract", "p1", "high", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.CreateSpec(ctx, "existing", "Existing child", "p1", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, parent.Slug, "existing", storage.EdgeTypeComposes)
	require.NoError(t, err)
	_, err = s.AddEdge(ctx, parent.Slug, "existing", storage.EdgeTypeDependsOn)
	require.NoError(t, err)
	request := storage.SubdivideRequest{ExpectedVersion: parent.Version, Reason: "Split work",
		Children: []storage.ChildSpecDraft{{Slug: "fresh", Intent: "Concrete work", Priority: "p1", Complexity: "low"}}}
	stale := request
	stale.ExpectedVersion++
	_, err = s.SubdivideSpec(ctx, parent.Slug, "operator", stale)
	require.ErrorIs(t, err, storage.ErrConcurrentModification)
	collision := request
	collision.Children = append(append([]storage.ChildSpecDraft(nil), request.Children...), storage.ChildSpecDraft{Slug: "existing", Intent: "Must not overwrite", Priority: "p1", Complexity: "low"})
	_, err = s.SubdivideSpec(ctx, parent.Slug, "operator", collision)
	require.ErrorIs(t, err, storage.ErrSpecAlreadyExists)
	_, err = s.GetSpec(ctx, "fresh")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	current, err := s.GetSpec(ctx, parent.Slug)
	require.NoError(t, err)
	require.Equal(t, parent.Version, current.Version)
	require.Equal(t, storage.SpecRoleWork, current.Role)
	changes, err := s.ListChanges(ctx, parent.Slug, storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	var count int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM subdivisions WHERE project_slug='subdivision-conflicts'`).Scan(&count))
	require.Zero(t, count)
	result, err := s.SubdivideSpec(ctx, parent.Slug, "operator", request)
	require.NoError(t, err)
	require.Equal(t, []string{"fresh"}, result.ChildSlugs)
}

func TestSubdivisionValidatesDraftsBeforeWriting(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("subdivision-validation"))
	for _, children := range [][]storage.ChildSpecDraft{
		nil,
		{{Slug: "../bad", Intent: "Intent", Priority: "p1", Complexity: "low"}},
		{{Slug: "parent", Intent: "Intent", Priority: "p1", Complexity: "low"}},
		{{Slug: "a", Intent: " ", Priority: "p1", Complexity: "low"}},
		{{Slug: "a", Intent: "Intent", Priority: "p1", Complexity: "low"}, {Slug: "a", Intent: "Intent", Priority: "p1", Complexity: "low"}},
		make([]storage.ChildSpecDraft, 33),
	} {
		_, err := s.SubdivideSpec(ctx, "parent", "operator", storage.SubdivideRequest{ExpectedVersion: 1, Reason: "Reason", Children: children})
		require.ErrorIs(t, err, storage.ErrInvalidSubdivisionRequest)
	}
}
