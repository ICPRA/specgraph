// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestSearchWorkbenchKnowledge(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("knowledge-search"))
	_, err := s.EnsureProject(ctx, "knowledge-other")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,id,slug,intent,notes,stage,version) VALUES
		('knowledge-search','spec-literal','literal',$1,$2,'shape',7),
		('knowledge-other','spec-hidden','hidden','foreign-only','','spark',1)`, "Literal 100% a_b C++ \u4e2d\u6587", strings.Repeat("\u524d", 800)+"\u4e2d\u5fc3?"+strings.Repeat("\u540e", 800))
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO decisions(project_slug,id,slug,title,body,rationale,question,status,version) VALUES
		('knowledge-search','dec-history','history','Title term','Body term','Rationale term','Question term','superseded',9),
		('knowledge-other','dec-hidden','hidden','foreign-only','','','','proposed',1)`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO changelog_entries(project_slug,id,spec_slug,version,stage,summary,reason,changes) VALUES
		('knowledge-search','cl-review','literal',6,'shape','Change summary','Written review basis','[{"field":"stage","old_value":"spark","new_value":"shape"}]'),
		('knowledge-other','cl-hidden','hidden',1,'spark','foreign-only','','[]')`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,id,slug,intent,stage,version,spark_output,shape_output,specify_output,decompose_output) VALUES
		('knowledge-search','spec-authoring','authoring','Current definition','specify',11,$1,$2,$3,$4),
		('knowledge-other','spec-other-authoring','other-authoring','Other definition','spark',1,$1,$2,$3,$4)`,
		`{"definition":{"formula":"a\\b \"\u6570\u5b66\" sparkonly"}}`,
		`{"assumptions":[{"text":"shapeonly premise"}],"limit":314159}`,
		`{"criteria":["specifyonly invariant"],"verified":true}`,
		`{"parts":[{"label":"decomposeonly leaf"}]}`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO changelog_entries(project_slug,id,spec_slug,version,stage,summary,reason,changes) VALUES
		('knowledge-search','cl-authoring','authoring',10,'shape','Revision','Definition changed',$1),
		('knowledge-other','cl-other-authoring','other-authoring',10,'shape','Revision','Definition changed',$1)`,
		`[{"field":"notes","old_value":"historical \"\u5386\u53f2\" \\alpha","new_value":"revised proof"}]`)
	require.NoError(t, err)
	for _, query := range []string{"a\\b \"\u6570\u5b66\" sparkonly", "shapeonly premise", "314159", "specifyonly invariant", "true", "decomposeonly leaf"} {
		result, searchErr := s.SearchWorkbenchKnowledge(ctx, query)
		require.NoError(t, searchErr)
		require.Len(t, result.Items, 1, "same content in another project must stay hidden")
		item := result.Items[0]
		require.Equal(t, "spec", item.Kind)
		require.Equal(t, "spec-authoring", item.ID)
		require.Equal(t, "authoring", item.Slug)
		require.Equal(t, "specify", item.Status)
		require.Equal(t, int32(11), item.Version)
		require.Contains(t, item.Excerpt, query)
		require.Equal(t, 1, strings.Count(item.Excerpt, query), "JSON array values must not be repeated")
	}
	for _, query := range []string{"historical \"\u5386\u53f2\" \\alpha", "revised proof"} {
		result, searchErr := s.SearchWorkbenchKnowledge(ctx, query)
		require.NoError(t, searchErr)
		require.Len(t, result.Items, 1, "historical text belongs to its change record only")
		item := result.Items[0]
		require.Equal(t, "change", item.Kind)
		require.Equal(t, "cl-authoring", item.ID)
		require.Equal(t, "authoring", item.Slug)
		require.Equal(t, "shape", item.Status)
		require.Equal(t, int32(10), item.Version)
		require.Contains(t, item.Excerpt, query)
	}
	for _, query := range []string{"%", "_", "c++", "\u4e2d\u6587", "\u4e2d\u5fc3?"} {
		result, err := s.SearchWorkbenchKnowledge(ctx, query)
		require.NoError(t, err)
		require.Equal(t, query, result.Query)
		require.Equal(t, "specgraph-records", result.Scope)
		require.False(t, result.HasMore)
		require.Len(t, result.Items, 1)
		item := result.Items[0]
		require.Equal(t, "spec", item.Kind)
		require.Equal(t, "spec-literal", item.ID)
		require.Equal(t, "literal", item.Slug)
		require.Equal(t, "shape", item.Status)
		require.Equal(t, int32(7), item.Version)
		require.LessOrEqual(t, utf8.RuneCountInString(item.Excerpt), 600)
		require.Contains(t, strings.ToLower(item.Excerpt), query)
		if query == "\u4e2d\u5fc3?" {
			require.True(t, strings.HasPrefix(item.Excerpt, strings.Repeat("\u524d", 200)))
			require.True(t, strings.HasSuffix(item.Excerpt, strings.Repeat("\u540e", 200)))
		}
	}
	for _, query := range []string{"TITLE TERM", "body term", "rationale term", "question term"} {
		result, err := s.SearchWorkbenchKnowledge(ctx, query)
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		require.Equal(t, "decision", result.Items[0].Kind)
		require.Equal(t, "dec-history", result.Items[0].ID)
		require.Equal(t, "superseded", result.Items[0].Status)
		require.Equal(t, int32(9), result.Items[0].Version)
	}
	for _, query := range []string{"change summary", "written review basis"} {
		result, err := s.SearchWorkbenchKnowledge(ctx, query)
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		require.Equal(t, "change", result.Items[0].Kind)
		require.Equal(t, "cl-review", result.Items[0].ID)
		require.Equal(t, "literal", result.Items[0].Slug)
		require.Equal(t, int32(6), result.Items[0].Version)
	}
	empty, err := s.SearchWorkbenchKnowledge(ctx, "foreign-only")
	require.NoError(t, err)
	require.Empty(t, empty.Items)
	require.False(t, empty.HasMore)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,id,slug,intent,updated_at) VALUES
		('knowledge-search','spec-exact','needle','Exact identifier',now()-interval '1 day')`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,id,slug,intent)
		SELECT 'knowledge-search','spec-page-' || lpad(n::text,2,'0'),'page-' || n,'needle' FROM generate_series(1,21) n`)
	require.NoError(t, err)
	page, err := s.SearchWorkbenchKnowledge(ctx, "NEEDLE")
	require.NoError(t, err)
	require.True(t, page.HasMore)
	require.Len(t, page.Items, 20)
	require.Equal(t, "spec-exact", page.Items[0].ID, "exact identifiers precede newer content matches")
	for i := 1; i < len(page.Items); i++ {
		require.Equal(t, fmt.Sprintf("spec-page-%02d", i), page.Items[i].ID)
	}
	byID, err := s.SearchWorkbenchKnowledge(ctx, "SPEC-EXACT")
	require.NoError(t, err)
	require.Len(t, byID.Items, 1)
	require.Equal(t, "spec-exact", byID.Items[0].ID)

	other, err := s.ScopedExisting(ctx, "knowledge-other")
	require.NoError(t, err)
	for _, source := range []struct {
		kind, reference, id, slug string
		version                   int32
		missing                   error
	}{
		{"spec", "literal", "spec-literal", "literal", 7, storage.ErrSpecNotFound},
		{"decision", "history", "dec-history", "history", 9, storage.ErrDecisionNotFound},
		{"change", "cl-review", "cl-review", "literal", 6, postgres.ErrWorkbenchChangeNotFound},
	} {
		record, err := s.ReadWorkbenchKnowledgeRecord(ctx, source.kind, source.reference)
		require.NoError(t, err)
		require.Equal(t, source.kind, record.Kind)
		require.Equal(t, source.id, record.ID)
		require.Equal(t, source.slug, record.Slug)
		require.Equal(t, source.version, record.Version)
		require.Equal(t, "knowledge-search", record.SpecgraphProject)
		switch value := record.Record.(type) {
		case *storage.Spec:
			require.Contains(t, value.Notes, strings.Repeat("\u524d", 800), "full original notes, not the search excerpt")
		case *storage.Decision:
			require.Equal(t, "Rationale term", value.Rationale)
			require.Equal(t, storage.DecisionStatusSuperseded, value.Status)
		case *storage.ChangeLogEntry:
			require.Equal(t, "Change summary", value.Summary)
			require.Equal(t, "Written review basis", value.Reason)
			require.Equal(t, []storage.FieldChange{{Field: "stage", OldValue: "spark", NewValue: "shape"}}, value.Changes)
			require.False(t, value.Date.IsZero())
		default:
			t.Fatalf("unexpected source record type %T", value)
		}
		_, err = other.ReadWorkbenchKnowledgeRecord(ctx, source.kind, source.reference)
		require.ErrorIs(t, err, source.missing)
	}
	_, err = s.ReadWorkbenchKnowledgeRecord(ctx, "unsupported", "literal")
	require.ErrorContains(t, err, "invalid knowledge record kind")
	_, err = s.Pool().Exec(ctx, `UPDATE specs SET version=8,notes='Updated source' WHERE project_slug='knowledge-search' AND slug='literal'`)
	require.NoError(t, err)
	current, err := s.ReadWorkbenchKnowledgeRecord(ctx, "spec", "literal")
	require.NoError(t, err)
	require.Equal(t, int32(8), current.Version)
	require.Equal(t, "Updated source", current.Record.(*storage.Spec).Notes)
}
