// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestSpecFieldSourceRefs(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("field-sources"))
	initial, err := s.CreateSpec(ctx, "task", "Initial requirement", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	readRefs := func() map[string]string {
		t.Helper()
		refs, err := s.ReadSpecSourceRefs(ctx, "task")
		require.NoError(t, err)
		return refs
	}
	readChange := func(id, field string) storage.FieldChange {
		t.Helper()
		record, err := s.ReadWorkbenchKnowledgeRecord(ctx, "change", id)
		require.NoError(t, err)
		require.Equal(t, "task", record.Slug)
		for _, change := range record.Record.(*storage.ChangeLogEntry).Changes {
			if change.Field == field {
				return change
			}
		}
		t.Fatalf("source %s lacks field %s", id, field)
		return storage.FieldChange{}
	}
	refs := readRefs()
	require.Len(t, refs, 1)
	intentSource := refs["intent"]
	require.NotEmpty(t, intentSource)
	require.Equal(t, storage.FieldChange{Field: "intent", OldValue: "", NewValue: "Initial requirement"}, readChange(intentSource, "intent"))
	for _, field := range []string{"shape_output", "specify_output"} {
		var previous string
		for _, value := range []string{"first source", "second source"} {
			var output any
			if field == "shape_output" {
				shape := &storage.ShapeOutput{ChosenApproach: value}
				require.NoError(t, s.StoreShapeOutput(ctx, "task", shape))
				output = shape
			} else {
				specify := &storage.SpecifyOutput{Invariants: []string{value}}
				require.NoError(t, s.StoreSpecifyOutput(ctx, "task", specify))
				output = specify
			}
			current := readRefs()
			require.Equal(t, intentSource, current["intent"])
			source := current[field]
			require.NotEmpty(t, source)
			require.NotEqual(t, previous, source)
			record, err := s.ReadWorkbenchKnowledgeRecord(ctx, "change", source)
			require.NoError(t, err)
			require.Equal(t, initial.Version, record.Version, "same global version must still distinguish original content events")
			expected, err := json.Marshal(output)
			require.NoError(t, err)
			delta := readChange(source, field)
			require.JSONEq(t, string(expected), delta.NewValue)
			if previous != "" {
				old := readChange(previous, field)
				require.JSONEq(t, old.NewValue, delta.OldValue)
				require.Contains(t, old.NewValue, "first source", "prior event must remain original")
			}
			previous = source
		}
	}
	stable := readRefs()
	require.NoError(t, s.StoreShapeOutput(ctx, "task", &storage.ShapeOutput{ChosenApproach: "second source"}))
	require.NoError(t, s.StoreSpecifyOutput(ctx, "task", &storage.SpecifyOutput{Invariants: []string{"second source"}}))
	require.Equal(t, stable, readRefs(), "same field content must not mint new source identity")
	require.NoError(t, s.TransitionStage(ctx, "task", storage.SpecStageSpark, storage.SpecStageShape))
	notes, priority := "Metadata note", "p1"
	_, err = s.UpdateSpec(ctx, "task", nil, nil, &priority, nil, &notes)
	require.NoError(t, err)
	require.Equal(t, stable, readRefs(), "stage and unrelated metadata retain content sources")
	intent := "Revised requirement"
	_, err = s.UpdateSpec(ctx, "task", &intent, nil, nil, nil, nil)
	require.NoError(t, err)
	updated := readRefs()
	require.NotEqual(t, intentSource, updated["intent"])
	require.Equal(t, stable["shape_output"], updated["shape_output"])
	require.Equal(t, stable["specify_output"], updated["specify_output"])
	require.Equal(t, intent, readChange(updated["intent"], "intent").NewValue)
	changesBefore, err := s.ListChanges(ctx, "task", storage.ChangeLogFilter{})
	require.NoError(t, err)
	rollback := errors.New("synthetic rollback after content and pointer writes")
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.StoreShapeOutput(txCtx, "task", &storage.ShapeOutput{ChosenApproach: "rolled back"}); err != nil {
			return err
		}
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	require.Equal(t, updated, readRefs())
	changesAfter, err := s.ListChanges(ctx, "task", storage.ChangeLogFilter{})
	require.NoError(t, err)
	require.ElementsMatch(t, changesBefore, changesAfter, "event and pointer commit or roll back together")
	current, err := s.ReadWorkbenchKnowledgeRecord(ctx, "spec", "task")
	require.NoError(t, err)
	require.NotNil(t, current.SourceRefs)
	require.Equal(t, updated, *current.SourceRefs)
	require.Equal(t, "field-sources", current.SpecgraphProject)
	_, err = s.EnsureProject(ctx, "field-sources-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "field-sources-other")
	require.NoError(t, err)
	_, err = other.ReadSpecSourceRefs(ctx, "task")
	require.ErrorIs(t, err, storage.ErrSpecNotFound)
	_, err = other.ReadWorkbenchKnowledgeRecord(ctx, "change", updated["intent"])
	require.ErrorIs(t, err, postgres.ErrWorkbenchChangeNotFound)
	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug,id,intent) VALUES('field-sources','legacy','legacy-id','Legacy intent');
	 INSERT INTO changelog_entries(id,project_slug,spec_slug,version,changes) VALUES('legacy-event','field-sources','legacy',1,'[{"field":"intent","old_value":"","new_value":"Legacy intent"}]')`)
	require.NoError(t, err)
	unknown, err := s.ReadSpecSourceRefs(ctx, "legacy")
	require.NoError(t, err)
	require.NotNil(t, unknown)
	require.Empty(t, unknown, "unlinked history is not automatically guessed into a current pointer")
	legacy, err := s.ReadWorkbenchKnowledgeRecord(ctx, "spec", "legacy")
	require.NoError(t, err)
	wire, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.Contains(t, string(wire), `"sourceRefs":{}`)
}
