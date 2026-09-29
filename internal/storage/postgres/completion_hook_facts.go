// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/specgraph/specgraph/internal/storage"
)

// Called only by the original fact owner, after its successful transition, in that transaction.
func (s *Store) triggerCompletionHooks(ctx context.Context, slug, kind, id string) error {
	if kind == "execution" || kind == "manual" {
		scope, err := s.workCompletionScopeReferences(ctx, slug)
		if err != nil {
			return err
		}
		query := `UPDATE execution_events e SET completion_context=jsonb_build_object('specId',s.id,'completionGeneration',s.completion_generation,'fieldSourceRefs',s.field_source_refs,'scope',$4::jsonb)
 FROM specs s WHERE e.project_slug=$1 AND e.spec_slug=$2 AND e.id=$3 AND e.event_type='completion'
 AND s.project_slug=e.project_slug AND s.slug=e.spec_slug AND s.role='work' AND s.stage='done'`
		if kind == "manual" {
			query = `UPDATE manual_completions e SET completion_context=jsonb_build_object('specId',s.id,'completionGeneration',s.completion_generation,'fieldSourceRefs',s.field_source_refs,'scope',$4::jsonb)
 FROM specs s WHERE e.project_slug=$1 AND e.spec_slug=$2 AND e.id=$3
 AND s.project_slug=e.project_slug AND s.slug=e.spec_slug AND s.role='work' AND s.stage='done'`
		}
		tag, err := s.exec(ctx, query, s.project, slug, id, scope)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return storage.ErrCompletionHookConflict
		}
	}
	_, err := s.exec(ctx, `UPDATE delivery_test_hooks h SET fact_kind=$3,fact_id=$4,triggered_at=$5
 FROM specs s WHERE h.hook_kind='completion_program' AND h.project_slug=$1 AND h.source_task_slug=$2
 AND h.fact_id IS NULL AND h.cancelled_at IS NULL AND s.project_slug=h.project_slug AND s.slug=h.source_task_slug
 AND s.id=h.source_spec_id AND s.role=h.source_role
 AND ((h.source_role='work' AND $3 IN ('execution','manual')) OR (h.source_role='summary' AND $3='summary'))`, s.project, slug, kind, id, s.now())
	return err
}

func (s *Store) checkCompletionHookSource(ctx context.Context, hook *storage.CompletionProgramHook) error {
	var matches bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2 AND id=$3 AND role=$4)`, s.project, hook.SourceTaskSlug, hook.SourceSpecID, hook.SourceRole).Scan(&matches); err != nil {
		return fmt.Errorf("postgres: checkCompletionHookSource: %w", err)
	}
	if !matches || hook.Fact == nil {
		return storage.ErrCompletionHookConflict
	}
	if hook.SourceRole == "summary" {
		if hook.Fact.Kind != "summary" || !validReviewID(hook.Fact.ID) {
			return storage.ErrInvalidCompletionHook
		}
		state, err := s.readSummaryInSnapshot(ctx, hook.SourceTaskSlug)
		if err != nil {
			return err
		}
		if !state.Accepted || state.LatestAcceptance == nil || state.LatestAcceptance.ID != hook.Fact.ID {
			return storage.ErrCompletionHookConflict
		}
		return nil
	}
	return s.checkWorkCompletionFact(ctx, hook.SourceTaskSlug, hook.Fact.Kind, hook.Fact.ID)
}

// checkWorkCompletionFact is shared by fixed completion hooks and exact-run joins.
func (s *Store) checkWorkCompletionFact(ctx context.Context, slug, kind, factID string) error {
	if kind != "execution" && kind != "manual" {
		return storage.ErrInvalidCompletionHook
	}
	review, err := s.ReadReviewStatus(ctx, slug)
	if err != nil {
		return err
	}
	for _, state := range review.Reviews {
		if state.HumanHold {
			return storage.ErrReviewHumanHold
		}
	}
	scope, err := s.workCompletionScopeReferences(ctx, slug)
	if err != nil {
		return err
	}
	query := `SELECT EXISTS(SELECT 1 FROM execution_events e JOIN specs s ON s.project_slug=e.project_slug AND s.slug=e.spec_slug
 WHERE e.project_slug=$1 AND e.spec_slug=$2 AND e.id=$3 AND e.event_type='completion' AND s.stage='done' AND s.role='work'
 AND e.completion_context=jsonb_build_object('specId',s.id,'completionGeneration',s.completion_generation,'fieldSourceRefs',s.field_source_refs,'scope',$4::jsonb))`
	if kind == "manual" {
		query = `SELECT EXISTS(SELECT 1 FROM manual_completions e JOIN specs s ON s.project_slug=e.project_slug AND s.slug=e.spec_slug
 WHERE e.project_slug=$1 AND e.spec_slug=$2 AND e.id=$3 AND s.stage='done' AND s.role='work'
 AND e.completion_context=jsonb_build_object('specId',s.id,'completionGeneration',s.completion_generation,'fieldSourceRefs',s.field_source_refs,'scope',$4::jsonb))`
	}
	var matches bool
	if err := s.queryRow(ctx, query, s.project, slug, factID, scope).Scan(&matches); err != nil {
		return fmt.Errorf("postgres: checkWorkCompletionFact: %w", err)
	}
	if !matches {
		return storage.ErrCompletionHookConflict
	}
	return nil
}

// Preserve the original scope owner's identities and revisions, never its document bodies.
func (s *Store) workCompletionScopeReferences(ctx context.Context, slug string) (json.RawMessage, error) {
	contexts, err := s.readScopeContexts(ctx, []string{slug})
	if err != nil {
		return nil, err
	}
	scope := contexts[slug]
	sources := make([]map[string]string, 0, len(scope.Sources))
	sourceSlugs := make([]string, 0, len(scope.Sources))
	for _, source := range scope.Sources {
		sources = append(sources, map[string]string{"id": source.ID, "slug": source.Slug, "revision": source.Revision})
		sourceSlugs = append(sourceSlugs, source.Slug)
	}
	slugs := make([]string, 0, len(scope.Decisions))
	for _, decision := range scope.Decisions {
		slugs = append(slugs, decision.Slug)
	}
	decisions, err := s.readDecisionSourceRefs(ctx, slugs)
	if err != nil {
		return nil, err
	}
	relationBody, err := json.Marshal(scope.Relations)
	if err != nil {
		return nil, fmt.Errorf("postgres: workCompletionScopeReferences: %w", err)
	}
	var relations json.RawMessage
	err = s.queryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('from',r."from",'to',r."to",'type',r."type",'changeId',
 COALESCE((SELECT max(c.id)::text FROM composition_changes c WHERE c.project_slug=$1 AND c.from_slug=r."from" AND c.to_slug=r."to" AND c.edge_type=r."type"),'0'))
 ORDER BY r."type" COLLATE "C",r."from" COLLATE "C",r."to" COLLATE "C"),'[]'::jsonb)
 FROM jsonb_to_recordset($2::jsonb) AS r("from" text,"to" text,"type" text)`, s.project, relationBody).Scan(&relations)
	if err != nil {
		return nil, fmt.Errorf("postgres: workCompletionScopeReferences: %w", err)
	}
	// A transient relation can disappear from the current set; keep its scoped lineage reference.
	var relationChangeID string
	if err := s.queryRow(ctx, `SELECT COALESCE(max(id)::text,'0') FROM composition_changes WHERE project_slug=$1
 AND ((edge_type='COMPOSES' AND to_slug=ANY($2)) OR (edge_type='DECIDED_IN' AND from_slug=ANY($2)))`, s.project, sourceSlugs).Scan(&relationChangeID); err != nil {
		return nil, fmt.Errorf("postgres: workCompletionScopeReferences: %w", err)
	}
	result, encodeErr := json.Marshal(struct {
		Sources          []map[string]string                `json:"sources"`
		Relations        json.RawMessage                    `json:"relations"`
		RelationChangeID string                             `json:"relationChangeId"`
		Decisions        []storage.SummaryDecisionReference `json:"decisions"`
	}{sources, relations, relationChangeID, decisions})
	if encodeErr != nil {
		return nil, fmt.Errorf("postgres: workCompletionScopeReferences: %w", encodeErr)
	}
	return result, nil
}

// Traverse only actual hard prerequisites and the summary owner's effective obligations.
func (s *Store) checkCompletionHookDeadlock(ctx context.Context, source, target string) error {
	g, err := s.loadSummaryGraph(ctx)
	if err != nil {
		return err
	}
	queue, visited := []string{source}, map[string]bool{}
	for len(queue) > 0 {
		slug := queue[0]
		queue = queue[1:]
		if slug == target {
			return storage.ErrCompletionHookConflict
		}
		if visited[slug] {
			continue
		}
		visited[slug] = true
		queue = append(queue, g.dependencies[slug]...)
		if g.nodes[slug].Role == storage.SpecRoleSummary {
			state, err := s.summaryState(ctx, g, slug)
			if err != nil {
				return err
			}
			for _, obligation := range state.Obligations {
				if obligation.Effective {
					queue = append(queue, obligation.Slug)
				}
			}
		}
	}
	return nil
}
