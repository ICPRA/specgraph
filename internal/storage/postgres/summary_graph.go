// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

type summaryLoss struct {
	Goal          string `json:"goal"`
	Slug          string `json:"slug"`
	DispositionID string `json:"dispositionId"`
	Authorized    bool   `json:"authorized,omitempty"`
}

func (s *Store) summaryRemovalDispositions(ctx context.Context, from, to string) ([]string, []summaryLoss, error) {
	var specEdge bool
	if scanErr := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM edges e JOIN specs f ON f.project_slug=e.project_slug AND f.slug=e.from_slug JOIN specs t ON t.project_slug=e.project_slug AND t.slug=e.to_slug WHERE e.project_slug=$1 AND e.from_slug=$2 AND e.to_slug=$3 AND e.edge_type='COMPOSES')`, s.project, from, to).Scan(&specEdge); scanErr != nil {
		return nil, nil, fmt.Errorf("postgres: check summary composition edge: %w", scanErr)
	}
	ids := []string{}
	losses := []summaryLoss{}
	if !specEdge {
		return ids, losses, nil
	}
	g, err := s.loadSummaryGraph(ctx)
	if err != nil {
		return nil, nil, err
	}
	after := make(map[string][]string, len(g.children))
	for slug, children := range g.children {
		after[slug] = children
	}
	after[from] = slices.DeleteFunc(slices.Clone(after[from]), func(child string) bool { return child == to })
	ancestors, _ := summaryReach(from, g.parents)
	for goal := range ancestors {
		if g.nodes[goal].Role != storage.SpecRoleSummary {
			continue
		}
		before, _ := summaryReach(goal, g.children)
		remaining, _ := summaryReach(goal, after)
		var state *storage.SummaryState
		for slug := range before {
			if remaining[slug] {
				continue
			}
			if state == nil {
				state, err = s.summaryState(ctx, g, goal)
				if err != nil {
					return nil, nil, err
				}
			}
			covered := false
			for _, o := range state.Obligations {
				if o.Slug != slug || o.PendingReview || o.Effective || (o.Disposition != "withdraw" && o.Disposition != "replace") {
					continue
				}
				if o.Disposition == "replace" {
					for i := range state.Dispositions {
						d := &state.Dispositions[i]
						if d.ID == o.DispositionID && !remaining[d.ReplacementSlug] {
							return nil, nil, storage.ErrSummaryNotAcceptable
						}
					}
				}
				ids = append(ids, o.DispositionID)
				losses = append(losses, summaryLoss{Goal: goal, Slug: slug, DispositionID: o.DispositionID})
				covered = true
				break
			}
			if !covered {
				return nil, nil, storage.ErrSummaryNotAcceptable
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids), losses, nil
}

func (s *Store) recordCompositionChange(ctx context.Context, from, to string, kind storage.EdgeType, operation string, ids []string, losses []summaryLoss) error {
	if kind != storage.EdgeTypeComposes && kind != storage.EdgeTypeDecidedIn {
		return nil
	}
	var actor *string
	if identity, ok := auth.IdentityFromContext(ctx); ok && identity.UserID != "" {
		actor = &identity.UserID
	}
	if ids == nil {
		ids = []string{}
	}
	if losses == nil {
		losses = []summaryLoss{}
	}
	_, err := s.exec(ctx, `INSERT INTO composition_changes(project_slug,from_slug,to_slug,edge_type,operation,actor_user_id,dispositions,lost_obligations)
 SELECT $1,$2,$3,$4,$5,$6,$7,$8 WHERE EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)
 AND (($4='COMPOSES' AND EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$3))
 OR ($4='DECIDED_IN' AND EXISTS(SELECT 1 FROM decisions WHERE project_slug=$1 AND slug=$3)))`, s.project, from, to, string(kind), operation, actor, ids, losses)
	return err
}
