// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/specgraph/specgraph/internal/storage"
)

func mergePair(goal, affected string) string { return goal + "\x00" + affected }

func (s *Store) mergeRequiredDispositions(ctx context.Context, before, final *summaryGraph, preview *storage.MergePreview) (map[string]bool, []summaryLoss, error) {
	required := map[string]bool{}
	losses := []summaryLoss{}
	sources := map[string]bool{}
	for _, source := range preview.Sources {
		sources[source.Slug] = true
	}
	for _, ancestor := range preview.Ancestors {
		goal := ancestor.GoalSlug
		beforeReach, beforeCycle := summaryReach(goal, before.children)
		finalReach, finalCycle := summaryReach(goal, final.children)
		if beforeCycle || finalCycle {
			return nil, nil, storage.ErrSummaryNotAcceptable
		}
		state, err := s.summaryState(ctx, before, goal)
		if err != nil {
			return nil, nil, err
		}
		prior := map[string]storage.SummaryObligation{}
		for _, obligation := range state.Obligations {
			prior[obligation.Slug] = obligation
		}
		for affected := range beforeReach {
			if affected == goal {
				continue
			}
			lost := !finalReach[affected]
			if !lost && !sources[affected] {
				continue
			}
			key := mergePair(goal, affected)
			obligation := prior[affected]
			covered := !obligation.PendingReview && !obligation.Effective &&
				(obligation.Disposition == "withdraw" || obligation.Disposition == "replace")
			required[key] = !covered
			if lost {
				loss := summaryLoss{Goal: goal, Slug: affected}
				if covered {
					loss.DispositionID = obligation.DispositionID
				}
				losses = append(losses, loss)
			}
		}
		for source := range sources {
			if source != goal && finalReach[source] && !beforeReach[source] {
				return nil, nil, fmt.Errorf("%w: goal %q newly reaches superseded source %q without a prior obligation", storage.ErrNodeMergeConflict, goal, source)
			}
		}
	}
	slices.SortFunc(losses, func(a, b summaryLoss) int { return slices.Compare([]string{a.Goal, a.Slug}, []string{b.Goal, b.Slug}) })
	return required, losses, nil
}

func (s *Store) verifyMergedSummary(ctx context.Context, preview *storage.MergePreview, losses []summaryLoss) error {
	g, err := s.loadSummaryGraph(ctx)
	if err != nil {
		return err
	}
	checks := map[string]string{}
	for _, loss := range losses {
		checks[mergePair(loss.Goal, loss.Slug)] = loss.DispositionID
	}
	for _, ancestor := range preview.Ancestors {
		state, err := s.summaryState(ctx, g, ancestor.GoalSlug)
		if err != nil {
			return err
		}
		for _, obligation := range state.Obligations {
			key := mergePair(ancestor.GoalSlug, obligation.Slug)
			id, needed := checks[key]
			if !needed {
				continue
			}
			if obligation.PendingReview || obligation.Effective || obligation.DispositionID != id ||
				(obligation.Disposition != "withdraw" && obligation.Disposition != "replace") {
				return fmt.Errorf("%w: final goal %q affected %q disposition is not valid", storage.ErrSummaryNotAcceptable, ancestor.GoalSlug, obligation.Slug)
			}
			delete(checks, key)
		}
		for _, source := range preview.Sources {
			if source.Slug == ancestor.GoalSlug {
				continue
			}
			reach, _ := summaryReach(ancestor.GoalSlug, g.children)
			if !reach[source.Slug] {
				continue
			}
			valid := false
			for _, obligation := range state.Obligations {
				if obligation.Slug == source.Slug && !obligation.PendingReview && !obligation.Effective &&
					(obligation.Disposition == "withdraw" || obligation.Disposition == "replace") {
					valid = true
				}
			}
			if !valid {
				return fmt.Errorf("%w: final goal %q still reaches superseded source %q without valid disposition", storage.ErrSummaryNotAcceptable, ancestor.GoalSlug, source.Slug)
			}
		}
	}
	for pair := range checks {
		goal, affected := splitMergePair(pair)
		return fmt.Errorf("%w: final goal %q lost affected %q without visible disposition", storage.ErrSummaryNotAcceptable, goal, affected)
	}
	return nil
}

func (s *Store) applyMergeRelations(ctx context.Context, plan *mergeGraphPlan, before *summaryGraph, losses []summaryLoss) error {
	byEdge := map[string][]summaryLoss{}
	for _, loss := range losses {
		goalReach, _ := summaryReach(loss.Goal, before.children)
		assigned := false
		for _, edge := range plan.deleted {
			if edge.Type != "COMPOSES" || !goalReach[edge.FromSlug] {
				continue
			}
			descendants, _ := summaryReach(edge.ToSlug, before.children)
			if descendants[loss.Slug] {
				byEdge[mergeRelationKey(edge)] = append(byEdge[mergeRelationKey(edge)], loss)
				assigned = true
				break
			}
		}
		if !assigned {
			return fmt.Errorf("%w: lost goal %q affected %q has no actual composition deletion", storage.ErrNodeMergeConflict, loss.Goal, loss.Slug)
		}
	}
	for _, edge := range plan.deleted {
		tag, err := s.exec(ctx, `DELETE FROM edges WHERE project_slug=$1 AND from_slug=$2 AND to_slug=$3 AND edge_type=$4`, s.project, edge.FromSlug, edge.ToSlug, edge.Type)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return storage.ErrNodeMergeConflict
		}
		attached := byEdge[mergeRelationKey(edge)]
		ids := []string{}
		for _, loss := range attached {
			ids = append(ids, loss.DispositionID)
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		if err := s.recordCompositionChange(ctx, edge.FromSlug, edge.ToSlug, storage.EdgeType(edge.Type), "DELETE", ids, attached); err != nil {
			return err
		}
	}
	for _, edge := range plan.inserted {
		hash := ""
		if edge.Type == "DEPENDS_ON" {
			var err error
			hash, err = s.lookupContentHash(ctx, edge.ToSlug)
			if err != nil {
				return err
			}
		}
		if _, err := s.exec(ctx, `INSERT INTO edges(project_slug,from_slug,to_slug,edge_type,content_hash_at_link) VALUES($1,$2,$3,$4,$5)`, s.project, edge.FromSlug, edge.ToSlug, edge.Type, hash); err != nil {
			return err
		}
		if err := s.recordCompositionChange(ctx, edge.FromSlug, edge.ToSlug, storage.EdgeType(edge.Type), "INSERT", nil, nil); err != nil {
			return err
		}
	}
	return nil
}
