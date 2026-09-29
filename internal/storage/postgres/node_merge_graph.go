// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/specgraph/specgraph/internal/storage"
)

func mergeRelationKey(r storage.MergeRelationRef) string {
	return r.Type + "\x00" + r.FromSlug + "\x00" + r.ToSlug
}

func sortMergeRelations(relations []storage.MergeRelationRef) {
	slices.SortFunc(relations, func(a, b storage.MergeRelationRef) int {
		return slices.Compare([]string{a.Type, a.FromSlug, a.ToSlug}, []string{b.Type, b.FromSlug, b.ToSlug})
	})
}

func normalizeMergeRequest(req *storage.MergeRequest) {
	if req.Expected.Sources == nil {
		req.Expected.Sources = []storage.MergeSourceRef{}
	}
	if req.Expected.Contexts == nil {
		req.Expected.Contexts = []storage.MergeSourceRef{}
	}
	if req.Expected.Relations == nil {
		req.Expected.Relations = []storage.MergeRelationRef{}
	}
	if req.Expected.Lineage == nil {
		req.Expected.Lineage = []storage.MergeRelationRef{}
	}
	if req.Expected.Ancestors == nil {
		req.Expected.Ancestors = []storage.MergeAncestorRef{}
	}
	if req.Relations == nil {
		req.Relations = []storage.MergeRelationDisposition{}
	}
	if req.AddedRelations == nil {
		req.AddedRelations = []storage.MergeRelationRef{}
	}
	if req.Dispositions == nil {
		req.Dispositions = []storage.SummaryDispositionRequest{}
	}
	for i := range req.Relations {
		if req.Relations[i].Replacements == nil {
			req.Relations[i].Replacements = []storage.MergeRelationRef{}
		}
		sortMergeRelations(req.Relations[i].Replacements)
	}
	slices.SortFunc(req.Relations, func(a, b storage.MergeRelationDisposition) int {
		return slices.Compare([]string{a.Before.Type, a.Before.FromSlug, a.Before.ToSlug}, []string{b.Before.Type, b.Before.FromSlug, b.Before.ToSlug})
	})
	sortMergeRelations(req.AddedRelations)
	slices.SortFunc(req.Dispositions, func(a, b storage.SummaryDispositionRequest) int {
		return slices.Compare([]string{a.GoalSlug, a.AffectedSlug}, []string{b.GoalSlug, b.AffectedSlug})
	})
}

func validMergeRelation(r storage.MergeRelationRef, added bool) bool {
	if !validMailText(r.FromSlug, 256) || !validMailText(r.ToSlug, 256) || r.Type == "SUPERSEDES" || !storage.EdgeType(r.Type).IsValid() {
		return false
	}
	return !added || r.ChangeID == ""
}

type mergeGraphPlan struct {
	final    []storage.MergeRelationRef
	inserted []storage.MergeRelationRef
	deleted  []storage.MergeRelationRef
}

func (s *Store) planMergeGraph(ctx context.Context, req storage.MergeRequest) (*mergeGraphPlan, error) {
	before := make(map[string]storage.MergeRelationRef, len(req.Expected.Relations))
	sources := make(map[string]bool, len(req.Expected.Sources))
	for _, source := range req.Expected.Sources {
		sources[source.Slug] = true
	}
	contexts := make(map[string]bool, len(req.Expected.Contexts))
	for _, item := range req.Expected.Contexts {
		contexts[item.Slug] = true
	}
	for _, edge := range req.Expected.Relations {
		if !validMergeRelation(edge, false) {
			return nil, storage.ErrInvalidNodeMerge
		}
		key := mergeRelationKey(edge)
		if _, exists := before[key]; exists {
			return nil, storage.ErrInvalidNodeMerge
		}
		before[key] = edge
	}
	final := make(map[string]storage.MergeRelationRef, len(before)+len(req.AddedRelations))
	seen := make(map[string]bool, len(before))
	add := func(edge storage.MergeRelationRef) error {
		if !validMergeRelation(edge, true) || !(sources[edge.FromSlug] || sources[edge.ToSlug] || edge.FromSlug == req.Target.Slug || edge.ToSlug == req.Target.Slug) {
			return storage.ErrInvalidNodeMerge
		}
		key := mergeRelationKey(edge)
		if _, exists := final[key]; exists {
			return storage.ErrInvalidNodeMerge
		}
		final[key] = edge
		return nil
	}
	for _, action := range req.Relations {
		key := mergeRelationKey(action.Before)
		original, ok := before[key]
		if !ok || seen[key] || !reflect.DeepEqual(original, action.Before) {
			return nil, storage.ErrInvalidNodeMerge
		}
		seen[key] = true
		switch action.Action {
		case "retain":
			if len(action.Replacements) != 0 {
				return nil, storage.ErrInvalidNodeMerge
			}
			final[key] = original
		case "remove":
			if len(action.Replacements) != 0 {
				return nil, storage.ErrInvalidNodeMerge
			}
		case "rewire":
			if len(action.Replacements) == 0 {
				return nil, storage.ErrInvalidNodeMerge
			}
			for _, edge := range action.Replacements {
				if err := add(edge); err != nil {
					return nil, err
				}
			}
		default:
			return nil, storage.ErrInvalidNodeMerge
		}
	}
	if len(seen) != len(before) {
		return nil, fmt.Errorf("%w: every original relation needs an explicit disposition", storage.ErrInvalidNodeMerge)
	}
	for _, edge := range req.AddedRelations {
		if err := add(edge); err != nil {
			return nil, err
		}
	}
	plan := &mergeGraphPlan{final: []storage.MergeRelationRef{}, inserted: []storage.MergeRelationRef{}, deleted: []storage.MergeRelationRef{}}
	for key, edge := range final {
		plan.final = append(plan.final, edge)
		if _, exists := before[key]; !exists {
			plan.inserted = append(plan.inserted, edge)
		}
	}
	for key, edge := range before {
		if _, exists := final[key]; !exists {
			plan.deleted = append(plan.deleted, edge)
		}
	}
	sortMergeRelations(plan.final)
	sortMergeRelations(plan.inserted)
	sortMergeRelations(plan.deleted)
	for _, edge := range plan.inserted {
		for _, endpoint := range []string{edge.FromSlug, edge.ToSlug} {
			if endpoint == req.Target.Slug {
				continue
			}
			exists, err := s.nodeExists(ctx, endpoint)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, storage.ErrNodeMergeConflict
			}
			if !sources[endpoint] && !contexts[endpoint] {
				var spec bool
				if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, endpoint).Scan(&spec); err != nil {
					return nil, err
				}
				if spec {
					return nil, fmt.Errorf("%w: preview context for %q before adding its relation", storage.ErrNodeMergeConflict, endpoint)
				}
			}
		}
	}
	return plan, nil
}

func mergeEdgesWithPlan(before []storage.MergeRelationRef, plan *mergeGraphPlan) []storage.MergeRelationRef {
	removed := map[string]bool{}
	for _, edge := range plan.deleted {
		removed[mergeRelationKey(edge)] = true
	}
	result := make([]storage.MergeRelationRef, 0, len(before)+len(plan.inserted))
	for _, edge := range before {
		if !removed[mergeRelationKey(edge)] {
			result = append(result, edge)
		}
	}
	result = append(result, plan.inserted...)
	return result
}

func cycleInMergeEdges(edges []storage.MergeRelationRef, kinds ...string) bool {
	allowed := map[string]bool{}
	for _, kind := range kinds {
		allowed[kind] = true
	}
	links := map[string][]string{}
	for _, edge := range edges {
		if !allowed[edge.Type] {
			continue
		}
		from, to := edge.FromSlug, edge.ToSlug
		if edge.Type == "BLOCKS" {
			from, to = to, from
		}
		links[from] = append(links[from], to)
	}
	seen, active := map[string]bool{}, map[string]bool{}
	var visit func(string) bool
	visit = func(node string) bool {
		if active[node] {
			return true
		}
		if seen[node] {
			return false
		}
		seen[node], active[node] = true, true
		for _, to := range links[node] {
			if visit(to) {
				return true
			}
		}
		delete(active, node)
		return false
	}
	for node := range links {
		if visit(node) {
			return true
		}
	}
	return false
}

func (s *Store) validateMergeCycles(ctx context.Context, plan *mergeGraphPlan) error {
	rows, err := s.query(ctx, `SELECT from_slug,to_slug,edge_type FROM edges WHERE project_slug=$1 AND edge_type IN('DEPENDS_ON','BLOCKS','COMPOSES')`, s.project)
	if err != nil {
		return err
	}
	before := []storage.MergeRelationRef{}
	for rows.Next() {
		var edge storage.MergeRelationRef
		if err := rows.Scan(&edge.FromSlug, &edge.ToSlug, &edge.Type); err != nil {
			rows.Close()
			return err
		}
		before = append(before, edge)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	final := mergeEdgesWithPlan(before, plan)
	if cycleInMergeEdges(final, "DEPENDS_ON", "BLOCKS") {
		return storage.ErrDependencyCycle
	}
	if cycleInMergeEdges(final, "COMPOSES") {
		return fmt.Errorf("%w: final composition cycle", storage.ErrNodeMergeConflict)
	}
	return nil
}

func plannedMergeSummaryGraph(before *summaryGraph, created *summaryGraph, plan *mergeGraphPlan, sources []storage.MergeSourceRef) *summaryGraph {
	final := &summaryGraph{
		nodes: map[string]summaryNode{}, children: map[string][]string{}, parents: map[string][]string{}, dependencies: map[string][]string{},
		relations: []storage.SummaryRelationReference{}, changes: slices.Clone(before.changes), decisions: before.decisions,
		holds: before.holds, memo: map[string]*storage.SummaryState{},
	}
	for slug, node := range created.nodes {
		final.nodes[slug] = node
	}
	for _, source := range sources {
		node := final.nodes[source.Slug]
		node.Stage = "superseded"
		final.nodes[source.Slug] = node
	}
	removed := map[string]bool{}
	for _, edge := range plan.deleted {
		removed[mergeRelationKey(edge)] = true
	}
	for _, relation := range before.relations {
		edge := storage.MergeRelationRef{FromSlug: relation.FromSlug, ToSlug: relation.ToSlug, Type: relation.Type}
		if !removed[mergeRelationKey(edge)] {
			final.relations = append(final.relations, relation)
		}
	}
	for _, edge := range plan.inserted {
		if edge.Type != "COMPOSES" && edge.Type != "DECIDED_IN" && edge.Type != "DEPENDS_ON" && edge.Type != "BLOCKS" {
			continue
		}
		if _, fromSpec := final.nodes[edge.FromSlug]; !fromSpec {
			continue
		}
		if edge.Type == "COMPOSES" {
			if _, toSpec := final.nodes[edge.ToSlug]; !toSpec {
				continue
			}
		}
		final.relations = append(final.relations, storage.SummaryRelationReference{FromSlug: edge.FromSlug, ToSlug: edge.ToSlug, Type: edge.Type, Present: true})
	}
	for _, relation := range final.relations {
		switch relation.Type {
		case "COMPOSES":
			final.children[relation.FromSlug] = append(final.children[relation.FromSlug], relation.ToSlug)
			final.parents[relation.ToSlug] = append(final.parents[relation.ToSlug], relation.FromSlug)
		case "DEPENDS_ON":
			final.dependencies[relation.FromSlug] = append(final.dependencies[relation.FromSlug], relation.ToSlug)
		case "BLOCKS":
			final.dependencies[relation.ToSlug] = append(final.dependencies[relation.ToSlug], relation.FromSlug)
		}
	}
	return final
}
