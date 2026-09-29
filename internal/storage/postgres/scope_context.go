// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// readScopeContexts must run in the caller's snapshot or serialized transaction.
func (s *Store) readScopeContexts(ctx context.Context, roots []string) (map[string]storage.ScopeContext, error) {
	// UNION visits each (root, source) once, including in diamonds and cycles;
	// it does not enumerate exponentially many ancestry paths.
	rows, err := s.query(ctx, `WITH RECURSIVE ancestry(root,slug) AS (
		SELECT slug,slug FROM specs WHERE project_slug=$1 AND slug=ANY($2)
		UNION
		SELECT a.root,p.slug FROM ancestry a
		JOIN edges e ON e.project_slug=$1 AND e.to_slug=a.slug AND e.edge_type='COMPOSES'
		JOIN specs p ON p.project_slug=$1 AND p.slug=e.from_slug
	) SELECT a.root,s.id,s.slug,s.scope_revision::text,spec_scope_contract(s.intent,s.shape_output,s.specify_output)
	FROM ancestry a JOIN specs s ON s.project_slug=$1 AND s.slug=a.slug
	ORDER BY a.root COLLATE "C",s.slug COLLATE "C"`, s.project, roots)
	if err != nil {
		return nil, fmt.Errorf("postgres: scope sources: %w", err)
	}
	type sourceRow struct {
		root   string
		source storage.SpecScopeBaseline
	}
	sources, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceRow, error) {
		var item sourceRow
		scanErr := row.Scan(&item.root, &item.source.ID, &item.source.Slug, &item.source.Revision, &item.source.Contract)
		if scanErr != nil {
			return item, fmt.Errorf("postgres: readScopeContexts: %w", scanErr)
		}
		return item, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: readScopeContexts: %w", err)
	}
	contexts := make(map[string]storage.ScopeContext, len(roots))
	members := make(map[string]map[string]bool, len(roots))
	allSources := make(map[string]bool)
	var slugs []string
	for _, item := range sources {
		current, exists := contexts[item.root]
		if !exists {
			current = storage.ScopeContext{Sources: []storage.SpecScopeBaseline{}, Relations: []storage.ScopeRelation{}, Decisions: []*storage.Decision{}}
			members[item.root] = make(map[string]bool)
		}
		current.Sources = append(current.Sources, item.source)
		contexts[item.root] = current
		members[item.root][item.source.Slug] = true
		if !allSources[item.source.Slug] {
			allSources[item.source.Slug] = true
			slugs = append(slugs, item.source.Slug)
		}
	}
	for _, root := range roots {
		if _, exists := contexts[root]; !exists {
			return nil, storage.ErrSpecNotFound
		}
	}
	rows, err = s.query(ctx, `SELECT e.from_slug,e.to_slug,e.edge_type FROM edges e WHERE e.project_slug=$1 AND (
		(e.edge_type='DECIDED_IN' AND e.from_slug=ANY($2)) OR
		(e.edge_type='COMPOSES' AND e.to_slug=ANY($2) AND NOT EXISTS (
			SELECT 1 FROM slices n WHERE n.project_slug=e.project_slug AND n.slug=e.from_slug)))
		ORDER BY e.edge_type COLLATE "C",e.from_slug COLLATE "C",e.to_slug COLLATE "C"`, s.project, slugs)
	if err != nil {
		return nil, fmt.Errorf("postgres: scope relations: %w", err)
	}
	relations, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.ScopeRelation, error) {
		var relation storage.ScopeRelation
		scanErr := row.Scan(&relation.From, &relation.To, &relation.Type)
		if scanErr != nil {
			return relation, fmt.Errorf("postgres: readScopeContexts: %w", scanErr)
		}
		return relation, nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: readScopeContexts: %w", err)
	}
	decisionSlugs := []string{}
	for _, relation := range relations {
		if relation.Type == "DECIDED_IN" {
			decisionSlugs = append(decisionSlugs, relation.To)
		}
	}
	rows, err = s.query(ctx, `SELECT `+decisionSelectCols+` FROM decisions d WHERE d.project_slug=$1 AND d.slug=ANY($2) ORDER BY d.slug COLLATE "C"`, s.project, decisionSlugs)
	if err != nil {
		return nil, fmt.Errorf("postgres: scope decisions: %w", err)
	}
	decisions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*storage.Decision, error) { return scanDecisionRow(row) })
	if err != nil {
		return nil, fmt.Errorf("postgres: readScopeContexts: %w", err)
	}
	foundDecisions := make(map[string]bool, len(decisions))
	for _, decision := range decisions {
		foundDecisions[decision.Slug] = true
	}
	for _, slug := range decisionSlugs {
		if !foundDecisions[slug] {
			return nil, fmt.Errorf("scope decision %q: %w", slug, storage.ErrDecisionNotFound)
		}
	}
	for root, current := range contexts {
		linked := make(map[string]bool)
		for _, relation := range relations {
			if relation.Type == "COMPOSES" && members[root][relation.To] {
				if !members[root][relation.From] {
					return nil, fmt.Errorf("scope parent %q: %w", relation.From, storage.ErrSpecNotFound)
				}
				current.Relations = append(current.Relations, relation)
			} else if relation.Type == "DECIDED_IN" && members[root][relation.From] {
				current.Relations = append(current.Relations, relation)
				linked[relation.To] = true
			}
		}
		for _, decision := range decisions {
			if linked[decision.Slug] {
				current.Decisions = append(current.Decisions, decision)
			}
		}
		contexts[root] = current
	}
	return contexts, nil
}
