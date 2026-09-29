// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ReadRequirementChangePreview reads Spec-only structural candidates in one snapshot.
func (s *Store) ReadRequirementChangePreview(ctx context.Context, slug string) (storage.RequirementChangePreview, error) {
	var result storage.RequirementChangePreview
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var err error
		result, err = s.readRequirementChangePreviewInSnapshot(snapshotCtx, slug)
		return err
	})
	if err != nil {
		return storage.RequirementChangePreview{}, err
	}
	return result, nil
}

// The caller owns either a read snapshot or the project-locked write transaction.
func (s *Store) readRequirementChangePreviewInSnapshot(ctx context.Context, slug string) (storage.RequirementChangePreview, error) {
	result := storage.RequirementChangePreview{SpecSlug: slug}
	contexts, err := s.readScopeContexts(ctx, []string{slug})
	if err != nil {
		return storage.RequirementChangePreview{}, err
	}
	result.Scope = contexts[slug]
	candidates, err := s.requirementChangeCandidates(ctx, []string{slug})
	if err != nil {
		return storage.RequirementChangePreview{}, err
	}
	rows, err := s.query(ctx, `SELECT id,slug,intent,stage,role,version,scope_revision::text
		FROM specs WHERE project_slug=$1 AND slug=ANY($2) ORDER BY slug COLLATE "C"`, s.project, candidates[slug])
	if err != nil {
		return storage.RequirementChangePreview{}, fmt.Errorf("postgres: requirement change candidates: %w", err)
	}
	result.Nodes, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.ChangeImpactNode, error) {
		node := storage.ChangeImpactNode{ParentSlugs: []string{}}
		if scanErr := row.Scan(&node.ID, &node.Slug, &node.Intent, &node.Stage, &node.Role, &node.Version, &node.ScopeRevision); scanErr != nil {
			return node, fmt.Errorf("postgres: scan requirement change candidate: %w", scanErr)
		}
		return node, nil
	})
	if err != nil {
		return storage.RequirementChangePreview{}, fmt.Errorf("postgres: collect requirement change candidates: %w", err)
	}
	slugs := make([]string, len(result.Nodes))
	indices := make(map[string]int, len(result.Nodes))
	for i := range result.Nodes {
		node := &result.Nodes[i]
		slugs[i] = node.Slug
		indices[node.Slug] = i
	}
	rows, err = s.query(ctx, `SELECT e.from_slug,e.to_slug,e.edge_type FROM edges e
			JOIN specs f ON f.project_slug=e.project_slug AND f.slug=e.from_slug
			JOIN specs t ON t.project_slug=e.project_slug AND t.slug=e.to_slug
			WHERE e.project_slug=$1 AND e.edge_type IN ('COMPOSES','DEPENDS_ON','BLOCKS')
			AND e.to_slug=ANY($2) AND (e.from_slug=ANY($2) OR e.edge_type='COMPOSES')
			ORDER BY e.edge_type COLLATE "C",e.from_slug COLLATE "C",e.to_slug COLLATE "C"`, s.project, slugs)
	if err != nil {
		return storage.RequirementChangePreview{}, fmt.Errorf("postgres: requirement change relations: %w", err)
	}
	result.Relations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.ScopeRelation, error) {
		var relation storage.ScopeRelation
		if scanErr := row.Scan(&relation.From, &relation.To, &relation.Type); scanErr != nil {
			return relation, fmt.Errorf("postgres: scan requirement change relation: %w", scanErr)
		}
		return relation, nil
	})
	if err != nil {
		return storage.RequirementChangePreview{}, fmt.Errorf("postgres: collect requirement change relations: %w", err)
	}
	for _, relation := range result.Relations {
		if relation.Type == "COMPOSES" {
			i := indices[relation.To]
			result.Nodes[i].ParentSlugs = append(result.Nodes[i].ParentSlugs, relation.From)
		}
	}
	return result, nil
}

// Both callers use the same mixed-edge closure, including dependencies reached
// between containment hops. UNION visits nodes, not exponentially many paths.
func (s *Store) requirementChangeCandidates(ctx context.Context, seeds []string) (map[string][]string, error) {
	rows, err := s.query(ctx, `WITH RECURSIVE links(from_slug,to_slug) AS (
		SELECT CASE WHEN e.edge_type='DEPENDS_ON' THEN e.to_slug ELSE e.from_slug END,
		       CASE WHEN e.edge_type='DEPENDS_ON' THEN e.from_slug ELSE e.to_slug END
		FROM edges e
		JOIN specs f ON f.project_slug=e.project_slug AND f.slug=e.from_slug
		JOIN specs t ON t.project_slug=e.project_slug AND t.slug=e.to_slug
		WHERE e.project_slug=$1 AND e.edge_type IN ('COMPOSES','DEPENDS_ON','BLOCKS')
	), candidates(seed,slug) AS (
		SELECT slug,slug FROM specs WHERE project_slug=$1 AND slug=ANY($2)
		UNION
		SELECT c.seed,l.to_slug FROM candidates c JOIN links l ON l.from_slug=c.slug
	) SELECT seed,slug FROM candidates ORDER BY seed COLLATE "C",slug COLLATE "C"`, s.project, seeds)
	if err != nil {
		return nil, fmt.Errorf("postgres: requirement change candidates: %w", err)
	}
	defer rows.Close()
	result := make(map[string][]string, len(seeds))
	for rows.Next() {
		var seed, slug string
		if scanErr := rows.Scan(&seed, &slug); scanErr != nil {
			return nil, fmt.Errorf("postgres: scan requirement change candidate: %w", scanErr)
		}
		result[seed] = append(result[seed], slug)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return result, fmt.Errorf("postgres: read requirement change candidate rows: %w", rowsErr)
	}
	return result, nil
}
