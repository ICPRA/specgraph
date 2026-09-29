// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"

	"github.com/specgraph/specgraph/internal/storage"
)

// lockDependencyState must precede spec and edge writes in the transaction.
// ponytail: project-wide serialization; partition by dependent only if contention requires it.
func (s *Store) lockDependencyState(ctx context.Context) error {
	var project string
	if err := s.queryRow(ctx, `SELECT slug FROM projects WHERE slug = $1 FOR NO KEY UPDATE`, s.project).Scan(&project); err != nil {
		return fmt.Errorf("postgres: lock dependency state: %w", err)
	}
	return nil
}

// dependencyRevision is a mechanical change marker, not a semantic comparison.
func (s *Store) dependencyRevision(ctx context.Context, slug string) (int64, error) {
	var revision int64
	if err := s.queryRow(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM dependency_changes WHERE project_slug = $1 AND dependent_slug = $2`,
		s.project, slug,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("postgres: dependency revision for %q: %w", slug, err)
	}
	return revision, nil
}

// checkPrerequisites requires every direct prerequisite to be an actual done spec.
func (s *Store) checkPrerequisites(ctx context.Context, slug string) error {
	rows, err := s.query(ctx, `
			SELECT prerequisite.slug,COALESCE(specs.stage,''),COALESCE(specs.role,'') FROM (
				SELECT to_slug AS slug FROM edges
				WHERE project_slug = $1 AND from_slug = $2 AND edge_type = 'DEPENDS_ON'
				UNION ALL
				SELECT from_slug AS slug FROM edges
				WHERE project_slug = $1 AND to_slug = $2 AND edge_type = 'BLOCKS'
			) prerequisite
			LEFT JOIN specs ON specs.project_slug = $1 AND specs.slug = prerequisite.slug
			`, s.project, slug)
	if err != nil {
		return fmt.Errorf("postgres: check prerequisites for %q: %w", slug, err)
	}
	var summaries []string
	blocked := false
	for rows.Next() {
		var prerequisite, stage, role string
		if err := rows.Scan(&prerequisite, &stage, &role); err != nil {
			rows.Close()
			return fmt.Errorf("postgres: checkPrerequisites: %w", err)
		}
		if role == "summary" {
			summaries = append(summaries, prerequisite)
		} else if stage != "done" {
			blocked = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("postgres: checkPrerequisites: %w", err)
	}
	rows.Close()
	if !blocked && len(summaries) > 0 {
		g, err := s.loadSummaryGraph(ctx)
		if err != nil {
			return err
		}
		for _, goal := range summaries {
			state, err := s.summaryState(ctx, g, goal)
			if err != nil {
				return err
			}
			if !state.Accepted {
				blocked = true
				break
			}
		}
	}
	if blocked {
		return fmt.Errorf("postgres: prerequisites for %q: %w", slug, storage.ErrDependenciesNotReady)
	}
	return nil
}
