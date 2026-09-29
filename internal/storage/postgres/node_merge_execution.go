// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"

	"github.com/specgraph/specgraph/internal/storage"
)

// Merge receipts, not editable SUPERSEDES edges, carry the recursive source identity.
func (s *Store) mergedSourceResponsibility(ctx context.Context, target string) (string, string, error) {
	rows, err := s.query(ctx, `WITH RECURSIVE lineage(slug) AS (
 SELECT x.source_slug FROM node_merges m JOIN node_merge_sources x ON x.project_slug=m.project_slug AND x.merge_id=m.id
 WHERE m.project_slug=$1 AND m.target_slug=$2
 UNION
 SELECT x.source_slug FROM lineage l JOIN node_merges m ON m.project_slug=$1 AND m.target_slug=l.slug
 JOIN node_merge_sources x ON x.project_slug=m.project_slug AND x.merge_id=m.id
 ) SELECT l.slug,
 s.human_owner_user_id IS NOT NULL,
 EXISTS(SELECT 1 FROM node_ownership_operations o WHERE o.project_slug=$1 AND o.task_slug=l.slug AND o.status='pending'),
 EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=$1 AND d.task_slug=l.slug AND d.released_at IS NULL),
 EXISTS(SELECT 1 FROM run_bindings b WHERE b.project_slug=$1 AND b.task_spec_slug=l.slug AND b.state IN('prepared','bound')
 AND NOT EXISTS(SELECT 1 FROM run_dispatches d WHERE d.project_slug=b.project_slug AND d.run_id=b.id)
 AND NOT EXISTS(SELECT 1 FROM run_preparation_cancellations c WHERE c.project_slug=b.project_slug AND c.run_id=b.id)),
 EXISTS(SELECT 1 FROM claims c WHERE c.project_slug=$1 AND c.spec_slug=l.slug AND c.lease_expires>now())
 FROM lineage l JOIN specs s ON s.project_slug=$1 AND s.slug=l.slug ORDER BY l.slug COLLATE "C"`, s.project, target)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()
	for rows.Next() {
		var slug string
		var owner, pending, dispatch, preparation, claim bool
		if err := rows.Scan(&slug, &owner, &pending, &dispatch, &preparation, &claim); err != nil {
			return "", "", err
		}
		switch {
		case owner:
			return slug, "human_owner", nil
		case pending:
			return slug, "pending_takeover", nil
		case dispatch:
			return slug, "dispatch", nil
		case preparation:
			return slug, "preparation", nil
		case claim:
			return slug, "claim", nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", "", err
	}
	return "", "", nil
}

func (s *Store) rejectMergedSourceExecution(ctx context.Context, target string) error {
	slug, reason, err := s.mergedSourceResponsibility(ctx, target)
	if err != nil {
		return err
	}
	switch reason {
	case "human_owner":
		return fmt.Errorf("merge source %q: %w", slug, storage.ErrNodeOwnershipConflict)
	case "pending_takeover":
		return fmt.Errorf("merge source %q: %w", slug, storage.ErrNodeOwnershipPending)
	case "dispatch", "preparation":
		return fmt.Errorf("merge source %q: %w", slug, storage.ErrDispatchResponsibilityHeld)
	case "claim":
		return fmt.Errorf("merge source %q: %w", slug, storage.ErrSpecAlreadyClaimed)
	}
	return nil
}
