// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// ListSpecSubdivisions includes immutable child lineage, even after detachment.
func (s *Store) ListSpecSubdivisions(ctx context.Context, slug string) (storage.SubdivisionList, error) {
	var result storage.SubdivisionList
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var exists bool
		if err := s.queryRow(snapshotCtx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, slug).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: ListSpecSubdivisions: %w", err)
		}
		if !exists {
			return storage.ErrSpecNotFound
		}
		rows, err := s.query(snapshotCtx, `SELECT d.id,d.parent_slug,d.actor,d.reason,d.created_at FROM subdivisions d
			WHERE d.project_slug=$1 AND (d.parent_slug=$2 OR EXISTS(SELECT 1 FROM subdivision_children c WHERE c.project_slug=d.project_slug AND c.operation_id=d.id AND c.child_slug=$2))
			ORDER BY d.created_at DESC,d.id DESC LIMIT 21`, s.project, slug)
		if err != nil {
			return fmt.Errorf("postgres: list subdivisions: %w", err)
		}
		result.Items, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (storage.SubdivisionReference, error) {
			var item storage.SubdivisionReference
			scanErr := row.Scan(&item.ID, &item.Parent, &item.Actor, &item.Reason, &item.CreatedAt)
			if scanErr != nil {
				return item, fmt.Errorf("postgres: ListSpecSubdivisions: %w", scanErr)
			}
			return item, nil
		})
		if err != nil {
			return fmt.Errorf("postgres: ListSpecSubdivisions: %w", err)
		}
		result.HasMore = len(result.Items) > 20
		if result.HasMore {
			result.Items = result.Items[:20]
		}
		return nil
	})
	if err != nil {
		return storage.SubdivisionList{}, err
	}
	return result, nil
}

// ReadSubdivision combines recorded source identity with current scope on demand.
func (s *Store) ReadSubdivision(ctx context.Context, id string) (storage.SubdivisionState, error) {
	var result storage.SubdivisionState
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		err := s.queryRow(snapshotCtx, `SELECT d.id,d.parent_slug,d.result_version,p.id,d.source_version
			FROM subdivisions d JOIN specs p ON p.project_slug=d.project_slug AND p.slug=d.parent_slug
			WHERE d.project_slug=$1 AND d.id=$2`, s.project, id).
			Scan(&result.Receipt.ID, &result.Receipt.Parent, &result.Receipt.ParentVersion, &result.SourceReference.ID, &result.SourceReference.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrSubdivisionNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read subdivision: %w", err)
		}
		parent := result.Receipt.Parent
		result.SourceReference.Slug = parent
		rows, err := s.query(snapshotCtx, `SELECT c.child_slug,
			EXISTS(SELECT 1 FROM edges e WHERE e.project_slug=c.project_slug AND e.from_slug=$3 AND e.to_slug=c.child_slug AND e.edge_type='COMPOSES')
			FROM subdivision_children c
			WHERE c.project_slug=$1 AND c.operation_id=$2 ORDER BY c.child_slug COLLATE "C"`, s.project, id, parent)
		if err != nil {
			return err
		}
		type childRow struct {
			slug     string
			attached bool
		}
		children, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (childRow, error) {
			var child childRow
			scanErr := row.Scan(&child.slug, &child.attached)
			if scanErr != nil {
				return child, fmt.Errorf("postgres: ReadSubdivision: %w", scanErr)
			}
			return child, nil
		})
		if err != nil {
			return fmt.Errorf("postgres: subdivision children: %w", err)
		}
		slugs := []string{parent}
		result.Receipt.ChildSlugs = make([]string, 0, len(children))
		for _, child := range children {
			slugs = append(slugs, child.slug)
			result.Receipt.ChildSlugs = append(result.Receipt.ChildSlugs, child.slug)
		}
		specs, err := s.BatchGetSpecs(snapshotCtx, slugs)
		if err != nil {
			return err
		}
		contexts, err := s.readScopeContexts(snapshotCtx, slugs)
		if err != nil {
			return err
		}
		result.Parent, result.ParentScope = specs[parent], contexts[parent]
		result.Children = make([]storage.SubdivisionChildState, 0, len(children))
		for _, child := range children {
			result.Children = append(result.Children, storage.SubdivisionChildState{Spec: specs[child.slug], Scope: contexts[child.slug], Attached: child.attached})
		}
		return nil
	})
	if err != nil {
		return storage.SubdivisionState{}, err
	}
	return result, nil
}
