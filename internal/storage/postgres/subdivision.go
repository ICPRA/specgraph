// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// Matches the native spec boundary; storage cannot import the server validator.
var subdivisionSlugPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9_/-]*[a-z0-9])?$`)
var subdivisionKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// SubdivideSpec creates child work and lightweight lineage in one transaction.
// It does not approve or complete nodes, or release any execution responsibility.
func (s *Store) SubdivideSpec(ctx context.Context, parent, actor string, request storage.SubdivideRequest) (storage.SubdivisionResult, error) {
	if request.ExpectedVersion < 1 || strings.TrimSpace(actor) == "" ||
		strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 10000 ||
		len(request.Children) < 1 || len(request.Children) > 32 {
		return storage.SubdivisionResult{}, fmt.Errorf("baseline, identity, reason or child count: %w", storage.ErrInvalidSubdivisionRequest)
	}
	seen := map[string]bool{parent: true}
	for _, child := range request.Children {
		if !subdivisionSlugPattern.MatchString(child.Slug) || len(child.Slug) > 256 || seen[child.Slug] ||
			strings.TrimSpace(child.Intent) == "" || len(child.Intent) > 10000 ||
			!storage.SpecPriority(child.Priority).IsValid() || !storage.SpecComplexity(child.Complexity).IsValid() {
			return storage.SubdivisionResult{}, fmt.Errorf("child %q: %w", child.Slug, storage.ErrInvalidSubdivisionRequest)
		}
		seen[child.Slug] = true
	}
	var result storage.SubdivisionResult
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var version int32
		if err := s.queryRow(txCtx, `SELECT version FROM specs WHERE project_slug = $1 AND slug = $2 FOR UPDATE`, s.project, parent).Scan(&version); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: subdivision parent lock: %w", err)
		}
		if version != request.ExpectedVersion {
			return storage.ErrConcurrentModification
		}
		rows, err := s.query(txCtx, `SELECT child.slug FROM edges e
			JOIN specs child ON child.project_slug = e.project_slug AND child.slug = e.to_slug
			WHERE e.project_slug = $1 AND e.from_slug = $2 AND e.edge_type = 'COMPOSES'`, s.project, parent)
		if err != nil {
			return err
		}
		children, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("postgres: subdivision child baseline: %w", err)
		}
		slices.Sort(children)
		specs, err := s.BatchGetSpecs(txCtx, []string{parent})
		if err != nil {
			return err
		}
		spec := specs[parent]
		result = storage.SubdivisionResult{ID: newID("sub"), Parent: parent, ParentVersion: version + 1,
			ChildSlugs: make([]string, 0, len(request.Children))}
		childIDs := make([]string, 0, len(request.Children))
		for _, draft := range request.Children {
			exists, err := s.nodeExists(txCtx, draft.Slug)
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("subdivision child %q: %w", draft.Slug, storage.ErrSpecAlreadyExists)
			}
			child, err := s.CreateSpec(txCtx, draft.Slug, draft.Intent, draft.Priority, draft.Complexity,
				storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			if err != nil {
				return err
			}
			if _, err := s.AddEdge(txCtx, parent, child.Slug, storage.EdgeTypeComposes); err != nil {
				return err
			}
			result.ChildSlugs = append(result.ChildSlugs, child.Slug)
			childIDs = append(childIDs, child.ID)
		}
		now := s.now()
		if _, err := s.exec(txCtx, `UPDATE specs SET role = 'summary', version = version + 1, updated_at = $1
			WHERE project_slug = $2 AND slug = $3`, now, s.project, parent); err != nil {
			return fmt.Errorf("postgres: subdivision parent role: %w", err)
		}
		newChildren := append(slices.Clone(children), result.ChildSlugs...)
		slices.Sort(newChildren)
		deltas := []storage.FieldChange{{Field: "children", OldValue: strings.Join(children, "\n"), NewValue: strings.Join(newChildren, "\n")}}
		if spec.Role != storage.SpecRoleSummary {
			deltas = append(deltas, storage.FieldChange{Field: "role", OldValue: string(spec.Role), NewValue: string(storage.SpecRoleSummary)})
		}
		if err := s.createChangeLog(txCtx, parent, &storage.ChangeLogEntry{
			Version: result.ParentVersion, Stage: string(spec.Stage), ContentHash: spec.ContentHash,
			Checkpoint: true, Summary: fmt.Sprintf("Spec subdivided by %s; children: %s", actor, strings.Join(result.ChildSlugs, ", ")), Reason: request.Reason, Date: now,
		}, deltas); err != nil {
			return err
		}
		if _, err := s.exec(txCtx, `INSERT INTO subdivisions
			(id, project_slug, parent_slug, actor, reason, source_version, result_version, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			result.ID, s.project, parent, actor, request.Reason, version, result.ParentVersion, now); err != nil {
			return fmt.Errorf("postgres: subdivision receipt: %w", err)
		}
		for i, slug := range result.ChildSlugs {
			if _, err := s.exec(txCtx, `INSERT INTO subdivision_children(operation_id, project_slug, child_slug, child_id) VALUES ($1,$2,$3,$4)`, result.ID, s.project, slug, childIDs[i]); err != nil {
				return fmt.Errorf("postgres: subdivision lineage: %w", err)
			}
		}
		slices.Sort(result.ChildSlugs)
		return nil
	})
	if err != nil {
		return storage.SubdivisionResult{}, err
	}
	return result, nil
}
