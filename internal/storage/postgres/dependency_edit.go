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

// AddDependency adds a prerequisite unless either supported representation exists.
// The caller owns authentication, authorization and input validation.
func (s *Store) AddDependency(ctx context.Context, dependent, actor string, request storage.DependencyEditRequest) (storage.DependencyEditResult, error) {
	return s.editDependency(ctx, dependent, actor, "add", request)
}

// RemoveDependency removes both representations of the requested prerequisite.
// The caller owns authentication, authorization and input validation.
func (s *Store) RemoveDependency(ctx context.Context, dependent, actor string, request storage.DependencyEditRequest) (storage.DependencyEditResult, error) {
	return s.editDependency(ctx, dependent, actor, "remove", request)
}

func (s *Store) editDependency(ctx context.Context, dependent, actor, operation string, request storage.DependencyEditRequest) (storage.DependencyEditResult, error) {
	var result storage.DependencyEditResult
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var previous storage.DependencyEditRequest
		var previousActor, previousDependent string
		replayErr := s.queryRow(txCtx, `
			SELECT dependent_slug, prerequisite_slug, actor, reason, expected_version,
			       expected_prerequisite_version, expected_revision, result_revision, changed, operation
			FROM dependency_edits WHERE project_slug = $1 AND idempotency_key = $2`,
			s.project, request.IdempotencyKey,
		).Scan(&previousDependent, &previous.Prerequisite, &previousActor, &previous.Reason,
			&previous.ExpectedVersion, &previous.ExpectedPrerequisiteVersion, &previous.ExpectedRevision,
			&result.Revision, &result.Changed, &result.Operation)
		if replayErr == nil {
			previous.IdempotencyKey = request.IdempotencyKey
			if previousActor != actor || previousDependent != dependent || previous != request || result.Operation != operation {
				return storage.ErrConcurrentModification
			}
			result.Dependent, result.Prerequisite, result.Replayed = dependent, request.Prerequisite, true
			return nil
		}
		if !errors.Is(replayErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: dependency edit replay: %w", replayErr)
		}
		// Match LifecycleSupersedeSpec's byte order, independent of DB locale.
		rows, err := s.query(txCtx, `SELECT slug FROM specs WHERE project_slug = $1 AND slug = ANY($2) ORDER BY slug COLLATE "C" FOR UPDATE`, s.project, []string{dependent, request.Prerequisite})
		if err != nil {
			return err
		}
		if _, collectErr := pgx.CollectRows(rows, pgx.RowTo[string]); collectErr != nil {
			return fmt.Errorf("postgres: dependency edit endpoint locks: %w", collectErr)
		}
		spec, err := s.GetSpec(txCtx, dependent)
		if err != nil {
			return err
		}
		prerequisite, err := s.GetSpec(txCtx, request.Prerequisite)
		if err != nil {
			return err
		}
		revision, err := s.dependencyRevision(txCtx, dependent)
		if err != nil {
			return err
		}
		if spec.Version != request.ExpectedVersion || prerequisite.Version != request.ExpectedPrerequisiteVersion || revision != request.ExpectedRevision {
			return storage.ErrConcurrentModification
		}
		if operation == "add" {
			var exists bool
			if scanErr := s.queryRow(txCtx, `SELECT EXISTS (
				SELECT 1 FROM edges WHERE project_slug = $1 AND
				((from_slug = $2 AND to_slug = $3 AND edge_type = 'DEPENDS_ON') OR
				 (from_slug = $3 AND to_slug = $2 AND edge_type = 'BLOCKS'))
			)`, s.project, dependent, request.Prerequisite).Scan(&exists); scanErr != nil {
				return fmt.Errorf("postgres: dependency edit existing prerequisite: %w", scanErr)
			}
			if !exists {
				if dependent == request.Prerequisite {
					return storage.ErrDependencyCycle
				}
				prerequisites, getTransitiveDepsErr := s.GetTransitiveDeps(txCtx, request.Prerequisite)
				if getTransitiveDepsErr != nil {
					return getTransitiveDepsErr
				}
				for _, prerequisite := range prerequisites {
					if prerequisite.Slug == dependent {
						return storage.ErrDependencyCycle
					}
				}
				if _, err := s.AddEdge(txCtx, dependent, request.Prerequisite, storage.EdgeTypeDependsOn); err != nil {
					return err
				}
			}
		} else {
			if removeEdgeErr := s.RemoveEdge(txCtx, dependent, request.Prerequisite, storage.EdgeTypeDependsOn); removeEdgeErr != nil {
				return removeEdgeErr
			}
			if removeEdgeErr := s.RemoveEdge(txCtx, request.Prerequisite, dependent, storage.EdgeTypeBlocks); removeEdgeErr != nil {
				return removeEdgeErr
			}
		}
		resultRevision, err := s.dependencyRevision(txCtx, dependent)
		if err != nil {
			return err
		}
		result = storage.DependencyEditResult{
			Operation: operation,
			Dependent: dependent, Prerequisite: request.Prerequisite,
			Revision: resultRevision, Changed: resultRevision != revision,
		}
		if _, err := s.exec(txCtx, `
			INSERT INTO dependency_edits (id, project_slug, dependent_slug, prerequisite_slug,
			    actor, reason, idempotency_key, expected_version, expected_prerequisite_version,
			    expected_revision, result_revision, changed, created_at, operation)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			newID("de"), s.project, dependent, request.Prerequisite, actor, request.Reason,
			request.IdempotencyKey, request.ExpectedVersion, request.ExpectedPrerequisiteVersion,
			request.ExpectedRevision, result.Revision, result.Changed, s.now(), operation,
		); err != nil {
			return fmt.Errorf("postgres: dependency edit receipt: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.DependencyEditResult{}, err
	}
	return result, nil
}

// ReadDependencyEditState returns a consistent baseline and bounded operator history.
func (s *Store) ReadDependencyEditState(ctx context.Context, slug string) (storage.DependencyEditState, error) {
	result := storage.DependencyEditState{Operations: make([]storage.DependencyEditRecord, 0)}
	err := s.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		if err := s.queryRow(txCtx, `SELECT version FROM specs WHERE project_slug = $1 AND slug = $2`, s.project, slug).Scan(&result.SpecVersion); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: dependency edit spec version: %w", err)
		}
		var err error
		result.Revision, err = s.dependencyRevision(txCtx, slug)
		if err != nil {
			return err
		}
		rows, err := s.query(txCtx, `
			SELECT id, actor, reason, prerequisite_slug, result_revision, changed, created_at, operation
			FROM dependency_edits WHERE project_slug = $1 AND dependent_slug = $2
			ORDER BY created_at DESC, id DESC LIMIT 20`, s.project, slug)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var record storage.DependencyEditRecord
			if err := rows.Scan(&record.ID, &record.Actor, &record.Reason, &record.Prerequisite, &record.Revision, &record.Changed, &record.CreatedAt, &record.Operation); err != nil {
				return fmt.Errorf("postgres: dependency edit receipt scan: %w", err)
			}
			result.Operations = append(result.Operations, record)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("postgres: dependency edit receipt rows: %w", err)
		}
		return nil
	})
	if err != nil {
		return storage.DependencyEditState{}, err
	}
	return result, nil
}
