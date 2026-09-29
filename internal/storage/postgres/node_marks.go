// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

// NodeMark records an attributed judgment, not execution eligibility or a spec change.
type NodeMark struct {
	ID        string    `json:"id"`
	TaskSlug  string    `json:"taskSlug" db:"task_slug"`
	Kind      string    `json:"kind"`
	Value     string    `json:"value"`
	Reason    string    `json:"reason"`
	Actor     string    `json:"actor"`
	CreatedAt time.Time `json:"createdAt" db:"created_at"`
}

// NodeMarkHistory pages immutable revisions by their decimal identity cursor.
type NodeMarkHistory struct {
	Items      []NodeMark `json:"items"`
	HasMore    bool       `json:"hasMore"`
	NextCursor string     `json:"nextCursor"`
}

const nodeMarkColumns = `id::text,task_slug,kind,value,reason,actor,created_at`

// SetNodeMark appends a validated operator request without updating the spec.
func (s *Store) SetNodeMark(ctx context.Context, slug, kind, value, reason, actor string) (*NodeMark, error) {
	return s.setNodeMark(ctx, slug, kind, value, reason, actor, nil)
}

// SetNodeMarkIfCurrent appends only if the observed revision for this kind is current.
// An empty expected ID requires that this kind has no recorded judgment yet.
func (s *Store) SetNodeMarkIfCurrent(ctx context.Context, slug, kind, value, reason, actor, expectedID string) (*NodeMark, error) {
	return s.setNodeMark(ctx, slug, kind, value, reason, actor, &expectedID)
}

func (s *Store) setNodeMark(ctx context.Context, slug, kind, value, reason, actor string, expectedID *string) (*NodeMark, error) {
	var mark NodeMark
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		if expectedID != nil {
			var current string
			if err := s.queryRow(txCtx, `SELECT COALESCE((SELECT id::text FROM node_marks
				WHERE project_slug=$1 AND task_slug=$2 AND kind=$3 ORDER BY id DESC LIMIT 1),'')`,
				s.project, slug, kind).Scan(&current); err != nil {
				return fmt.Errorf("postgres: read current node mark: %w", err)
			}
			if current != *expectedID {
				return storage.ErrConcurrentModification
			}
		}
		err := s.queryRow(txCtx, `INSERT INTO node_marks(project_slug,task_slug,kind,value,reason,actor)
		SELECT project_slug,slug,$3,$4,$5,$6 FROM specs WHERE project_slug=$1 AND slug=$2
		RETURNING `+nodeMarkColumns, s.project, slug, kind, value, reason, actor).
			Scan(&mark.ID, &mark.TaskSlug, &mark.Kind, &mark.Value, &mark.Reason, &mark.Actor, &mark.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrSpecNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: append node mark: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &mark, nil
}

// ReadNodeMarkHistory reads up to 50 revisions before a validated positive cursor.
// A zero cursor starts at the newest revision.
func (s *Store) ReadNodeMarkHistory(ctx context.Context, slug string, beforeID int64) (*NodeMarkHistory, error) {
	var exists bool
	if err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, slug).Scan(&exists); err != nil {
		return nil, fmt.Errorf("postgres: ReadNodeMarkHistory: %w", err)
	}
	if !exists {
		return nil, storage.ErrSpecNotFound
	}
	rows, err := s.query(ctx, `SELECT `+nodeMarkColumns+` FROM node_marks
		WHERE project_slug=$1 AND task_slug=$2 AND ($3::bigint=0 OR id<$3)
		ORDER BY node_marks.id DESC LIMIT 51`, s.project, slug, beforeID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read node mark history: %w", err)
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[NodeMark])
	if err != nil {
		return nil, fmt.Errorf("postgres: ReadNodeMarkHistory: %w", err)
	}
	page := &NodeMarkHistory{Items: items, HasMore: len(items) > 50}
	if page.HasMore {
		page.Items = items[:50]
		page.NextCursor = page.Items[49].ID
	}
	return page, nil
}
