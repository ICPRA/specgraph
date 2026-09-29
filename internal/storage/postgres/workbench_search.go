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

// WorkbenchKnowledgeItem points to an original stored record and its version.
type WorkbenchKnowledgeItem struct {
	Kind      string    `json:"kind"`
	ID        string    `json:"id"`
	Slug      string    `json:"slug"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	Version   int32     `json:"version"`
	Excerpt   string    `json:"excerpt"`
	UpdatedAt time.Time `json:"updatedAt" db:"updated_at"`
}

// WorkbenchKnowledgeSearch covers stored records, not files or conversations.
type WorkbenchKnowledgeSearch struct {
	Query   string                   `json:"query"`
	Scope   string                   `json:"scope"`
	HasMore bool                     `json:"hasMore"`
	Items   []WorkbenchKnowledgeItem `json:"items"`
}

// ErrWorkbenchChangeNotFound identifies an absent project-scoped change record.
var ErrWorkbenchChangeNotFound = errors.New("workbench change record not found")

// Association errors require an explicit host mapping, never a default project.
var (
	ErrWorkbenchProjectAssociationMissing   = errors.New("no SpecGraph project association for the native project")
	ErrWorkbenchProjectAssociationAmbiguous = errors.New("multiple SpecGraph project associations for the native project")
)

// ProjectForWorkbenchKnowledge resolves recorded operator selections, not authorization.
// Historical packages remain associations even when their run was cancelled.
func (s *Store) ProjectForWorkbenchKnowledge(ctx context.Context, environmentID, nativeProjectID string) (string, error) {
	rows, err := s.query(ctx, `SELECT DISTINCT project_slug FROM context_packages
		WHERE (body->'dispatch_target'->>'environmentId'=$1
		AND body->'dispatch_target'->>'projectId'=$2)
		OR (body->'program_target'->>'environmentId'=$1
		AND body->'program_target'->>'nativeProjectId'=$2) LIMIT 2`, environmentID, nativeProjectID)
	if err != nil {
		return "", fmt.Errorf("postgres: resolve knowledge project association: %w", err)
	}
	projects, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", fmt.Errorf("postgres: ProjectForWorkbenchKnowledge: %w", err)
	}
	switch len(projects) {
	case 0:
		return "", ErrWorkbenchProjectAssociationMissing
	case 1:
		return projects[0], nil
	default:
		return "", ErrWorkbenchProjectAssociationAmbiguous
	}
}

// WorkbenchKnowledgeRecord contains the source record, not a reconstructed search snapshot.
type WorkbenchKnowledgeRecord struct {
	Kind             string             `json:"kind"`
	ID               string             `json:"id"`
	Slug             string             `json:"slug"`
	Version          int32              `json:"version"`
	SpecgraphProject string             `json:"specgraphProject"`
	Record           any                `json:"record"`
	SourceRefs       *map[string]string `json:"sourceRefs,omitempty"`
}

// ReadWorkbenchKnowledgeRecord uses a node slug or an immutable change ID.
func (s *Store) ReadWorkbenchKnowledgeRecord(ctx context.Context, kind, reference string) (*WorkbenchKnowledgeRecord, error) {
	switch kind {
	case "spec":
		var result *WorkbenchKnowledgeRecord
		err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
			record, err := s.GetSpec(snapshotCtx, reference)
			if err != nil {
				return err
			}
			refs, err := s.ReadSpecSourceRefs(snapshotCtx, reference)
			if err != nil {
				return err
			}
			result = &WorkbenchKnowledgeRecord{Kind: kind, ID: record.ID, Slug: record.Slug, Version: record.Version, SpecgraphProject: s.project, Record: record, SourceRefs: &refs}
			return nil
		})
		return result, err
	case "decision":
		record, err := s.GetDecision(ctx, reference)
		if err != nil {
			return nil, err
		}
		return &WorkbenchKnowledgeRecord{Kind: kind, ID: record.ID, Slug: record.Slug, Version: int32(record.Version), SpecgraphProject: s.project, Record: record}, nil //nolint:gosec // GetDecision scans PostgreSQL integer into int32 before its domain int conversion.
	case "change":
		var record storage.ChangeLogEntry
		var changes []byte
		err := s.queryRow(ctx, `SELECT id,spec_slug,version,stage,content_hash,checkpoint,summary,reason,changes,date
			FROM changelog_entries WHERE project_slug=$1 AND id=$2`, s.project, reference).
			Scan(&record.ID, &record.SpecSlug, &record.Version, &record.Stage, &record.ContentHash, &record.Checkpoint, &record.Summary, &record.Reason, &changes, &record.Date)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWorkbenchChangeNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("postgres: read workbench change: %w", err)
		}
		record.Changes, err = unmarshalFieldChanges(changes)
		if err != nil {
			return nil, err
		}
		return &WorkbenchKnowledgeRecord{Kind: kind, ID: record.ID, Slug: record.SpecSlug, Version: record.Version, SpecgraphProject: s.project, Record: &record}, nil
	default:
		return nil, fmt.Errorf("postgres: invalid knowledge record kind %q", kind)
	}
}

// SearchWorkbenchKnowledge matches literal case-insensitive substrings.
// The caller validates the query at the request boundary.
func (s *Store) SearchWorkbenchKnowledge(ctx context.Context, query string) (*WorkbenchKnowledgeSearch, error) {
	// ponytail: linear scan of project records; add a substring index when corpus size requires it.
	rows, err := s.query(ctx, `WITH records AS (
		SELECT 'spec' AS kind, id, slug, intent AS title, stage AS status, version, updated_at,
			concat_ws(E'\n', id, slug, intent, notes, authoring.body) AS content
		FROM specs
		LEFT JOIN LATERAL (
			SELECT string_agg(value #>> '{}', E'\n' ORDER BY ordinal) AS body
			FROM jsonb_path_query(jsonb_build_array(spark_output, shape_output, specify_output, decompose_output),
				'strict $.** ? (@.type() == "string" || @.type() == "number" || @.type() == "boolean")')
				WITH ORDINALITY AS text_values(value, ordinal)
		) authoring ON true
		WHERE project_slug=$1
		UNION ALL
		SELECT 'decision', id, slug, title, status, version, updated_at,
			concat_ws(E'\n', id, slug, title, body, rationale, question)
		FROM decisions WHERE project_slug=$1
		UNION ALL
		SELECT 'change', id, spec_slug, summary, stage, version, date,
			concat_ws(E'\n', id, spec_slug, summary, reason, history.body)
		FROM changelog_entries
		LEFT JOIN LATERAL (
			SELECT string_agg(concat_ws(E'\n', delta->>'old_value', delta->>'new_value'), E'\n' ORDER BY ordinal) AS body
			FROM jsonb_array_elements(changes) WITH ORDINALITY AS deltas(delta, ordinal)
		) history ON true
		WHERE project_slug=$1
	), matches AS (
		SELECT *, strpos(lower(content), lower($2)) AS position FROM records
	)
	SELECT kind, id, slug, title, status, version,
		substring(content FROM greatest(1, position-(600-char_length($2))/2) FOR 600) AS excerpt, updated_at
	FROM matches WHERE position > 0
	ORDER BY (lower(id)=lower($2) OR lower(slug)=lower($2)) DESC, updated_at DESC, kind, id
	LIMIT 21`, s.project, query)
	if err != nil {
		return nil, fmt.Errorf("postgres: search workbench records: %w", err)
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchKnowledgeItem])
	if err != nil {
		return nil, fmt.Errorf("postgres: read workbench search: %w", err)
	}
	result := &WorkbenchKnowledgeSearch{Query: query, Scope: "specgraph-records", Items: items, HasMore: len(items) > 20}
	if result.HasMore {
		result.Items = result.Items[:20]
	}
	return result, nil
}
