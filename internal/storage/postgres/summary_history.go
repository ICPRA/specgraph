// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/specgraph/specgraph/internal/storage"
)

// ReadSummaryHistory retains each table's real order, without inventing a shared chronology.
func (s *Store) ReadSummaryHistory(ctx context.Context, goal, kind, beforeID string) (*storage.SummaryHistory, error) {
	if !validMailText(goal, 256) || (kind != "dispositions" && kind != "acceptances") || (beforeID != "" && !validReviewID(beforeID)) {
		return nil, storage.ErrInvalidSummary
	}
	page := &storage.SummaryHistory{GoalSlug: goal, Kind: kind}
	read := func(ctx context.Context) error {
		state, err := s.readSummaryInSnapshot(ctx, goal)
		if err != nil {
			return err
		}
		query := `SELECT record || jsonb_build_object('id',id::text) FROM summary_obligations WHERE project_slug=$1 AND goal_slug=$2 AND ($3='' OR id<NULLIF($3,'')::bigint) ORDER BY id DESC LIMIT 51`
		if kind == "acceptances" {
			query = `SELECT record || jsonb_build_object('id',id::text,'expectedReferences',context) FROM summary_acceptances WHERE project_slug=$1 AND goal_slug=$2 AND ($3='' OR id<NULLIF($3,'')::bigint) ORDER BY id DESC LIMIT 51`
		}
		rows, err := s.query(ctx, query, s.project, goal, beforeID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var body []byte
			if scanErr := rows.Scan(&body); scanErr != nil {
				return fmt.Errorf("postgres: scan summary history: %w", scanErr)
			}
			if kind == "dispositions" {
				var d storage.SummaryDisposition
				if decodeErr := json.Unmarshal(body, &d); decodeErr != nil {
					return fmt.Errorf("postgres: decode summary disposition history: %w", decodeErr)
				}
				page.Dispositions = append(page.Dispositions, d)
			} else {
				var a storage.SummaryAcceptance
				if decodeErr := json.Unmarshal(body, &a); decodeErr != nil {
					return fmt.Errorf("postgres: decode summary acceptance history: %w", decodeErr)
				}
				a.Current = state.Accepted && state.LatestAcceptance.ID == a.ID
				page.Acceptances = append(page.Acceptances, a)
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return fmt.Errorf("postgres: read summary history rows: %w", rowsErr)
		}
		if len(page.Dispositions) > 50 {
			page.HasMore = true
			page.Dispositions = page.Dispositions[:50]
			page.NextCursor = page.Dispositions[49].ID
		}
		if len(page.Acceptances) > 50 {
			page.HasMore = true
			page.Acceptances = page.Acceptances[:50]
			page.NextCursor = page.Acceptances[49].ID
		}
		return nil
	}
	err := s.RunReadSnapshot(ctx, read)
	return page, err
}
