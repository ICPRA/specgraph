// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

func validMergeSources(slugs []string) bool {
	if len(slugs) < 2 || len(slugs) > 32 {
		return false
	}
	seen := make(map[string]bool, len(slugs))
	for _, slug := range slugs {
		if !validMailText(slug, 256) || seen[slug] {
			return false
		}
		seen[slug] = true
	}
	return true
}

func (s *Store) PreviewNodeMerge(ctx context.Context, sourceSlugs, contextSlugs []string) (*storage.MergePreview, error) {
	if !validMergeSources(sourceSlugs) || len(contextSlugs) > 32 {
		return nil, storage.ErrInvalidNodeMerge
	}
	seen := map[string]bool{}
	for _, slug := range sourceSlugs {
		seen[slug] = true
	}
	for _, slug := range contextSlugs {
		if !validMailText(slug, 256) || seen[slug] {
			return nil, storage.ErrInvalidNodeMerge
		}
		seen[slug] = true
	}
	var preview *storage.MergePreview
	read := func(ctx context.Context) error {
		var err error
		preview, err = s.previewNodeMerge(ctx, sourceSlugs, contextSlugs)
		return err
	}
	if _, inTx := txFromContext(ctx); inTx {
		err := read(ctx)
		return preview, err
	}
	err := s.RunReadSnapshot(ctx, read)
	return preview, err
}

func (s *Store) previewNodeMerge(ctx context.Context, sourceSlugs, contextSlugs []string) (*storage.MergePreview, error) {
	p := &storage.MergePreview{Sources: []storage.MergeSourceRef{}, Contexts: []storage.MergeSourceRef{}, Relations: []storage.MergeRelationRef{}, Lineage: []storage.MergeRelationRef{}, Ancestors: []storage.MergeAncestorRef{}}
	rows, err := s.query(ctx, `SELECT id,slug,version,stage,role FROM specs WHERE project_slug=$1 AND slug=ANY($2) ORDER BY slug COLLATE "C"`, s.project, sourceSlugs)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var source storage.MergeSourceRef
		if err := rows.Scan(&source.ID, &source.Slug, &source.Version, &source.Stage, &source.Role); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan merge source: %w", err)
		}
		p.Sources = append(p.Sources, source)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read merge sources: %w", err)
	}
	rows.Close()
	if len(p.Sources) != len(sourceSlugs) {
		return nil, storage.ErrSpecNotFound
	}
	rows, err = s.query(ctx, `SELECT e.from_slug,e.to_slug,e.edge_type,
 CASE WHEN e.edge_type IN('COMPOSES','DECIDED_IN') THEN COALESCE((SELECT max(c.id)::text FROM composition_changes c
 WHERE c.project_slug=e.project_slug AND c.from_slug=e.from_slug AND c.to_slug=e.to_slug AND c.edge_type=e.edge_type),'0') ELSE '' END
 FROM edges e WHERE e.project_slug=$1 AND (e.from_slug=ANY($2) OR e.to_slug=ANY($2))
 AND e.edge_type IN('DEPENDS_ON','BLOCKS','COMPOSES','RELATES_TO','INFORMS','DECIDED_IN','SUPERSEDES')
 ORDER BY e.edge_type COLLATE "C",e.from_slug COLLATE "C",e.to_slug COLLATE "C"`, s.project, sourceSlugs)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var relation storage.MergeRelationRef
		if err := rows.Scan(&relation.FromSlug, &relation.ToSlug, &relation.Type, &relation.ChangeID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan merge relation: %w", err)
		}
		if relation.Type == "SUPERSEDES" {
			p.Lineage = append(p.Lineage, relation)
		} else {
			p.Relations = append(p.Relations, relation)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: read merge relations: %w", err)
	}
	rows.Close()
	contextSet := map[string]bool{}
	for _, slug := range contextSlugs {
		contextSet[slug] = true
	}
	for _, relation := range p.Relations {
		for _, slug := range []string{relation.FromSlug, relation.ToSlug} {
			if !seenSourceSlug(p.Sources, slug) {
				contextSet[slug] = true
			}
		}
	}
	if len(contextSet) > 0 {
		candidates := make([]string, 0, len(contextSet))
		for slug := range contextSet {
			candidates = append(candidates, slug)
		}
		rows, err = s.query(ctx, `SELECT id,slug,version,stage,role FROM specs WHERE project_slug=$1 AND slug=ANY($2) ORDER BY slug COLLATE "C"`, s.project, candidates)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var item storage.MergeSourceRef
			if err := rows.Scan(&item.ID, &item.Slug, &item.Version, &item.Stage, &item.Role); err != nil {
				rows.Close()
				return nil, err
			}
			p.Contexts = append(p.Contexts, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		for _, slug := range contextSlugs {
			if !seenSourceSlug(p.Contexts, slug) {
				return nil, storage.ErrSpecNotFound
			}
		}
	}
	g, err := s.loadSummaryGraph(ctx)
	if err != nil {
		return nil, err
	}
	ancestorSlugs := map[string]bool{}
	for _, source := range append(slices.Clone(p.Sources), p.Contexts...) {
		ancestors, cycle := summaryReach(source.Slug, g.parents)
		if cycle {
			return nil, storage.ErrSummaryNotAcceptable
		}
		for slug := range ancestors {
			if g.nodes[slug].Role == storage.SpecRoleSummary {
				ancestorSlugs[slug] = true
			}
		}
	}
	ordered := make([]string, 0, len(ancestorSlugs))
	for slug := range ancestorSlugs {
		ordered = append(ordered, slug)
	}
	slices.Sort(ordered)
	for _, slug := range ordered {
		state, err := s.summaryState(ctx, g, slug)
		if err != nil {
			return nil, err
		}
		p.Ancestors = append(p.Ancestors, storage.MergeAncestorRef{GoalSlug: slug, References: state.References})
	}
	return p, nil
}

func seenSourceSlug(refs []storage.MergeSourceRef, slug string) bool {
	for _, ref := range refs {
		if ref.Slug == slug {
			return true
		}
	}
	return false
}

func (s *Store) ReadNodeMergeReceipt(ctx context.Context, id string) (*storage.MergeReceipt, error) {
	if !validReviewID(id) {
		return nil, storage.ErrInvalidNodeMerge
	}
	var body []byte
	err := s.queryRow(ctx, `SELECT receipt || jsonb_build_object('id',id::text) FROM node_merges WHERE project_slug=$1 AND id=$2::bigint`, s.project, id).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrNodeMergeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read node merge receipt: %w", err)
	}
	var receipt storage.MergeReceipt
	if err := json.Unmarshal(body, &receipt); err != nil {
		return nil, fmt.Errorf("postgres: decode node merge receipt: %w", err)
	}
	return &receipt, nil
}

func (s *Store) ReadNodeMergeHistory(ctx context.Context, taskSlug, beforeID string) (*storage.MergeHistory, error) {
	if !validMailText(taskSlug, 256) || (beforeID != "" && !validReviewID(beforeID)) {
		return nil, storage.ErrInvalidNodeMerge
	}
	page := &storage.MergeHistory{Items: []storage.MergeReceipt{}}
	rows, err := s.query(ctx, `SELECT m.receipt || jsonb_build_object('id',m.id::text) FROM node_merges m
 WHERE m.project_slug=$1 AND ($3='' OR m.id<NULLIF($3,'')::bigint)
 AND (m.target_slug=$2 OR EXISTS(SELECT 1 FROM node_merge_sources x WHERE x.project_slug=m.project_slug AND x.merge_id=m.id AND x.source_slug=$2))
 ORDER BY m.id DESC LIMIT 51`, s.project, taskSlug, beforeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, fmt.Errorf("postgres: scan node merge history: %w", err)
		}
		var receipt storage.MergeReceipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			return nil, fmt.Errorf("postgres: decode node merge history: %w", err)
		}
		page.Items = append(page.Items, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read node merge history: %w", err)
	}
	if len(page.Items) > 50 {
		page.Items = page.Items[:50]
		page.HasMore = true
		page.NextCursor = page.Items[49].ID
	}
	return page, nil
}
