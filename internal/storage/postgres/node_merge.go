// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

func validMergeTarget(t storage.MergeTarget) bool {
	return validMailText(t.Slug, 256) && validMailText(t.Intent, 4000) &&
		(t.Role == storage.SpecRoleWork || t.Role == storage.SpecRoleSummary) &&
		t.Priority.IsValid() && t.Complexity.IsValid() && len(t.Notes) <= 4000
}

// MergeNodes creates no durable target until every source, relation and summary
// obligation has been checked under the same project lock.
func (s *Store) MergeNodes(ctx context.Context, req storage.MergeRequest, scope *storage.MailScope) (*storage.MergeReceipt, error) {
	normalizeMergeRequest(&req)
	if !validMergeTarget(req.Target) || !validMergeSources(mergeSourceSlugs(req.Expected.Sources)) ||
		!validMailText(req.Reason, 4000) || !validMailText(req.IdempotencyKey, 256) {
		return nil, storage.ErrInvalidNodeMerge
	}
	var receipt *storage.MergeReceipt
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		user, run, err := s.summaryActor(txCtx, scope)
		if err != nil {
			return err
		}
		var priorID string
		err = s.queryRow(txCtx, `SELECT id::text FROM node_merges WHERE project_slug=$1 AND actor_user_id=$2 AND actor_run_id IS NOT DISTINCT FROM $3::text AND idempotency_key=$4`, s.project, user, run, req.IdempotencyKey).Scan(&priorID)
		if err == nil {
			prior, readErr := s.ReadNodeMergeReceipt(txCtx, priorID)
			if readErr != nil {
				return readErr
			}
			if !reflect.DeepEqual(prior.Request, req) {
				return storage.ErrNodeMergeConflict
			}
			prior.Replayed = true
			receipt = prior
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read prior merge: %w", err)
		}
		preview, err := s.previewNodeMerge(txCtx, mergeSourceSlugs(req.Expected.Sources), mergeSourceSlugs(req.Expected.Contexts))
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(*preview, req.Expected) {
			return storage.ErrNodeMergeConflict
		}
		for _, source := range preview.Sources {
			if source.Stage == "superseded" {
				return fmt.Errorf("%w: source %q already superseded", storage.ErrNodeMergeConflict, source.Slug)
			}
			pending, err := s.pendingNodeOwnership(txCtx, source.Slug)
			if err != nil {
				return err
			}
			if pending != nil {
				return fmt.Errorf("source %q: %w", source.Slug, storage.ErrNodeOwnershipPending)
			}
			if source.Slug == req.Target.Slug {
				return storage.ErrInvalidNodeMerge
			}
		}
		for _, item := range preview.Contexts {
			if item.Slug == req.Target.Slug {
				return storage.ErrInvalidNodeMerge
			}
		}
		exists, err := s.nodeExists(txCtx, req.Target.Slug)
		if err != nil {
			return err
		}
		if exists {
			return storage.ErrSpecAlreadyExists
		}
		plan, err := s.planMergeGraph(txCtx, req)
		if err != nil {
			return err
		}
		if err := s.validateMergeCycles(txCtx, plan); err != nil {
			return err
		}
		before, err := s.loadSummaryGraph(txCtx)
		if err != nil {
			return err
		}
		created, err := s.CreateSpec(txCtx, req.Target.Slug, req.Target.Intent, string(req.Target.Priority), string(req.Target.Complexity),
			storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, req.Target.SparkOutput, req.Target.ShapeOutput, req.Target.SpecifyOutput, nil)
		if err != nil {
			return err
		}
		if req.Target.Role == storage.SpecRoleSummary || req.Target.Notes != "" {
			if err := s.finishMergeTarget(txCtx, created, req.Target, req.Reason); err != nil {
				return err
			}
		}
		withTarget, err := s.loadSummaryGraph(txCtx)
		if err != nil {
			return err
		}
		final := plannedMergeSummaryGraph(before, withTarget, plan, preview.Sources)
		required, losses, err := s.mergeRequiredDispositions(txCtx, before, final, preview)
		if err != nil {
			return err
		}
		used := map[string]bool{}
		ids := []string{}
		for _, disposition := range req.Dispositions {
			key := disposition.GoalSlug + "\x00" + disposition.AffectedSlug
			if _, allowed := required[key]; !allowed || !validSummaryDispositionInput(disposition) || used[key] ||
				(disposition.Disposition != "withdraw" && disposition.Disposition != "replace") {
				return storage.ErrInvalidNodeMerge
			}
			used[key] = true
			if disposition.BeforeSources == nil {
				disposition.BeforeSources = []storage.ReviewSource{}
			}
			if disposition.AfterSources == nil {
				disposition.AfterSources = []storage.ReviewSource{}
			}
			d, err := s.recordSummaryDispositionAgainstGraphs(txCtx, disposition, user, run, before, final)
			if err != nil {
				return err
			}
			required[key] = false
			ids = append(ids, d.ID)
			for i := range losses {
				if losses[i].Goal == d.GoalSlug && losses[i].Slug == d.AffectedSlug {
					losses[i].DispositionID = d.ID
				}
			}
		}
		for key, needed := range required {
			if needed {
				goal, affected := splitMergePair(key)
				return fmt.Errorf("%w: goal %q affected %q needs approved withdraw or replacement disposition", storage.ErrSummaryNotAcceptable, goal, affected)
			}
		}
		if err := s.applyMergeRelations(txCtx, plan, before, losses); err != nil {
			return err
		}
		sourceResults := make([]storage.MergeSourceResult, 0, len(preview.Sources))
		for _, source := range preview.Sources {
			if err := s.supersedeMergeSource(txCtx, source, req.Target.Slug, req.Reason); err != nil {
				return err
			}
			sourceResults = append(sourceResults, storage.MergeSourceResult{ID: source.ID, Slug: source.Slug, BeforeVersion: source.Version, AfterVersion: source.Version + 1})
			if _, err := s.exec(txCtx, `INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES($1,$2,$3,'SUPERSEDES')`, s.project, req.Target.Slug, source.Slug); err != nil {
				return err
			}
			plan.inserted = append(plan.inserted, storage.MergeRelationRef{FromSlug: req.Target.Slug, ToSlug: source.Slug, Type: "SUPERSEDES"})
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		sortMergeRelations(plan.inserted)
		receipt = &storage.MergeReceipt{IdempotencyKey: req.IdempotencyKey, Request: req, ActorUserID: user, ActorRunID: run,
			Sources: sourceResults, Target: storage.MergeTargetResult{ID: created.ID, Slug: created.Slug},
			InsertedRelations: plan.inserted, DeletedRelations: plan.deleted, DispositionIDs: ids, CreatedAt: s.now()}
		body, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		requestBody, err := json.Marshal(req)
		if err != nil {
			return err
		}
		if err := s.queryRow(txCtx, `INSERT INTO node_merges(project_slug,actor_user_id,actor_run_id,idempotency_key,request,receipt,target_slug,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id::text`,
			s.project, user, run, req.IdempotencyKey, requestBody, body, created.Slug, receipt.CreatedAt).Scan(&receipt.ID); err != nil {
			return err
		}
		for _, source := range sourceResults {
			if _, err := s.exec(txCtx, `INSERT INTO node_merge_sources(project_slug,merge_id,source_slug,source_id,before_version,after_version) VALUES($1,$2::bigint,$3,$4,$5,$6)`,
				s.project, receipt.ID, source.Slug, source.ID, source.BeforeVersion, source.AfterVersion); err != nil {
				return err
			}
		}
		if err := s.verifyMergedSummary(txCtx, preview, losses); err != nil {
			return err
		}
		return nil
	})
	return receipt, err
}

func mergeSourceSlugs(refs []storage.MergeSourceRef) []string {
	result := make([]string, 0, len(refs))
	for _, ref := range refs {
		result = append(result, ref.Slug)
	}
	return result
}

func splitMergePair(key string) (string, string) {
	for i := range key {
		if key[i] == 0 {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

func (s *Store) finishMergeTarget(ctx context.Context, created *storage.Spec, target storage.MergeTarget, reason string) error {
	now := s.now()
	if _, err := s.exec(ctx, `UPDATE specs SET role=$3,notes=$4,version=version+1,updated_at=$5 WHERE project_slug=$1 AND slug=$2`, s.project, created.Slug, target.Role, target.Notes, now); err != nil {
		return err
	}
	deltas := []storage.FieldChange{}
	if target.Role != storage.SpecRoleWork {
		deltas = append(deltas, storage.FieldChange{Field: "role", OldValue: "work", NewValue: string(target.Role)})
	}
	if target.Notes != "" {
		deltas = append(deltas, storage.FieldChange{Field: "notes", NewValue: target.Notes})
	}
	return s.createChangeLog(ctx, created.Slug, &storage.ChangeLogEntry{Version: 2, Stage: "spark", ContentHash: created.ContentHash,
		Checkpoint: true, Summary: "Merged draft role and notes", Reason: reason, Date: now}, deltas)
}

func (s *Store) supersedeMergeSource(ctx context.Context, source storage.MergeSourceRef, target, reason string) error {
	now := s.now()
	tag, err := s.exec(ctx, `UPDATE specs SET stage='superseded',superseded_by=$3,version=version+1,updated_at=$4
 WHERE project_slug=$1 AND slug=$2 AND id=$5 AND version=$6 AND stage<>'superseded'`, s.project, source.Slug, target, now, source.ID, source.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return storage.ErrNodeMergeConflict
	}
	var hash string
	if err := s.queryRow(ctx, `SELECT content_hash FROM specs WHERE project_slug=$1 AND slug=$2`, s.project, source.Slug).Scan(&hash); err != nil {
		return err
	}
	return s.createChangeLog(ctx, source.Slug, &storage.ChangeLogEntry{Version: source.Version + 1, Stage: "superseded", ContentHash: hash,
		Checkpoint: true, Summary: "Spec merged into " + target, Reason: reason, Date: now},
		[]storage.FieldChange{{Field: "stage", OldValue: source.Stage, NewValue: "superseded"}, {Field: "superseded_by", NewValue: target}})
}
