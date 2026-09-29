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
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

func (s *Store) summaryActor(ctx context.Context, scope *storage.MailScope) (userID string, runID *string, resultErr error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return "", nil, storage.ErrSummaryForbidden
	}
	user, err := s.ExistingAuth().GetUserByID(ctx, identity.UserID)
	if err != nil {
		return "", nil, err
	}
	if user.DeletedAt != nil {
		return "", nil, storage.ErrSummaryForbidden
	}
	if scope == nil {
		if identity.UserKind != storage.KindHuman || user.Kind != storage.KindHuman {
			return "", nil, storage.ErrSummaryForbidden
		}
		if lockErr := s.lockDependencyState(ctx); lockErr != nil {
			return "", nil, lockErr
		}
		return user.ID, nil, nil
	}
	run, err := s.PlanningManagerRun(ctx, *scope)
	if errors.Is(err, storage.ErrPlanningForbidden) || errors.Is(err, storage.ErrReviewForbidden) || errors.Is(err, storage.ErrInvalidReview) {
		return "", nil, storage.ErrSummaryForbidden
	}
	return user.ID, &run, err
}

func (s *Store) summaryApproval(ctx context.Context, g *summaryGraph, id string) (*storage.ReviewRequest, bool, error) {
	if !validReviewID(id) {
		return nil, false, nil
	}
	var requestID string
	err := s.queryRow(ctx, `SELECT request_id::text FROM review_decisions WHERE project_slug=$1 AND id=$2::bigint`, s.project, id).Scan(&requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("postgres: read summary approval: %w", err)
	}
	r, err := s.ReadReviewRequest(ctx, requestID)
	if err != nil {
		return nil, false, err
	}
	if r.Kind != "requirements" && r.Kind != "design" {
		return r, false, nil
	}
	ids, err := s.readEffectiveReviewDecision(ctx, id, r.Kind)
	if errors.Is(err, storage.ErrInvalidReview) {
		return r, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if g.holds[r.TaskSlug] {
		return r, false, nil
	}
	if r.Kind == "design" {
		if len(ids) == 0 {
			return r, false, nil
		}
		for _, requirement := range ids {
			required, valid, err := s.summaryApproval(ctx, g, requirement)
			if err != nil {
				return nil, false, err
			}
			if !valid || required.Kind != "requirements" {
				return r, false, nil
			}
			for i := range required.Sources {
				source := &required.Sources[i]
				if !summaryCurrentSource(g, source) {
					return r, false, nil
				}
			}
		}
	}
	return r, true, nil
}

// These are field pointers, not a second task tree. Shared parents outside this
// goal are excluded, and no execution stage or completion generation is retained.
func summaryObligationSourceRefs(g *summaryGraph, goal, affected string) (map[string]map[string]string, bool) {
	ancestors, cycle := summaryReach(goal, g.parents)
	reach, descendantCycle := summaryReach(goal, g.children)
	parents, _ := summaryReach(affected, g.parents)
	if cycle || descendantCycle {
		return nil, false
	}
	for slug := range parents {
		if reach[slug] {
			ancestors[slug] = true
		}
	}
	ancestors[affected] = true
	refs := make(map[string]map[string]string, len(ancestors))
	for slug := range ancestors {
		node, ok := g.nodes[slug]
		if !ok {
			return nil, false
		}
		refs[slug] = node.SourceRefs
	}
	return refs, true
}

func (s *Store) summaryDispositionValid(ctx context.Context, g *summaryGraph, d *storage.SummaryDisposition, applicability map[string]map[string]string) (bool, error) {
	if d.Disposition == "needs_review" {
		return false, nil
	}
	_, valid, err := s.summaryApproval(ctx, g, d.ReviewDecisionID)
	if err != nil || !valid {
		return false, err
	}
	current, ok := summaryObligationSourceRefs(g, d.GoalSlug, d.AffectedSlug)
	if !ok {
		return false, nil
	}
	for slug, refs := range current {
		if !reflect.DeepEqual(refs, applicability[slug]) {
			return false, nil
		}
	}
	// Detachment does not erase the fixed source basis used to approve the loss.
	for slug, refs := range applicability {
		n, ok := g.nodes[slug]
		if !ok || !reflect.DeepEqual(n.SourceRefs, refs) {
			return false, nil
		}
	}
	for i := range d.AfterSources {
		source := &d.AfterSources[i]
		if !summaryCurrentSource(g, source) {
			return false, nil
		}
	}
	if d.Disposition == "replace" {
		reach, cycle := summaryReach(d.GoalSlug, g.children)
		ancestors, _ := summaryReach(d.AffectedSlug, g.parents)
		if cycle || !reach[d.ReplacementSlug] || ancestors[d.ReplacementSlug] {
			return false, nil
		}
		// Replacement must itself remain an obligation; replacing one withdrawal with another is not satisfaction.
		var status string
		err := s.queryRow(ctx, `SELECT record->>'disposition' FROM summary_obligations WHERE project_slug=$1 AND goal_slug=$2 AND affected_slug=$3 ORDER BY id DESC LIMIT 1`, s.project, d.GoalSlug, d.ReplacementSlug).Scan(&status)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("postgres: read replacement disposition: %w", err)
		}
		if status == "withdraw" || status == "replace" || status == "needs_review" {
			return false, nil
		}
	}
	return true, nil
}

// RecordSummaryDisposition records an authorized disposition with its fixed approval and source basis.
func (s *Store) RecordSummaryDisposition(ctx context.Context, req storage.SummaryDispositionRequest, scope *storage.MailScope) (*storage.SummaryDisposition, error) {
	if !validSummaryDispositionInput(req) {
		return nil, storage.ErrInvalidSummary
	}
	if req.BeforeSources == nil {
		req.BeforeSources = []storage.ReviewSource{}
	}
	if req.AfterSources == nil {
		req.AfterSources = []storage.ReviewSource{}
	}
	var result *storage.SummaryDisposition
	err := s.RunInTransaction(ctx, func(ctx context.Context) error {
		user, run, err := s.summaryActor(ctx, scope)
		if err != nil {
			return err
		}
		var encoded []byte
		err = s.queryRow(ctx, `SELECT record || jsonb_build_object('id',id::text) FROM summary_obligations WHERE project_slug=$1 AND actor_user_id=$2 AND actor_run_id IS NOT DISTINCT FROM $3::text AND idempotency_key=$4`, s.project, user, run, req.IdempotencyKey).Scan(&encoded)
		if err == nil {
			var old storage.SummaryDisposition
			if decodeErr := json.Unmarshal(encoded, &old); decodeErr != nil {
				return fmt.Errorf("postgres: decode prior summary disposition: %w", decodeErr)
			}
			if !reflect.DeepEqual(old.SummaryDispositionRequest, req) {
				return storage.ErrSummaryConflict
			}
			result = &old
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read prior summary disposition: %w", err)
		}
		g, err := s.loadSummaryGraph(ctx)
		if err != nil {
			return err
		}
		result, err = s.recordSummaryDispositionAgainstGraphs(ctx, req, user, run, g, g)
		return err
	})
	return result, err
}

func validSummaryDispositionInput(req storage.SummaryDispositionRequest) bool {
	if !validMailText(req.GoalSlug, 256) || !validMailText(req.AffectedSlug, 256) || req.GoalSlug == req.AffectedSlug || !validMailText(req.Reason, 4000) || !validMailText(req.IdempotencyKey, 256) ||
		!slices.Contains([]string{"retain", "adjust", "replace", "withdraw", "needs_review"}, req.Disposition) || (req.Disposition == "replace") != (req.ReplacementSlug != "") || req.ReplacementSlug == req.AffectedSlug {
		return false
	}
	if req.Disposition != "needs_review" && (len(req.BeforeSources) == 0 || len(req.AfterSources) == 0 || !validReviewID(req.ReviewDecisionID)) {
		return false
	}
	if req.ReviewDecisionID != "" && !validReviewID(req.ReviewDecisionID) {
		return false
	}
	return true
}

// The public single-disposition path uses the same graph twice. Merge supplies
// the actual before graph for applicability and the planned final graph for replacement.
func (s *Store) recordSummaryDispositionAgainstGraphs(ctx context.Context, req storage.SummaryDispositionRequest, user string, run *string, before, final *summaryGraph) (*storage.SummaryDisposition, error) {
	state, err := s.summaryState(ctx, before, req.GoalSlug)
	if err != nil {
		return nil, err
	}
	found := false
	for _, o := range state.Obligations {
		if o.Slug == req.AffectedSlug {
			found = true
		}
	}
	if !found {
		return nil, storage.ErrInvalidSummary
	}
	if req.Disposition == "retain" || req.Disposition == "adjust" {
		current, _ := summaryReach(req.GoalSlug, final.children)
		if !current[req.AffectedSlug] {
			return nil, storage.ErrSummaryNotAcceptable
		}
	}
	var approval *storage.ReviewRequest
	if req.ReviewDecisionID != "" {
		var valid bool
		approval, valid, err = s.summaryApproval(ctx, before, req.ReviewDecisionID)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, storage.ErrInvalidSummary
		}
	}
	for i := range req.BeforeSources {
		source := &req.BeforeSources[i]
		if sourceErr := s.validateReviewSource(ctx, source); sourceErr != nil {
			return nil, sourceErr
		}
	}
	for i := range req.AfterSources {
		source := &req.AfterSources[i]
		if sourceErr := s.validateReviewSource(ctx, source); sourceErr != nil {
			return nil, sourceErr
		}
		if req.Disposition == "needs_review" {
			continue
		}
		if !slices.Contains(approval.Sources, *source) {
			return nil, storage.ErrInvalidSummary
		}
		if !summaryCurrentSource(final, source) {
			return nil, storage.ErrSummaryConflict
		}
	}
	if req.Disposition == "replace" {
		finalState, stateErr := s.summaryState(ctx, final, req.GoalSlug)
		if stateErr != nil {
			return nil, stateErr
		}
		found = false
		for _, o := range finalState.Obligations {
			if o.Slug == req.ReplacementSlug && o.Effective && !o.PendingReview {
				found = true
			}
		}
		current, _ := summaryReach(req.GoalSlug, final.children)
		ancestors, _ := summaryReach(req.AffectedSlug, final.parents)
		if !found || !current[req.ReplacementSlug] || ancestors[req.ReplacementSlug] {
			return nil, storage.ErrInvalidSummary
		}
	}
	refs, valid := summaryObligationSourceRefs(before, req.GoalSlug, req.AffectedSlug)
	if !valid {
		return nil, storage.ErrSummaryNotAcceptable
	}
	if before != final {
		finalRefs, finalValid := summaryObligationSourceRefs(final, req.GoalSlug, req.AffectedSlug)
		if !finalValid {
			return nil, storage.ErrSummaryNotAcceptable
		}
		for slug, sourceRefs := range finalRefs {
			if prior, exists := refs[slug]; exists && !reflect.DeepEqual(prior, sourceRefs) {
				return nil, storage.ErrSummaryConflict
			}
			refs[slug] = sourceRefs
		}
	}
	d := storage.SummaryDisposition{SummaryDispositionRequest: req, ActorUserID: user, ActorRunID: run, ActorKind: "human", CreatedAt: s.now()}
	if run != nil {
		d.ActorKind = "agent"
	}
	body, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("postgres: encode summary disposition: %w", err)
	}
	err = s.queryRow(ctx, `INSERT INTO summary_obligations(project_slug,goal_slug,affected_slug,actor_user_id,actor_run_id,idempotency_key,record,applicability) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id::text`, s.project, req.GoalSlug, req.AffectedSlug, user, run, req.IdempotencyKey, body, refs).Scan(&d.ID)
	if err != nil {
		return nil, fmt.Errorf("postgres: record summary disposition: %w", err)
	}
	return &d, nil
}

func (s *Store) readSummaryAcceptance(ctx context.Context, id string) (*storage.SummaryAcceptance, error) {
	var encoded []byte
	var refs storage.SummaryReferences
	err := s.queryRow(ctx, `SELECT record || jsonb_build_object('id',id::text),context FROM summary_acceptances WHERE project_slug=$1 AND id=$2::bigint`, s.project, id).Scan(&encoded, &refs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrSummaryAcceptanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read summary acceptance: %w", err)
	}
	var a storage.SummaryAcceptance
	decodeErr := json.Unmarshal(encoded, &a)
	a.ExpectedReferences = refs
	if decodeErr != nil {
		return &a, fmt.Errorf("postgres: decode summary acceptance: %w", decodeErr)
	}
	return &a, nil
}

// AcceptSummary records a goal acceptance against the exact current summary references.
func (s *Store) AcceptSummary(ctx context.Context, req storage.SummaryAcceptRequest, scope *storage.MailScope) (*storage.SummaryAcceptance, error) {
	if req.ImpactReview == nil {
		req.ImpactReview = []storage.SummaryImpactReview{}
	}
	if !validMailText(req.GoalSlug, 256) || !validMailText(req.Basis, 4000) || !validMailText(req.IdempotencyKey, 256) || !req.GoalsSatisfied || len(req.EvidenceSources) == 0 || len(req.ExpectedReferences.Nodes) == 0 {
		return nil, storage.ErrInvalidSummary
	}
	var result *storage.SummaryAcceptance
	err := s.RunInTransaction(ctx, func(ctx context.Context) error {
		user, run, err := s.summaryActor(ctx, scope)
		if err != nil {
			return err
		}
		var priorID string
		err = s.queryRow(ctx, `SELECT id::text FROM summary_acceptances WHERE project_slug=$1 AND actor_user_id=$2 AND actor_run_id IS NOT DISTINCT FROM $3::text AND idempotency_key=$4`, s.project, user, run, req.IdempotencyKey).Scan(&priorID)
		if err == nil {
			old, acceptanceErr := s.readSummaryAcceptance(ctx, priorID)
			if acceptanceErr != nil {
				return acceptanceErr
			}
			if !reflect.DeepEqual(old.SummaryAcceptRequest, req) {
				return storage.ErrSummaryConflict
			}
			state, snapshotErr := s.readSummaryInSnapshot(ctx, req.GoalSlug)
			if snapshotErr != nil {
				return snapshotErr
			}
			old.Current = state.Accepted && state.LatestAcceptance.ID == old.ID
			result = old
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: read prior summary acceptance: %w", err)
		}
		g, err := s.loadSummaryGraph(ctx)
		if err != nil {
			return err
		}
		state, err := s.summaryState(ctx, g, req.GoalSlug)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(req.ExpectedReferences, state.References) {
			return storage.ErrSummaryConflict
		}
		if !state.Acceptable {
			return storage.ErrSummaryNotAcceptable
		}
		covered := map[string]bool{}
		for _, review := range req.ImpactReview {
			if !validMailText(review.Basis, 4000) || !slices.Contains(state.ReviewCandidates, review.Slug) || covered[review.Slug] {
				return storage.ErrInvalidSummary
			}
			if review.DispositionID != "" {
				found := false
				for _, o := range state.Obligations {
					if o.Slug == review.Slug && o.DispositionID == review.DispositionID && !o.PendingReview {
						found = true
					}
				}
				if !found {
					return storage.ErrInvalidSummary
				}
			}
			covered[review.Slug] = true
		}
		for _, slug := range state.ReviewCandidates {
			if !covered[slug] {
				return storage.ErrSummaryNotAcceptable
			}
		}
		for i := range req.EvidenceSources {
			source := &req.EvidenceSources[i]
			if sourceErr := s.validateReviewSource(ctx, source); sourceErr != nil {
				return sourceErr
			}
			if !summaryCurrentSource(g, source) {
				return storage.ErrSummaryConflict
			}
		}
		a := storage.SummaryAcceptance{SummaryAcceptRequest: req, ActorUserID: user, ActorRunID: run, ActorKind: "human", CreatedAt: s.now(), Current: true}
		if run != nil {
			a.ActorKind = "agent"
		}
		body, err := json.Marshal(a)
		if err != nil {
			return fmt.Errorf("postgres: encode summary acceptance: %w", err)
		}
		// Context references have one retained owner, separate from the named opinion.
		err = s.queryRow(ctx, `INSERT INTO summary_acceptances(project_slug,goal_slug,actor_user_id,actor_run_id,idempotency_key,record,context) VALUES($1,$2,$3,$4,$5,$6::jsonb-'expectedReferences',$7) RETURNING id::text`, s.project, req.GoalSlug, user, run, req.IdempotencyKey, body, state.References).Scan(&a.ID)
		if err != nil {
			return fmt.Errorf("postgres: record summary acceptance: %w", err)
		}
		if hookErr := s.triggerCompletionHooks(ctx, req.GoalSlug, "summary", a.ID); hookErr != nil {
			return hookErr
		}
		result = &a
		return nil
	})
	return result, err
}

// RevokeSummaryAcceptance records an explicit revocation without deleting the acceptance.
func (s *Store) RevokeSummaryAcceptance(ctx context.Context, id, reason string, scope *storage.MailScope) (*storage.SummaryAcceptance, error) {
	if !validReviewID(id) || !validMailText(reason, 4000) {
		return nil, storage.ErrInvalidSummary
	}
	var result *storage.SummaryAcceptance
	err := s.RunInTransaction(ctx, func(ctx context.Context) error {
		user, run, err := s.summaryActor(ctx, scope)
		if err != nil {
			return err
		}
		a, err := s.readSummaryAcceptance(ctx, id)
		if err != nil {
			return err
		}
		if a.RevokedAt != nil {
			if !reflect.DeepEqual(a.RevokedByRunID, run) || *a.RevokedByUserID != user || *a.RevocationReason != reason {
				return storage.ErrSummaryConflict
			}
			result = a
			result.Current = false
			return nil
		}
		now := s.now()
		a.RevokedAt = &now
		a.RevokedByUserID = &user
		a.RevokedByRunID = run
		a.RevocationReason = &reason
		a.Current = false
		body, err := json.Marshal(a)
		if err != nil {
			return fmt.Errorf("postgres: encode revoked summary acceptance: %w", err)
		}
		_, err = s.exec(ctx, `UPDATE summary_acceptances SET record=$3::jsonb-'expectedReferences' WHERE project_slug=$1 AND id=$2::bigint`, s.project, id, body)
		if err != nil {
			return err
		}
		result = a
		return nil
	})
	return result, err
}
