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

// Only explicit submission mappings establish pending authoring responsibility.
func (s *Store) currentAuthoringSubmission(ctx context.Context, slug string) (*storage.ReviewRequest, error) {
	var id string
	err := s.queryRow(ctx, `SELECT r.id::text FROM review_requests r JOIN run_bindings b ON b.project_slug=r.project_slug AND b.id=r.completion_run_id
 WHERE r.project_slug=$1 AND b.task_spec_slug=$2 ORDER BY r.id DESC LIMIT 1`, s.project, slug).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read current authoring submission: %w", err)
	}
	return s.ReadReviewRequest(ctx, id)
}

func (s *Store) checkAuthoringOwner(ctx context.Context, runID, kind string, allowCompleted bool) (taskSlug, taskStage string, resultErr error) {
	var slug, purpose, state, stage, role string
	err := s.queryRow(ctx, `SELECT b.task_spec_slug,COALESCE(p.body->'dispatch_target'->>'workPurpose',''),b.state,s.stage,s.role
 FROM run_bindings b JOIN context_packages p ON p.project_slug=b.project_slug AND p.id=b.package_id AND p.task_spec_slug=b.task_spec_slug
 JOIN specs s ON s.project_slug=b.project_slug AND s.slug=b.task_spec_slug
 WHERE b.project_slug=$1 AND b.id=$2 FOR UPDATE OF s,b`, s.project, runID).Scan(&slug, &purpose, &state, &stage, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", storage.ErrRunBindingNotFound
	}
	if err != nil {
		return "", "", fmt.Errorf("postgres: read authoring owner: %w", err)
	}
	if purpose != kind || (kind != "requirements" && kind != "design") {
		return "", "", fmt.Errorf("completionRunId must own the matching authoring purpose: %w", storage.ErrInvalidReview)
	}
	if role == string(storage.SpecRoleSummary) {
		return "", "", storage.ErrSummaryNotExecutable
	}
	if !(stage == "approved" || stage == "in_progress" || stage == "review" || (allowCompleted && stage == "done")) {
		return "", "", storage.ErrSpecNotApproved
	}
	pending, err := s.currentAuthoringSubmission(ctx, slug)
	if err != nil {
		return "", "", err
	}
	if pending != nil && *pending.CompletionRunID != runID && stage == "review" {
		return "", "", storage.ErrRunBindingConflict
	}
	sameSubmission := pending != nil && *pending.CompletionRunID == runID
	hasPending := sameSubmission && stage == "review"
	completed := allowCompleted && sameSubmission && stage == "done" && state == "completed"
	if (stage == "review" && !hasPending) || (stage == "done" && !completed) || (!(state == "bound" || (hasPending && state == "handed_off") || completed)) {
		return "", "", storage.ErrRunBindingConflict
	}
	var competing, cancelled, owns bool
	err = s.queryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent<>$3) OR
 EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND run_id<>$3 AND released_at IS NULL),
 EXISTS(SELECT 1 FROM run_preparation_cancellations WHERE project_slug=$1 AND run_id=$3),
 EXISTS(SELECT 1 FROM claims WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3) OR
 EXISTS(SELECT 1 FROM run_dispatches WHERE project_slug=$1 AND task_slug=$2 AND run_id=$3 AND released_at IS NULL)`, s.project, slug, runID).Scan(&competing, &cancelled, &owns)
	if err != nil {
		return "", "", fmt.Errorf("postgres: read authoring ownership conflicts: %w", err)
	}
	if competing || cancelled || (!hasPending && !owns && !completed) {
		return "", "", storage.ErrRunBindingConflict
	}
	return slug, stage, nil
}

func (s *Store) submitAuthoringSources(ctx context.Context, req *storage.AssignReviewRequest) error {
	if req.CompletionRunID == nil {
		return nil
	}
	if req.AuthorResponsibility.Kind != "agent" || req.AuthorResponsibility.RunID != *req.CompletionRunID {
		return storage.ErrInvalidReview
	}
	slug, stage, err := s.checkAuthoringOwner(ctx, *req.CompletionRunID, req.Kind, false)
	if err != nil {
		return err
	}
	if stage != "review" {
		review := "review"
		_, err = s.UpdateSpec(ctx, slug, nil, &review, nil, nil, nil)
	}
	return err
}

// An accepted whole submission permits only its explicitly listed current output fields to differ.
func (s *Store) checkAuthoringCompletion(ctx context.Context, slug, runID, kind, expectedRequest string) (map[string]bool, error) {
	submission, err := s.currentAuthoringSubmission(ctx, slug)
	if err != nil {
		return nil, err
	}
	if submission == nil || *submission.CompletionRunID != runID || (expectedRequest != "" && submission.ID != expectedRequest) {
		return nil, fmt.Errorf("authoring completion requires its current explicit source submission: %w", storage.ErrInvalidReview)
	}
	owner, stage, err := s.checkAuthoringOwner(ctx, runID, kind, true)
	if err != nil {
		return nil, err
	}
	if owner != slug || (stage != "review" && stage != "done") || submission.Kind != kind || submission.AuthorResponsibility.Kind != "agent" || submission.AuthorResponsibility.RunID != runID {
		return nil, storage.ErrRunBindingConflict
	}
	status, err := s.ReadReviewStatus(ctx, submission.TaskSlug)
	if err != nil {
		return nil, err
	}
	for _, state := range status.Reviews {
		if state.Kind == kind {
			if state.HumanHold {
				return nil, storage.ErrReviewHumanHold
			}
			if state.Request == nil || state.Request.ID != submission.ID {
				return nil, storage.ErrReviewForbidden
			}
		}
	}
	if len(submission.Decisions) == 0 {
		return nil, storage.ErrInvalidReview
	}
	decision := submission.Decisions[len(submission.Decisions)-1]
	if _, err = s.readEffectiveReviewDecision(ctx, decision.ID, kind); err != nil {
		return nil, err
	}
	if requirementsErr := s.checkReviewRequirements(ctx, kind, submission.RequirementDecisionIDs); requirementsErr != nil {
		return nil, requirementsErr
	}
	refs, err := s.ReadSpecSourceRefs(ctx, slug)
	if err != nil {
		return nil, err
	}
	outputs := map[string]bool{}
	for i := range submission.Sources {
		source := &submission.Sources[i]
		if sourceErr := s.validateReviewSource(ctx, source); sourceErr != nil {
			return nil, sourceErr
		}
		if source.Kind == "specgraph" && source.SpecSlug == slug {
			if refs[source.Field] != source.ChangeID {
				return nil, storage.ErrCompletionRequiresRequirementReview
			}
			outputs[source.Field] = true
		}
	}
	return outputs, nil
}
