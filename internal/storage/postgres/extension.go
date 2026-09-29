// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/specgraph/specgraph/internal/storage"
)

// projectManaged reports whether the store's project runs under workbench
// management, where completion claims must be confirmed by an accepted
// delivery (plan v1 §7.2).
func (s *Store) projectManaged(ctx context.Context) (bool, error) {
	var managed bool
	err := s.queryRow(ctx,
		`SELECT managed FROM projects WHERE slug = $1`,
		s.project,
	).Scan(&managed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("postgres: project managed: %q: %w", s.project, storage.ErrProjectNotFound)
	}
	if err != nil {
		return false, fmt.Errorf("postgres: project managed: %w", err)
	}
	return managed, nil
}

// hasAcceptedDelivery checks the latest delivery of the claiming execution,
// not an accepted historical delivery from another execution or candidate.
func (s *Store) hasAcceptedDelivery(ctx context.Context, specSlug, runID string) (bool, error) {
	var exists bool
	err := s.queryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM run_bindings rb
			JOIN LATERAL (
				SELECT d.id FROM deliveries d
				WHERE d.run_binding_id = rb.id AND d.project_slug = rb.project_slug
				ORDER BY d.submitted_at DESC, d.id DESC LIMIT 1
			) d ON true
			JOIN LATERAL (
				SELECT a.verdict,a.actor_kind FROM acceptances a
				WHERE a.delivery_id = d.id AND a.project_slug = rb.project_slug
				ORDER BY a.created_at DESC, a.id DESC LIMIT 1
			) a ON true
			WHERE rb.project_slug = $1
			  AND rb.task_spec_slug = $2
			  AND rb.id = $3
			  AND a.verdict = 'accepted'
			  AND a.actor_kind = 'human'
		)`,
		s.project, specSlug, runID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres: has accepted delivery: %w", err)
	}
	return exists, nil
}

// SetProjectManaged flips the managed flag for a project.
func (s *Store) SetProjectManaged(ctx context.Context, slug string, managed bool) error {
	tag, err := s.exec(ctx,
		`UPDATE projects SET managed = $1, updated_at = $2 WHERE slug = $3`,
		managed, s.now(), slug,
	)
	if err != nil {
		return fmt.Errorf("postgres: set project managed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: set project managed: %q: %w", slug, storage.ErrProjectNotFound)
	}
	return nil
}

// insertAcceptance is private to the attributed shared review transaction.
func (s *Store) insertAcceptance(ctx context.Context, deliveryID, fingerprint, verdict, approver string, conditions json.RawMessage, actorKind string, reviewerRunID, requestID *string) (string, error) {
	id := newID("acc")
	if len(conditions) == 0 {
		conditions = json.RawMessage("{}")
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		// The same spec lock orders completion against new decisions.
		var slug string
		err := s.queryRow(txCtx, `SELECT s.slug FROM specs s
			JOIN run_bindings rb ON rb.project_slug = s.project_slug AND rb.task_spec_slug = s.slug
			JOIN deliveries d ON d.project_slug = rb.project_slug AND d.run_binding_id = rb.id
			WHERE d.project_slug = $1 AND d.id = $2 FOR UPDATE OF s`, s.project, deliveryID).Scan(&slug)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrDeliveryNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: acceptance task lock: %w", err)
		}
		tag, err := s.exec(txCtx,
			`INSERT INTO acceptances (id, project_slug, delivery_id, requirements_fingerprint, verdict, approver, conditions, created_at,actor_kind,reviewer_run_id,review_request_id)
		 SELECT $1, $2, d.id, $4, $5, $6, $7, $8,$9,$10,$11::bigint
		 FROM deliveries d JOIN run_bindings rb ON rb.id = d.run_binding_id AND rb.project_slug = d.project_slug
		 WHERE d.id = $3 AND d.project_slug = $2`,
			id, s.project, deliveryID, fingerprint, verdict, approver, conditions, s.now(), actorKind, reviewerRunID, requestID,
		)
		if err != nil {
			return fmt.Errorf("postgres: insert acceptance: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("postgres: insert acceptance: %q: %w", deliveryID, storage.ErrDeliveryNotFound)
		}
		return nil
	})
	return id, err
}

// PrepareRun claims the task spec and persists an immutable context package
// plus a run binding (generation 1, state prepared). The claim and both
// inserts share one transaction (ADR-004). Each execution owns its own lease;
// a second preparation must not renew another execution's claim.
func (s *Store) PrepareRun(ctx context.Context, taskSlug, workspace string) (bindingID string, err error) {
	pkgID := newID("pkg")
	bindingID = newID("rb")
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		if _, claimSpecErr := s.ClaimSpec(txCtx, taskSlug, bindingID, 30*time.Minute); claimSpecErr != nil {
			return fmt.Errorf("postgres: prepare run claim: %w", claimSpecErr)
		}
		// ClaimSpec holds the primary spec row lock through package publication.
		bundle, generateBundleErr := s.GenerateBundle(txCtx, taskSlug)
		if generateBundleErr != nil {
			return fmt.Errorf("postgres: prepare run bundle: %w", generateBundleErr)
		}
		if checkPrerequisitesErr := s.checkPrerequisites(txCtx, taskSlug); checkPrerequisitesErr != nil {
			return checkPrerequisitesErr
		}
		dependencyRevision, generateBundleErr := s.dependencyRevision(txCtx, taskSlug)
		if generateBundleErr != nil {
			return generateBundleErr
		}
		now := s.now()
		packageBody := map[string]any{
			"task_slug":           taskSlug,
			"workspace":           workspace,
			"generated_at":        now.UTC().Format(time.RFC3339),
			"spec_version":        bundle.Spec.Version,
			"dependency_revision": dependencyRevision,
			"bundle":              bundle,
		}
		contexts, generateBundleErr := s.readScopeContexts(txCtx, []string{taskSlug})
		if generateBundleErr != nil {
			return generateBundleErr
		}
		packageBody["scope_context"] = contexts[taskSlug]
		if _, err := s.exec(txCtx,
			`INSERT INTO context_packages (id, project_slug, task_spec_slug, body, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			pkgID, s.project, taskSlug, packageBody, now,
		); err != nil {
			return fmt.Errorf("postgres: prepare run package: %w", err)
		}
		if _, err := s.exec(txCtx,
			`INSERT INTO run_bindings (id, project_slug, task_spec_slug, package_id, generation, workspace, state, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, 1, $5, 'prepared', $6, $6)`,
			bindingID, s.project, taskSlug, pkgID, workspace, now,
		); err != nil {
			return fmt.Errorf("postgres: prepare run binding: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return bindingID, nil
}

// BindRunThread is the legacy binding API for identities without an environment.
// It cannot replace or upgrade an existing environment-qualified identity.
func (s *Store) BindRunThread(ctx context.Context, bindingID, threadRef string) error {
	return s.bindRunThread(ctx, bindingID, "", threadRef)
}

// BindRunThreadInEnvironment assigns an explicit environment/thread identity once.
// Repeating it never resets the run's subsequent state or update timestamp.
func (s *Store) BindRunThreadInEnvironment(ctx context.Context, bindingID, environmentID, threadRef string) error {
	if !validMailText(environmentID, 256) || !validMailText(threadRef, 256) {
		return storage.ErrInvalidRunBinding
	}
	return s.bindRunThread(ctx, bindingID, environmentID, threadRef)
}

func (s *Store) bindRunThread(ctx context.Context, bindingID, environmentID, threadRef string) error {
	return s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var slug, state, priorThread, priorEnvironment, executor string
		var targetOK bool
		err := s.queryRow(txCtx, `SELECT rb.task_spec_slug,rb.state,rb.thread_ref,rb.environment_id,rb.executor_kind,
 NOT EXISTS(SELECT 1 FROM context_packages cp WHERE cp.project_slug=rb.project_slug AND cp.id=rb.package_id
 AND ((cp.body->'dispatch_target' ? 'environmentId' AND cp.body->'dispatch_target'->>'environmentId' IS DISTINCT FROM $4)
 OR (cp.body->'dispatch_target' ? 'threadId' AND cp.body->'dispatch_target'->>'threadId' IS DISTINCT FROM $3)))
 FROM run_bindings rb WHERE rb.project_slug=$1 AND rb.id=$2 FOR UPDATE`, s.project, bindingID, threadRef, environmentID).
			Scan(&slug, &state, &priorThread, &priorEnvironment, &executor, &targetOK)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read run binding before bind: %w", err)
		}
		if executor != "agent" || !targetOK {
			return storage.ErrRunBindingConflict
		}
		if priorThread == threadRef && priorEnvironment == environmentID && state != "prepared" {
			return nil
		}
		if state != "prepared" || priorThread != "" || priorEnvironment != "" {
			return storage.ErrRunBindingConflict
		}
		if err := s.rejectNewNodeExecution(txCtx, slug); err != nil {
			return err
		}
		_, err = s.exec(txCtx, `UPDATE run_bindings SET thread_ref=$3,environment_id=$4,state='bound',updated_at=$5
 WHERE project_slug=$1 AND id=$2 AND state='prepared'`, s.project, bindingID, threadRef, environmentID, s.now())
		return err
	})
}

// CreateDelivery stores a candidate delivery against a run binding.
func (s *Store) CreateDelivery(ctx context.Context, bindingID string, snapshot json.RawMessage, submittedBy string, expectedAttemptID ...string) (string, error) {
	if len(expectedAttemptID) > 1 {
		return "", storage.ErrInvalidCandidateLoop
	}
	attemptID := ""
	if len(expectedAttemptID) == 1 {
		attemptID = expectedAttemptID[0]
	}
	deliveryID := newID("dlv")
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var slug, stage string
		err := s.queryRow(txCtx, `SELECT s.slug,s.stage FROM specs s
			JOIN run_bindings rb ON rb.project_slug = s.project_slug AND rb.task_spec_slug = s.slug
			WHERE rb.project_slug = $1 AND rb.id = $2 FOR UPDATE OF s`, s.project, bindingID).Scan(&slug, &stage)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: delivery task lock: %w", err)
		}
		var controlled bool
		if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM candidate_loops WHERE project_slug=$1 AND run_id=$2)`, s.project, bindingID).Scan(&controlled); err != nil {
			return fmt.Errorf("postgres: check candidate delivery owner: %w", err)
		}
		if controlled {
			if attemptID == "" {
				return storage.ErrInvalidCandidateLoop
			}
			var priorDelivery *string
			var abandoned bool
			err := s.queryRow(txCtx, `SELECT a.delivery_id,l.abandoned_at IS NOT NULL FROM candidate_attempts a
 JOIN candidate_loops l ON l.project_slug=a.project_slug AND l.run_id=a.run_id
 WHERE a.project_slug=$1 AND a.run_id=$2 AND a.id=$3`, s.project, bindingID, attemptID).Scan(&priorDelivery, &abandoned)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrCandidateLoopConflict
			}
			if err != nil {
				return fmt.Errorf("postgres: read candidate delivery attempt: %w", err)
			}
			if priorDelivery != nil {
				var same bool
				if err := s.queryRow(txCtx, `SELECT snapshot=$3::jsonb AND submitted_by=$4 FROM deliveries WHERE project_slug=$1 AND id=$2`, s.project, *priorDelivery, snapshot, submittedBy).Scan(&same); err != nil {
					return fmt.Errorf("postgres: compare candidate delivery replay: %w", err)
				}
				if !same {
					return storage.ErrCandidateLoopConflict
				}
				deliveryID = *priorDelivery
				return nil
			}
			if abandoned {
				return storage.ErrCandidateLoopStopped
			}
			var currentID string
			if err := s.queryRow(txCtx, `SELECT id FROM candidate_attempts WHERE project_slug=$1 AND run_id=$2 ORDER BY ordinal DESC LIMIT 1`, s.project, bindingID).Scan(&currentID); err != nil {
				return fmt.Errorf("postgres: read current candidate attempt: %w", err)
			}
			if attemptID != currentID {
				return storage.ErrCandidateLoopConflict
			}
			if _, err := testDeliveryCommit(snapshot); err != nil {
				return storage.ErrInvalidCandidateLoop
			}
		} else if attemptID != "" {
			return storage.ErrInvalidCandidateLoop
		}
		if stage == string(storage.SpecStageSuperseded) {
			return storage.ErrSpecTerminal
		}
		if err := s.rejectHumanOwnedNode(txCtx, slug); err != nil {
			return err
		}
		tag, err := s.exec(txCtx,
			`INSERT INTO deliveries (id, project_slug, run_binding_id, snapshot, submitted_by, submitted_at)
		 SELECT $1, $2, rb.id, $4, $5, $6
		 FROM run_bindings rb WHERE rb.id = $3 AND rb.project_slug = $2`,
			deliveryID, s.project, bindingID, snapshot, submittedBy, s.now(),
		)
		if err != nil {
			return fmt.Errorf("postgres: create delivery: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("postgres: create delivery: %q: %w", bindingID, storage.ErrRunBindingNotFound)
		}
		if controlled {
			if _, err := s.exec(txCtx, `UPDATE candidate_attempts SET delivery_id=$4 WHERE project_slug=$1 AND run_id=$2 AND id=$3 AND delivery_id IS NULL`, s.project, bindingID, attemptID, deliveryID); err != nil {
				return fmt.Errorf("postgres: bind candidate delivery: %w", err)
			}
		}
		// Non-Git deliveries remain ordinary deliveries. Hooks only match the
		// existing validated snapshot contract, never guessed alternate shapes.
		if commit, err := testDeliveryCommit(snapshot); err == nil {
			if _, err := s.exec(txCtx, `UPDATE delivery_test_hooks SET delivery_id=$3,triggered_at=$5
				WHERE hook_kind='delivery_test' AND project_slug=$1 AND source_run_id=$2 AND commit_sha=$4 AND delivery_id IS NULL AND cancelled_at IS NULL`,
				s.project, bindingID, deliveryID, commit, s.now()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return deliveryID, nil
}

// RunBindingTask returns the task spec slug for a run binding.
func (s *Store) RunBindingTask(ctx context.Context, bindingID string) (string, error) {
	var task string
	err := s.queryRow(ctx,
		`SELECT task_spec_slug FROM run_bindings WHERE id = $1 AND project_slug = $2`,
		bindingID, s.project,
	).Scan(&task)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("postgres: run binding task: %q: %w", bindingID, storage.ErrRunBindingNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("postgres: run binding task: %w", err)
	}
	return task, nil
}
