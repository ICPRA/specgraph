// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/specgraph/specgraph/internal/storage"
)

// Compile-time interface assertion.
var _ storage.ExecutionBackend = (*Store)(nil)

// GenerateBundle assembles a bundle from the spec and its linked decisions,
// active claim, and upstream dependencies with drift state.
func (s *Store) GenerateBundle(ctx context.Context, slug string) (*storage.Bundle, error) {
	spec, err := s.GetSpec(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: generate bundle: %w", err)
	}
	if spec.Role == storage.SpecRoleSummary {
		return nil, storage.ErrSummaryNotExecutable
	}

	if spec.Stage != storage.SpecStageApproved && string(spec.Stage) != "in_progress" {
		return nil, fmt.Errorf("postgres: generate bundle for %q: %w", slug, storage.ErrSpecNotApproved)
	}

	decisions, err := s.fetchLinkedDecisions(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: generate bundle decisions: %w", err)
	}

	claim, err := s.GetActiveClaim(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: generate bundle claim: %w", err)
	}

	deps, err := s.fetchBundleDependencies(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: generate bundle dependencies: %w", err)
	}

	return &storage.Bundle{
		Version:      2,
		Spec:         spec,
		Decisions:    decisions,
		Claim:        claim,
		Dependencies: deps,
	}, nil
}

// RecordProgress stores a progress event from an executing agent.
func (s *Store) RecordProgress(ctx context.Context, slug, agent, message string) error {
	_, err := s.recordClaimedEvent(ctx, slug, agent, "progress", message)
	return err
}

// RecordBlocker stores a blocker event from an executing agent.
func (s *Store) RecordBlocker(ctx context.Context, slug, agent, description string) error {
	_, err := s.recordClaimedEvent(ctx, slug, agent, "blocker", description)
	return err
}

// RecordCompletion stores a completion event and transitions the spec to done.
// The entire operation is atomic: claim verification, event insertion, spec
// stage transition, content hash recomputation, changelog checkpoint, claim
// deletion, and dependency hash refresh all occur in a single transaction.
func (s *Store) RecordCompletion(ctx context.Context, slug, agent string) error {
	return s.recordCompletion(ctx, slug, agent, "")
}

// CompleteOwnRun derives both selectors from the trusted host's current bound identity.
func (s *Store) CompleteOwnRun(ctx context.Context, scope storage.MailScope) (storage.RunSelfCompletion, error) {
	var result storage.RunSelfCompletion
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		run, _, err := s.reviewActorRun(txCtx, scope)
		if err != nil {
			return err
		}
		slug, err := s.RunBindingTask(txCtx, run)
		if err != nil {
			return err
		}
		if err := s.RecordCompletion(txCtx, slug, run); err != nil {
			return err
		}
		result = storage.RunSelfCompletion{RunID: run, Completed: slug}
		return nil
	})
	return result, err
}

func (s *Store) recordCompletion(ctx context.Context, slug, agent, expectedRequest string) error {
	return s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		var program bool
		if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_bindings WHERE project_slug=$1 AND id=$2 AND executor_kind='program')`, s.project, agent).Scan(&program); err != nil {
			return fmt.Errorf("postgres: recordCompletion: %w", err)
		}
		// Serialize with ClaimSpec before checking ownership: an expiring lease
		// must not be replaced between this check and the done transition.
		var stage string
		var version int32
		var role string
		err := s.queryRow(txCtx,
			`SELECT stage, version, role
			 FROM specs WHERE slug = $1 AND project_slug = $2 FOR UPDATE`,
			slug, s.project,
		).Scan(&stage, &version, &role)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: record completion: spec %q: %w", slug, storage.ErrSpecNotFound)
		}
		if err != nil {
			return fmt.Errorf("postgres: record completion: read spec: %w", err)
		}
		if stage == string(storage.SpecStageSuperseded) {
			return storage.ErrSpecTerminal
		}
		var alreadyCompleted bool
		if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion')`, s.project, slug, agent).Scan(&alreadyCompleted); err != nil {
			return fmt.Errorf("postgres: read completion replay identity: %w", err)
		}
		if !alreadyCompleted {
			if err := s.rejectHumanOwnedNode(txCtx, slug); err != nil {
				return err
			}
		}
		if role == string(storage.SpecRoleSummary) {
			return storage.ErrSummaryNotExecutable
		}
		var programCompletion *storage.ProgramCompletion
		if program {
			var checkProgramCompletionErr error
			programCompletion, checkProgramCompletionErr = s.checkProgramCompletion(txCtx, slug, agent, stage)
			if checkProgramCompletionErr != nil {
				return checkProgramCompletionErr
			}
		}
		review, err := s.ReadReviewStatus(txCtx, slug)
		if err != nil {
			return err
		}
		hasReview := false
		for _, state := range review.Reviews {
			hasReview = hasReview || state.Request != nil
			if state.HumanHold {
				return storage.ErrReviewHumanHold
			}
		}
		var purpose string
		err = s.queryRow(txCtx, `SELECT COALESCE(cp.body->(CASE WHEN rb.executor_kind='program' THEN 'program_target' ELSE 'dispatch_target' END)->>'workPurpose','') FROM run_bindings rb
		 JOIN context_packages cp ON cp.project_slug=rb.project_slug AND cp.id=rb.package_id
		 WHERE rb.project_slug=$1 AND rb.id=$2 AND rb.task_spec_slug=$3`, s.project, agent, slug).Scan(&purpose)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: recordCompletion: %w", err)
		}
		authoring := purpose == "requirements" || purpose == "design"
		if expectedRequest != "" && !authoring {
			return storage.ErrInvalidReview
		}
		if authoring {
			outputs, checkAuthoringCompletionErr := s.checkAuthoringCompletion(txCtx, slug, agent, purpose, expectedRequest)
			if checkAuthoringCompletionErr != nil {
				return checkAuthoringCompletionErr
			}
			if err := s.checkPreparedContractWithOutputs(txCtx, slug, agent, outputs); err != nil {
				return err
			}
			if stage == "done" {
				var completed bool
				if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM execution_events WHERE project_slug=$1 AND spec_slug=$2 AND agent=$3 AND event_type='completion')`, s.project, slug, agent).Scan(&completed); err != nil {
					return fmt.Errorf("postgres: recordCompletion: %w", err)
				}
				if !completed {
					return storage.ErrRunBindingConflict
				}
				return nil
			}
		} else if !program {
			if assertActiveClaimErr := s.assertActiveClaim(txCtx, slug, agent); assertActiveClaimErr != nil {
				return assertActiveClaimErr
			}
		}

		// Explicit work purposes determine completion evidence. Unknown legacy
		// purposes retain their original managed-project delivery-opinion gate.
		managed, err := s.projectManaged(txCtx)
		if err != nil {
			return err
		}
		var candidateEvidence *candidateCompletionEvidence
		switch {
		case purpose == "implementation":
			candidateEvidence, err = s.candidateCompletionDelivery(txCtx, agent, slug)
			if err != nil {
				return err
			}
			candidateDelivery := ""
			if candidateEvidence != nil {
				candidateDelivery = candidateEvidence.DeliveryID
			}
			if checkImplementationTestsErr := s.checkImplementationTestsForDelivery(txCtx, slug, agent, candidateDelivery); checkImplementationTestsErr != nil {
				return checkImplementationTestsErr
			}
			if checkPreparedContractErr := s.checkPreparedContract(txCtx, slug, agent); checkPreparedContractErr != nil {
				return checkPreparedContractErr
			}
		case purpose == "test_design" || purpose == "test_execution" || purpose == "requirements_review" || purpose == "design_review" || purpose == "investigation" || purpose == "coordination" || purpose == "knowledge":
			// Finishing this assignment is not approval of its sources or a claim that tested code passed.
			if checkPreparedContractErr := s.checkPreparedContract(txCtx, slug, agent); checkPreparedContractErr != nil {
				return checkPreparedContractErr
			}
		case !authoring && (managed || hasReview):
			if hasReview {
				var currentRun bool
				if scanErr := s.queryRow(txCtx, `SELECT COALESCE((SELECT d.run_binding_id=$3 FROM deliveries d
				 JOIN run_bindings b ON b.project_slug=d.project_slug AND b.id=d.run_binding_id
				 WHERE b.project_slug=$1 AND b.task_spec_slug=$2 ORDER BY d.submitted_at DESC,d.id DESC LIMIT 1),false)`, s.project, slug, agent).Scan(&currentRun); scanErr != nil {
					return fmt.Errorf("postgres: recordCompletion: %w", scanErr)
				}
				if !currentRun {
					return storage.ErrManagedCompletionRequiresAcceptance
				}
			}
			accepted, hasAcceptedDeliveryErr := s.hasAcceptedDelivery(txCtx, slug, agent)
			if hasAcceptedDeliveryErr != nil {
				return hasAcceptedDeliveryErr
			}
			if !accepted {
				return storage.ErrManagedCompletionRequiresAcceptance
			}
			if err := s.checkPreparedContract(txCtx, slug, agent); err != nil {
				return err
			}
		}

		if program && stage == "done" {
			return nil
		}

		// Insert completion event.
		eventID := newID("evt")
		now := s.now()
		var encodedProgramBasis []byte
		if programCompletion != nil {
			basis := map[string]any{"attemptId": programCompletion.AttemptID}
			if programCompletion.JudgmentID != nil {
				basis["judgmentId"] = programCompletion.JudgmentID
			}
			encodedProgramBasis, err = json.Marshal(basis)
			if err != nil {
				return fmt.Errorf("postgres: encode program completion basis: %w", err)
			}
		}
		_, err = s.exec(txCtx,
			`INSERT INTO execution_events (id, spec_slug, project_slug, agent, event_type, message, created_at,program_completion_basis)
			 VALUES ($1, $2, $3, $4, 'completion', '', $5,$6)`,
			eventID, slug, s.project, agent, now, encodedProgramBasis,
		)
		if err != nil {
			return fmt.Errorf("postgres: record completion event: %w", err)
		}

		// Insert HAS_EVENT edge.
		_, err = s.exec(txCtx,
			`INSERT INTO edges (from_slug, to_slug, edge_type, project_slug)
			 VALUES ($1, $2, 'HAS_EVENT', $3)`,
			slug, eventID, s.project,
		)
		if err != nil {
			return fmt.Errorf("postgres: record completion HAS_EVENT edge: %w", err)
		}

		newStage := "done"

		// Transition spec to done, bump version.
		tag, err := s.exec(txCtx,
			`UPDATE specs SET stage = 'done', version = version + 1,
			     updated_at = $1
			 WHERE slug = $2 AND project_slug = $3`,
			now, slug, s.project,
		)
		if err != nil {
			return fmt.Errorf("postgres: record completion: update spec: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("postgres: record completion: spec %q not updated", slug)
		}

		// Recompute content hash including authoring outputs.
		if hashErr := s.recomputeContentHash(txCtx, slug); hashErr != nil {
			return fmt.Errorf("postgres: record completion: %w", hashErr)
		}

		// Re-read spec to get updated content hash for changelog.
		var ch string
		err = s.queryRow(txCtx,
			`SELECT content_hash FROM specs WHERE slug = $1 AND project_slug = $2`,
			slug, s.project,
		).Scan(&ch)
		if err != nil {
			return fmt.Errorf("postgres: record completion: read content_hash: %w", err)
		}

		// Create checkpoint changelog entry.
		oldFields := &storage.SpecFields{Stage: stage}
		newFields := &storage.SpecFields{Stage: newStage}
		deltas := storage.ComputeFieldDeltas(oldFields, newFields)
		clEntry := &storage.ChangeLogEntry{
			Version:     version + 1,
			Stage:       newStage,
			ContentHash: ch,
			Checkpoint:  true,
			Summary:     "Spec completed",
			Date:        now,
		}
		if clErr := s.createChangeLog(txCtx, slug, clEntry, deltas); clErr != nil {
			return clErr
		}

		// Delete the claim.
		_, err = s.exec(txCtx,
			`DELETE FROM claims WHERE project_slug = $1 AND spec_slug = $2 AND agent = $3`,
			s.project, slug, agent,
		)
		if err != nil {
			return fmt.Errorf("postgres: record completion: delete claim: %w", err)
		}

		// Delete CLAIMED_BY edge.
		_, err = s.exec(txCtx,
			`DELETE FROM edges WHERE project_slug = $1 AND from_slug = $2 AND to_slug = $3 AND edge_type = 'CLAIMED_BY'`,
			s.project, slug, agent,
		)
		if err != nil {
			return fmt.Errorf("postgres: record completion: delete CLAIMED_BY edge: %w", err)
		}

		// Refresh dependency hashes on all specs that depend on this one.
		if err := s.RefreshDependencyHashes(txCtx, slug); err != nil {
			return fmt.Errorf("postgres: record completion: refresh dependency hashes: %w", err)
		}
		if managed || authoring || program {
			tag, err := s.exec(txCtx, `UPDATE run_bindings SET state = 'completed', updated_at = $1
				WHERE project_slug = $2 AND id = $3 AND task_spec_slug = $4`, now, s.project, agent, slug)
			if err != nil {
				return fmt.Errorf("postgres: complete run binding: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return storage.ErrRunBindingNotFound
			}
		}
		if candidateEvidence != nil {
			var encodedJoin, encodedSatisfaction []byte
			if candidateEvidence.Join != nil {
				encodedJoin, err = json.Marshal(candidateEvidence.Join)
				if err != nil {
					return fmt.Errorf("postgres: encode candidate completed join: %w", err)
				}
			}
			if candidateEvidence.Satisfaction != nil {
				encodedSatisfaction, err = json.Marshal(candidateEvidence.Satisfaction)
				if err != nil {
					return fmt.Errorf("postgres: encode candidate completed satisfaction: %w", err)
				}
			}
			encodedCondition, err := json.Marshal(candidateEvidence.Condition)
			if err != nil {
				return fmt.Errorf("postgres: encode candidate completed reports: %w", err)
			}
			if _, err := s.exec(txCtx, `UPDATE candidate_loops SET resolved_at=$3,completed_join=$4,completed_condition=$5,completed_satisfaction=$6
 WHERE project_slug=$1 AND run_id=$2 AND resolved_at IS NULL`, s.project, agent, now, encodedJoin, encodedCondition, encodedSatisfaction); err != nil {
				return fmt.Errorf("postgres: close candidate budget: %w", err)
			}
		}
		return s.triggerCompletionHooks(txCtx, slug, "execution", eventID)
	})
}

// GetExecutionEvents returns execution events for a spec, ordered by time descending.
// When limit is 0, all events are returned (no LIMIT clause).
func (s *Store) GetExecutionEvents(ctx context.Context, slug string, limit int) ([]*storage.ExecutionEvent, error) {
	rows, err := s.query(ctx,
		`SELECT id, spec_slug, agent, event_type, message, created_at
		 FROM execution_events
		 WHERE spec_slug = $1 AND project_slug = $2
		 ORDER BY created_at DESC, id DESC
		 LIMIT CASE WHEN $3 > 0 THEN $3 END`,
		slug, s.project, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: get execution events: %w", err)
	}
	defer rows.Close()

	var events []*storage.ExecutionEvent
	for rows.Next() {
		var (
			id        string
			specSlug  string
			agent     string
			typeStr   string
			message   string
			createdAt time.Time
		)
		if scanErr := rows.Scan(&id, &specSlug, &agent, &typeStr, &message, &createdAt); scanErr != nil {
			return nil, fmt.Errorf("postgres: get execution events: scan: %w", scanErr)
		}
		eventType, parseErr := storage.ParseExecutionEventType(typeStr)
		if parseErr != nil {
			return nil, fmt.Errorf("postgres: get execution events: parse type: %w", parseErr)
		}
		events = append(events, &storage.ExecutionEvent{
			ID:        id,
			SpecSlug:  specSlug,
			Agent:     agent,
			Type:      eventType,
			Message:   message,
			CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: get execution events: iterate: %w", err)
	}
	return events, nil
}

// GetPrimeData returns the data needed to compose a prime response.
func (s *Store) GetPrimeData(ctx context.Context, slug string) (*storage.PrimeData, error) {
	spec, err := s.GetSpec(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: get prime data: %w", err)
	}

	decisions, err := s.fetchLinkedDecisions(ctx, slug)
	if err != nil {
		return nil, fmt.Errorf("postgres: get prime data decisions: %w", err)
	}

	pd := &storage.PrimeData{
		Spec:      spec,
		Decisions: decisions,
	}

	merged, err := s.GetMergedConstitution(ctx)
	switch {
	case err == nil:
		pd.Constitution = merged.Constitution
		pd.ConstitutionProvenance = make([]storage.ProvenanceEntry, 0, len(merged.Provenance))
		for path, layer := range merged.Provenance {
			pd.ConstitutionProvenance = append(pd.ConstitutionProvenance, storage.ProvenanceEntry{
				Path:  path,
				Layer: layer,
			})
		}
		sort.Slice(pd.ConstitutionProvenance, func(i, j int) bool {
			return pd.ConstitutionProvenance[i].Path < pd.ConstitutionProvenance[j].Path
		})
	case errors.Is(err, storage.ErrConstitutionNotFound):
		// No layers exist — leave Constitution nil and Provenance empty.
		// Matches existing no-constitution behavior.
	default:
		return nil, fmt.Errorf("postgres: get prime data constitution: %w", err)
	}

	return pd, nil
}

// ReleaseExpiredClaims releases expired claims without an unresolved dispatch.
// Returns the count of released claims.
func (s *Store) ReleaseExpiredClaims(ctx context.Context) (int, error) {
	var count int
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		now := s.now()

		rows, qErr := s.query(txCtx,
			`DELETE FROM claims
			 WHERE project_slug = $1 AND lease_expires < $2
			 AND NOT EXISTS (
				 SELECT 1 FROM run_dispatches d
				 WHERE d.project_slug = claims.project_slug AND d.run_id = claims.agent
				 AND d.released_at IS NULL
			 )
			 RETURNING spec_slug, agent`,
			s.project, now,
		)
		if qErr != nil {
			return fmt.Errorf("postgres: release expired claims: %w", qErr)
		}
		defer rows.Close()

		type claimRow struct{ specSlug, agent string }
		var expired []claimRow
		for rows.Next() {
			var cr claimRow
			if scanErr := rows.Scan(&cr.specSlug, &cr.agent); scanErr != nil {
				return fmt.Errorf("postgres: release expired claims: scan: %w", scanErr)
			}
			expired = append(expired, cr)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return fmt.Errorf("postgres: release expired claims: iterate: %w", rowsErr)
		}

		for _, cr := range expired {
			_, delErr := s.exec(txCtx,
				`DELETE FROM edges WHERE project_slug = $1 AND from_slug = $2 AND to_slug = $3 AND edge_type = 'CLAIMED_BY'`,
				s.project, cr.specSlug, cr.agent,
			)
			if delErr != nil {
				return fmt.Errorf("postgres: release expired claims: delete edge for %q: %w", cr.specSlug, delErr)
			}
		}

		count = len(expired)
		return nil
	})
	return count, err
}

// GetActiveClaim returns the active (non-expired) claim for a spec, or nil
// if the spec is unclaimed. Implements storage.ClaimBackend.GetActiveClaim.
func (s *Store) GetActiveClaim(ctx context.Context, slug string) (*storage.Claim, error) {
	now := s.now()
	var agent string
	var claimedAt, leaseExpires time.Time

	err := s.queryRow(ctx,
		`SELECT agent, claimed_at, lease_expires
		 FROM claims
		 WHERE project_slug = $1 AND spec_slug = $2 AND lease_expires >= $3`,
		s.project, slug, now,
	).Scan(&agent, &claimedAt, &leaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: fetch active claim: %w", err)
	}

	return &storage.Claim{
		Slug:         slug,
		Agent:        agent,
		ClaimedAt:    claimedAt,
		LeaseExpires: leaseExpires,
	}, nil
}

// fetchBundleDependencies returns dependency info with drift state for the bundle.
func (s *Store) fetchBundleDependencies(ctx context.Context, slug string) ([]storage.DependencyInfo, error) {
	refs, err := s.GetDependenciesWithEdgeData(ctx, slug)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}

	deps := make([]storage.DependencyInfo, 0, len(refs))
	for _, ref := range refs {
		drifted := ref.ContentHashAtLink == "" || ref.ContentHashAtLink != ref.UpstreamContentHash
		var note string
		if drifted {
			if ref.ContentHashAtLink == "" {
				note = "dependency not yet baselined"
			} else {
				note = "content changed since baseline"
			}
		}
		deps = append(deps, storage.DependencyInfo{
			Slug:    ref.Slug,
			Stage:   storage.SpecStage(ref.Stage),
			Drifted: drifted,
			Note:    note,
		})
	}
	return deps, nil
}

// recordClaimedEvent verifies claim ownership and atomically inserts an execution event
// and its HAS_EVENT edge within a single transaction.
func (s *Store) recordClaimedEvent(ctx context.Context, slug, agent, eventType, message string, requestedID ...string) (string, error) {
	eventID := newID("evt")
	if len(requestedID) != 0 {
		eventID = requestedID[0]
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		if len(requestedID) != 0 {
			var priorSlug, priorAgent, priorType, priorMessage string
			err := s.queryRow(txCtx, `SELECT spec_slug,agent,event_type,message FROM execution_events WHERE project_slug=$1 AND id=$2`, s.project, eventID).
				Scan(&priorSlug, &priorAgent, &priorType, &priorMessage)
			if err == nil {
				if priorSlug != slug || priorAgent != agent || priorType != eventType || priorMessage != message {
					return storage.ErrNodeOwnershipConflict
				}
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("postgres: read claimed event replay: %w", err)
			}
		}
		if err := s.rejectHumanOwnedNode(txCtx, slug); err != nil {
			return err
		}
		if pending, err := s.pendingNodeOwnership(txCtx, slug); err != nil {
			return err
		} else if pending != nil {
			captured := false
			for _, item := range pending.Dispatches {
				captured = captured || item.RunID == agent
			}
			for _, item := range pending.Preparations {
				captured = captured || item.RunID == agent
			}
			if pending.FrozenClaim != nil && pending.FrozenClaim.Agent == agent {
				current, err := s.readNodeClaim(txCtx, slug)
				if err != nil {
					return err
				}
				if current == nil || current.Agent != agent || !current.ClaimedAt.Equal(pending.FrozenClaim.ClaimedAt) {
					return storage.ErrNodeOwnershipPending
				}
				captured = true
			}
			if !captured {
				return storage.ErrNodeOwnershipPending
			}
		}
		if err := s.assertActiveClaim(txCtx, slug, agent); err != nil {
			return err
		}

		now := s.now()

		_, err := s.exec(txCtx,
			`INSERT INTO execution_events (id, spec_slug, project_slug, agent, event_type, message, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			eventID, slug, s.project, agent, eventType, message, now,
		)
		if err != nil {
			return fmt.Errorf("postgres: record %s event: %w", eventType, err)
		}

		_, err = s.exec(txCtx,
			`INSERT INTO edges (from_slug, to_slug, edge_type, project_slug)
			 VALUES ($1, $2, 'HAS_EVENT', $3)`,
			slug, eventID, s.project,
		)
		if err != nil {
			return fmt.Errorf("postgres: record %s HAS_EVENT edge: %w", eventType, err)
		}

		return nil
	})
	return eventID, err
}

// assertActiveClaim checks that the given agent holds a non-expired claim on slug.
// Returns ErrAgentNotClaimOwner if the claim does not exist or belongs to someone else.
func (s *Store) assertActiveClaim(ctx context.Context, slug, agent string) error {
	now := s.now()
	var found string
	err := s.queryRow(ctx,
		`SELECT agent FROM claims
		 WHERE project_slug = $1 AND spec_slug = $2 AND agent = $3 AND lease_expires >= $4`,
		s.project, slug, agent, now,
	).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("postgres: %w", storage.ErrAgentNotClaimOwner)
	}
	if err != nil {
		return fmt.Errorf("postgres: assert active claim: %w", err)
	}
	return nil
}

// fetchLinkedDecisions retrieves all decisions linked to a spec via DECIDED_IN edges.
func (s *Store) fetchLinkedDecisions(ctx context.Context, slug string) ([]*storage.Decision, error) {
	rows, err := s.query(ctx,
		`SELECT to_slug FROM edges
		 WHERE from_slug = $1 AND edge_type = 'DECIDED_IN' AND project_slug = $2`,
		slug, s.project,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: fetch linked decisions: %w", err)
	}
	defer rows.Close()

	var decisionSlugs []string
	for rows.Next() {
		var toSlug string
		if scanErr := rows.Scan(&toSlug); scanErr != nil {
			return nil, fmt.Errorf("postgres: fetch linked decisions: scan: %w", scanErr)
		}
		decisionSlugs = append(decisionSlugs, toSlug)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: fetch linked decisions: iterate: %w", err)
	}

	decisions := make([]*storage.Decision, 0, len(decisionSlugs))
	for _, dSlug := range decisionSlugs {
		d, err := s.GetDecision(ctx, dSlug)
		if err != nil {
			return nil, fmt.Errorf("postgres: fetch linked decisions: get %q: %w", dSlug, err)
		}
		decisions = append(decisions, d)
	}
	return decisions, nil
}
