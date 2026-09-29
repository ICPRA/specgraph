// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

// ReadReviewStatus returns current requirements and design review states in one snapshot.
func (s *Store) ReadReviewStatus(ctx context.Context, slug string) (*storage.ReviewStatus, error) {
	states, err := s.readReviewStatuses(ctx, slug)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		return nil, storage.ErrSpecNotFound
	}
	return &states[0], nil
}

// ReadReviewDecisionRequest reads the request associated with a review decision.
func (s *Store) ReadReviewDecisionRequest(ctx context.Context, id string) (*storage.ReviewRequest, error) {
	if !validReviewID(id) {
		return nil, storage.ErrInvalidReview
	}
	var request *storage.ReviewRequest
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var requestID string
		err := s.queryRow(snapshotCtx, `SELECT request_id::text FROM review_decisions WHERE project_slug=$1 AND id=$2::bigint`, s.project, id).Scan(&requestID)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReviewRequestNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read review decision request: %w", err)
		}
		request, err = s.ReadReviewRequest(snapshotCtx, requestID)
		return err
	})
	return request, err
}

// ReadReviewRequest reads a source review request and its recorded decisions.
func (s *Store) ReadReviewRequest(ctx context.Context, id string) (*storage.ReviewRequest, error) {
	if !validReviewID(id) {
		return nil, storage.ErrInvalidReview
	}
	var encoded []byte
	err := s.queryRow(ctx, `SELECT jsonb_build_object('id',r.id::text,'taskSlug',r.task_slug,'kind',r.kind,
 'sources',r.sources,'authorResponsibility',r.author_responsibility,'requirementDecisionIds',r.requirement_decision_ids,
 'reviewerRunId',r.reviewer_run_id,'completionRunId',r.completion_run_id,'responsibleUserId',r.responsible_user_id,'maxReviewRounds',r.max_review_rounds,
 'createdBy',r.created_by,'createdAt',r.created_at,'decidedAt',r.decided_at,
 'decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',d.id::text,'requestId',d.request_id::text,
 'verdict',d.verdict,'basis',d.basis,'actorKind',d.actor_kind,'actor',d.actor,'reviewerRunId',d.reviewer_run_id,'createdAt',d.created_at) ORDER BY d.id)
 FROM review_decisions d WHERE d.project_slug=r.project_slug AND d.request_id=r.id),'[]'::jsonb))
 FROM review_requests r WHERE r.project_slug=$1 AND r.id=$2::bigint`, s.project, id).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrReviewRequestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read review request: %w", err)
	}
	var req storage.ReviewRequest
	if decodeErr := json.Unmarshal(encoded, &req); decodeErr != nil {
		return nil, fmt.Errorf("postgres: decode review request: %w", decodeErr)
	}
	return &req, nil
}

func (s *Store) readReviewStatuses(ctx context.Context, slug string) ([]storage.ReviewStatus, error) {
	rows, err := s.query(ctx, `SELECT jsonb_build_object('taskSlug',s.slug,'reviews',(
 SELECT jsonb_agg(jsonb_build_object(
 'kind',k.kind,'agentRejections',COALESCE(r.agent_rejections,0),'maxReviewRounds',COALESCE(r.max_review_rounds,$3::integer),
 'humanHold',COALESCE(r.human_hold,false),'responsibleUserId',r.responsible_user_id,
 'request',CASE WHEN r.id IS NULL THEN NULL ELSE jsonb_build_object(
 'id',r.id::text,'taskSlug',r.task_slug,'kind',r.kind,'sources',r.sources,'authorResponsibility',r.author_responsibility,
 'requirementDecisionIds',r.requirement_decision_ids,'reviewerRunId',r.reviewer_run_id,'completionRunId',r.completion_run_id,
 'responsibleUserId',r.responsible_user_id,'maxReviewRounds',r.max_review_rounds,
 'createdBy',r.created_by,'createdAt',r.created_at,'decidedAt',r.decided_at,
 'decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',d.id::text,'requestId',d.request_id::text,
 'verdict',d.verdict,'basis',d.basis,'actorKind',d.actor_kind,'actor',d.actor,'reviewerRunId',d.reviewer_run_id,'createdAt',d.created_at) ORDER BY d.id)
 FROM review_decisions d WHERE d.project_slug=r.project_slug AND d.request_id=r.id),'[]'::jsonb)) END) ORDER BY k.ordinal)
 FROM (VALUES('requirements',1),('design',2)) k(kind,ordinal)
 LEFT JOIN LATERAL (SELECT * FROM review_requests WHERE project_slug=s.project_slug AND task_slug=s.slug AND kind=k.kind ORDER BY id DESC LIMIT 1) r ON true))
 FROM specs s WHERE s.project_slug = $1 AND ($2='' OR s.slug=$2)
 AND ($2<>'' OR EXISTS(SELECT 1 FROM review_requests r WHERE r.project_slug=s.project_slug AND r.task_slug=s.slug)) ORDER BY s.slug`, s.project, slug, defaultMaxReviewRounds)
	if err != nil {
		return nil, err
	}
	encoded, err := pgx.CollectRows(rows, pgx.RowTo[json.RawMessage])
	if err != nil {
		return nil, fmt.Errorf("postgres: collect review statuses: %w", err)
	}
	states := make([]storage.ReviewStatus, 0, len(encoded))
	for _, body := range encoded {
		var state storage.ReviewStatus
		if decodeErr := json.Unmarshal(body, &state); decodeErr != nil {
			return nil, fmt.Errorf("postgres: decode review status: %w", decodeErr)
		}
		states = append(states, state)
	}
	return states, nil
}

// reviewActorRun resolves the trusted host's live bound identity, not its account owner.
func (s *Store) reviewActorRun(ctx context.Context, scope storage.MailScope) (runID, assignmentRole string, resultErr error) {
	if !validMailScope(scope) {
		return "", "", storage.ErrInvalidReview
	}
	rows, err := s.query(ctx, `SELECT r.id,COALESCE(p.body->'dispatch_target'->>'assignmentRole','') AS role
 FROM run_bindings r LEFT JOIN context_packages p ON p.project_slug=r.project_slug AND p.id=r.package_id
 WHERE r.project_slug=$1 AND r.environment_id=$2 AND r.thread_ref=$3 AND r.state='bound' LIMIT 2`, s.project, scope.EnvironmentID, scope.ThreadID)
	if err != nil {
		return "", "", err
	}
	type actor struct{ ID, Role string }
	actors, err := pgx.CollectRows(rows, pgx.RowToStructByName[actor])
	if err != nil {
		return "", "", fmt.Errorf("postgres: collect review actors: %w", err)
	}
	if len(actors) != 1 {
		return "", "", storage.ErrReviewForbidden
	}
	return actors[0].ID, actors[0].Role, nil
}

// AssignReview fixes source references and declared author responsibility; no delivery is needed.
// A nonnil scope is always the Agent route, even with a human-owned credential.
func (s *Store) AssignReview(ctx context.Context, input *storage.AssignReviewRequest, scope *storage.MailScope) (*storage.ReviewStatus, error) {
	req := *input
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || (scope == nil && identity.UserKind != storage.KindHuman) {
		return nil, storage.ErrReviewForbidden
	}
	if (req.Kind != "requirements" && req.Kind != "design") || len(req.Sources) == 0 || (req.MaxReviewRounds != nil && *req.MaxReviewRounds < 1) {
		return nil, storage.ErrInvalidReview
	}
	account, accountErr := s.ExistingAuth().GetUserByID(ctx, identity.UserID)
	if accountErr != nil {
		return nil, accountErr
	}
	if account.DeletedAt != nil {
		return nil, storage.ErrReviewForbidden
	}
	responsible := account.ID
	if account.Kind == storage.KindServiceAccount {
		responsible = account.OwnerUserID
	} else if account.Kind != storage.KindHuman {
		return nil, storage.ErrReviewForbidden
	}
	human, humanErr := s.ExistingAuth().GetUserByID(ctx, responsible)
	if humanErr != nil {
		return nil, humanErr
	}
	if human.Kind != storage.KindHuman || human.DeletedAt != nil {
		return nil, storage.ErrReviewForbidden
	}
	if req.RequirementDecisionIDs == nil {
		req.RequirementDecisionIDs = []string{}
	}
	transactionErr := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		var task string
		if scanErr := s.queryRow(txCtx, `SELECT slug FROM specs WHERE project_slug=$1 AND slug=$2 FOR UPDATE`, s.project, req.TaskSlug).Scan(&task); scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return storage.ErrSpecNotFound
			}
			return fmt.Errorf("postgres: lock review task: %w", scanErr)
		}
		var assigningRun *string
		if scope != nil {
			run, role, actorErr := s.reviewActorRun(txCtx, *scope)
			if actorErr != nil {
				return actorErr
			}
			if role != "manager" {
				return storage.ErrReviewForbidden
			}
			assigningRun = &run
		}
		for i := range req.Sources {
			source := &req.Sources[i]
			if sourceErr := s.validateReviewSource(txCtx, source); sourceErr != nil {
				return sourceErr
			}
		}
		if authorErr := s.checkReviewAuthor(txCtx, req.AuthorResponsibility, req.ReviewerRunID); authorErr != nil {
			return authorErr
		}
		if requirementsErr := s.checkReviewRequirements(txCtx, req.Kind, req.RequirementDecisionIDs); requirementsErr != nil {
			return requirementsErr
		}
		current, err := s.ReadReviewStatus(txCtx, req.TaskSlug)
		if err != nil {
			return err
		}
		var state storage.ReviewState
		for _, item := range current.Reviews {
			if item.Kind == req.Kind {
				state = item
			}
		}
		maxRounds := state.MaxReviewRounds
		if scope != nil && req.MaxReviewRounds != nil && *req.MaxReviewRounds != maxRounds {
			return storage.ErrInvalidReview
		}
		if scope == nil && req.MaxReviewRounds != nil {
			maxRounds = *req.MaxReviewRounds
		}
		hold := state.HumanHold || state.AgentRejections >= maxRounds
		if submissionErr := s.submitAuthoringSources(txCtx, &req); submissionErr != nil {
			return submissionErr
		}
		sources, err := json.Marshal(req.Sources)
		if err != nil {
			return fmt.Errorf("postgres: encode review sources: %w", err)
		}
		author, err := json.Marshal(req.AuthorResponsibility)
		if err != nil {
			return fmt.Errorf("postgres: encode review author: %w", err)
		}
		decisions, err := json.Marshal(req.RequirementDecisionIDs)
		if err != nil {
			return fmt.Errorf("postgres: encode review prerequisites: %w", err)
		}
		_, err = s.exec(txCtx, `INSERT INTO review_requests(project_slug,task_slug,kind,sources,author_responsibility,requirement_decision_ids,reviewer_run_id,responsible_user_id,max_review_rounds,agent_rejections,human_hold,created_by,assigned_by_run_id,completion_run_id)
   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, s.project, req.TaskSlug, req.Kind, sources, author, decisions, req.ReviewerRunID, responsible, maxRounds, state.AgentRejections, hold, identity.UserID, assigningRun, req.CompletionRunID)
		return err
	})
	if transactionErr != nil {
		return nil, transactionErr
	}
	return s.ReadReviewStatus(ctx, req.TaskSlug)
}

var reviewCommitPattern = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)

func (s *Store) checkReviewRequirements(ctx context.Context, kind string, ids []string) error {
	if (kind == "design" && len(ids) == 0) || (kind == "requirements" && len(ids) != 0) {
		return storage.ErrInvalidReview
	}
	for _, id := range ids {
		if _, err := s.readEffectiveReviewDecision(ctx, id, "requirements"); err != nil {
			return err
		}
	}
	return nil
}

// Reaffirmation preserves an approval; a later rejection revokes that exact approval permanently.
func (s *Store) readEffectiveReviewDecision(ctx context.Context, id, kind string) ([]string, error) {
	if !validReviewID(id) {
		return nil, storage.ErrInvalidReview
	}
	var body []byte
	err := s.queryRow(ctx, `SELECT r.requirement_decision_ids FROM review_decisions d JOIN review_requests r ON r.project_slug=d.project_slug AND r.id=d.request_id
 WHERE d.project_slug=$1 AND d.id=$2::bigint AND r.kind=$3 AND d.verdict='accepted'
 AND NOT EXISTS(SELECT 1 FROM review_decisions later WHERE later.project_slug=d.project_slug AND later.request_id=d.request_id AND later.id>d.id AND later.verdict='rejected')`, s.project, id, kind).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrInvalidReview
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read effective review decision: %w", err)
	}
	var requirements []string
	if decodeErr := json.Unmarshal(body, &requirements); decodeErr != nil {
		return nil, fmt.Errorf("postgres: decode review prerequisites: %w", decodeErr)
	}
	return requirements, nil
}

var reviewRootPattern = regexp.MustCompile(`^(?:/|[a-zA-Z]:[/\\]|\\\\)`)

func (s *Store) validateReviewSource(ctx context.Context, source *storage.ReviewSource) error {
	switch source.Kind {
	case "specgraph":
		if source.SpecSlug == "" || source.Field == "" || source.ChangeID == "" || source.EnvironmentID != "" || source.RepositoryRoot != "" || source.CommitSHA != "" || source.Path != "" || source.Entry != "" {
			return storage.ErrInvalidReview
		}
		record, err := s.ReadWorkbenchKnowledgeRecord(ctx, "change", source.ChangeID)
		if err != nil {
			if errors.Is(err, ErrWorkbenchChangeNotFound) {
				return storage.ErrInvalidReview
			}
			return err
		}
		if record.Slug != source.SpecSlug {
			return storage.ErrInvalidReview
		}
		changeRecord := record.Record.(*storage.ChangeLogEntry) //nolint:errcheck // The fixed "change" read branch returns *storage.ChangeLogEntry.
		for _, change := range changeRecord.Changes {
			if change.Field == source.Field {
				return nil
			}
		}
		return storage.ErrInvalidReview
	case "git":
		if source.SpecSlug != "" || source.Field != "" || source.ChangeID != "" || strings.TrimSpace(source.EnvironmentID) == "" || !reviewRootPattern.MatchString(source.RepositoryRoot) || !reviewCommitPattern.MatchString(source.CommitSHA) ||
			source.Path == "" || strings.ContainsAny(source.Path, "\\:\x00") || strings.HasPrefix(source.Path, "/") || source.Path == "." || source.Path == ".." || strings.HasPrefix(source.Path, "../") || path.Clean(source.Path) != source.Path || strings.ContainsRune(source.RepositoryRoot, 0) {
			return storage.ErrInvalidReview
		}
		// Only contextual registration is checked here. The existing host Git reader owns fixed-commit content reads.
		var registered bool
		err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM run_bindings r LEFT JOIN context_packages p ON p.project_slug=r.project_slug AND p.id=r.package_id
   WHERE r.project_slug=$1 AND r.environment_id=$2 AND (r.workspace=$3 OR p.body->'dispatch_target'->>'projectWorkspaceRoot'=$3))`, s.project, source.EnvironmentID, source.RepositoryRoot).Scan(&registered)
		if err != nil {
			return fmt.Errorf("postgres: check review source registration: %w", err)
		}
		if !registered {
			return storage.ErrInvalidReview
		}
		return nil
	default:
		return storage.ErrInvalidReview
	}
}

func (s *Store) checkReviewAuthor(ctx context.Context, author storage.ReviewAuthor, reviewer string) error {
	var reviewerEnvironment, reviewerThread string
	err := s.queryRow(ctx, `SELECT environment_id,thread_ref FROM run_bindings WHERE project_slug=$1 AND id=$2 AND state='bound'`, s.project, reviewer).Scan(&reviewerEnvironment, &reviewerThread)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrReviewForbidden
	}
	if err != nil {
		return fmt.Errorf("postgres: read reviewer binding: %w", err)
	}
	if reviewerEnvironment == "" || reviewerThread == "" {
		return storage.ErrReviewForbidden
	}
	switch author.Kind {
	case "agent":
		if author.RunID == "" || author.UserID != "" {
			return storage.ErrInvalidReview
		}
		var environment, thread string
		err := s.queryRow(ctx, `SELECT environment_id,thread_ref FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, author.RunID).Scan(&environment, &thread)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrInvalidReview
		}
		if err != nil {
			return fmt.Errorf("postgres: read review author binding: %w", err)
		}
		if author.RunID == reviewer || (environment == reviewerEnvironment && thread == reviewerThread) {
			return storage.ErrReviewSelfReview
		}
		if environment == "" || thread == "" {
			return storage.ErrReviewForbidden
		}
	case "human":
		if author.UserID == "" || author.RunID != "" {
			return storage.ErrInvalidReview
		}
		user, err := s.ExistingAuth().GetUserByID(ctx, author.UserID)
		if err != nil {
			return err
		}
		if user.Kind != storage.KindHuman || user.DeletedAt != nil {
			return storage.ErrReviewForbidden
		}
	default:
		return storage.ErrInvalidReview
	}
	return nil
}

func validReviewID(id string) bool {
	value, err := strconv.ParseInt(id, 10, 64)
	return err == nil && value > 0 && strconv.FormatInt(value, 10) == id
}

func validReviewDecision(verdict, basis string) bool {
	return (verdict == "accepted" || verdict == "rejected") && strings.TrimSpace(basis) != "" && utf8.RuneCountInString(basis) <= 4000
}

// SubmitReview records the assigned Agent's source review opinion.
func (s *Store) SubmitReview(ctx context.Context, scope storage.MailScope, requestID, verdict, basis string) (*storage.SourceReviewResult, error) {
	return s.recordSourceReview(ctx, "", requestID, verdict, basis, &scope)
}

// ReviewSource is the human intervention path; Agent credentials never select it implicitly.
func (s *Store) ReviewSource(ctx context.Context, slug, requestID, verdict, basis string) (*storage.SourceReviewResult, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrReviewForbidden
	}
	user, err := s.ExistingAuth().GetUserByID(ctx, identity.UserID)
	if err != nil {
		return nil, err
	}
	if user.Kind != storage.KindHuman || user.DeletedAt != nil {
		return nil, storage.ErrReviewForbidden
	}
	return s.recordSourceReview(ctx, slug, requestID, verdict, basis, nil)
}

func (s *Store) recordSourceReview(ctx context.Context, expectedSlug, requestID, verdict, basis string, scope *storage.MailScope) (*storage.SourceReviewResult, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return nil, storage.ErrReviewForbidden
	}
	if !validReviewID(requestID) || !validReviewDecision(verdict, basis) {
		return nil, storage.ErrInvalidReview
	}
	result := &storage.SourceReviewResult{}
	var slug string
	var completionRunID *string
	var authoringTask string
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if lockErr := s.lockDependencyState(txCtx); lockErr != nil {
			return lockErr
		}
		var kind, assigned string
		var authorJSON, requirementJSON []byte
		var decided bool
		err := s.queryRow(txCtx, `SELECT task_slug,kind,reviewer_run_id,author_responsibility,requirement_decision_ids,decided_at IS NOT NULL,completion_run_id FROM review_requests WHERE project_slug=$1 AND id=$2::bigint FOR UPDATE`, s.project, requestID).Scan(&slug, &kind, &assigned, &authorJSON, &requirementJSON, &decided, &completionRunID)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrReviewRequestNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read source review request: %w", err)
		}
		if expectedSlug != "" && expectedSlug != slug {
			return storage.ErrReviewRequestNotFound
		}
		current, err := s.ReadReviewStatus(txCtx, slug)
		if err != nil {
			return err
		}
		var state storage.ReviewState
		for _, item := range current.Reviews {
			if item.Kind == kind {
				state = item
			}
		}
		var reviewer *string
		actorKind := "human"
		actor := identity.UserID
		if scope != nil {
			actorKind = "agent"
			actual, _, actorErr := s.reviewActorRun(txCtx, *scope)
			if actorErr != nil {
				return actorErr
			}
			if actual != assigned {
				return storage.ErrReviewForbidden
			}
			if decided {
				return storage.ErrReviewAlreadyDecided
			}
			if state.Request.ID != requestID {
				return storage.ErrReviewForbidden
			}
			if state.HumanHold {
				return storage.ErrReviewHumanHold
			}
			var author storage.ReviewAuthor
			if decodeErr := json.Unmarshal(authorJSON, &author); decodeErr != nil {
				return fmt.Errorf("postgres: decode review author: %w", decodeErr)
			}
			if authorErr := s.checkReviewAuthor(txCtx, author, actual); authorErr != nil {
				return authorErr
			}
			reviewer = &actual
			actor = actual
		}
		if kind == "design" && verdict == "accepted" {
			var ids []string
			if decodeErr := json.Unmarshal(requirementJSON, &ids); decodeErr != nil {
				return fmt.Errorf("postgres: decode design prerequisites: %w", decodeErr)
			}
			if requirementsErr := s.checkReviewRequirements(txCtx, kind, ids); requirementsErr != nil {
				return requirementsErr
			}
		}
		decision := storage.ReviewDecision{RequestID: requestID, Verdict: verdict, Basis: basis, ActorKind: actorKind, Actor: actor, ReviewerRunID: reviewer, CreatedAt: s.now()}
		err = s.queryRow(txCtx, `INSERT INTO review_decisions(project_slug,request_id,verdict,basis,actor_kind,actor,reviewer_run_id,created_at)
   VALUES($1,$2::bigint,$3,$4,$5,$6,$7,$8) RETURNING id::text`, s.project, requestID, verdict, basis, actorKind, actor, reviewer, decision.CreatedAt).Scan(&decision.ID)
		if err != nil {
			return fmt.Errorf("postgres: record source review decision: %w", err)
		}
		if _, err = s.exec(txCtx, `UPDATE review_requests SET decided_at=COALESCE(decided_at,$3) WHERE project_slug=$1 AND id=$2::bigint`, s.project, requestID, decision.CreatedAt); err != nil {
			return err
		}
		// Historical opinions never clear a newer source dispute or its human hold.
		if state.Request.ID == requestID {
			rounds, hold := reviewRoundsAfterDecision(actorKind, verdict, state.AgentRejections, state.MaxReviewRounds)
			if _, err = s.exec(txCtx, `UPDATE review_requests SET agent_rejections=$3,human_hold=$4 WHERE project_slug=$1 AND id=$2::bigint`, s.project, requestID, rounds, hold); err != nil {
				return err
			}
		}
		result.Recorded = true
		result.Decision = decision
		if completionRunID != nil {
			authoringTask, err = s.RunBindingTask(txCtx, *completionRunID)
			if err != nil {
				return err
			}
		}
		result.Status, err = s.ReadReviewStatus(txCtx, slug)
		return err
	})
	if err != nil {
		return nil, err
	}
	// Approval is durable before attempting the separate authoring close transition.
	if verdict == "accepted" && completionRunID != nil {
		completion := &storage.AuthoringCompletion{RunID: *completionRunID, TaskSlug: authoringTask, Status: "completed"}
		if completionErr := s.recordCompletion(ctx, authoringTask, *completionRunID, requestID); completionErr != nil {
			completion.Status = "failed"
			completion.Message = "Approval recorded; authoring completion failed"
			switch {
			case errors.Is(completionErr, storage.ErrReviewHumanHold):
				completion.Message = "Approval recorded; authoring completion is waiting for human intervention"
			case errors.Is(completionErr, storage.ErrCompletionRequiresRequirementReview):
				completion.Message = "Approval recorded; a current output source or unsubmitted input differs from the approved submission or prepared contract"
			case errors.Is(completionErr, storage.ErrExecutionDependenciesChanged), errors.Is(completionErr, storage.ErrDependenciesNotReady):
				completion.Message = "Approval recorded; the authoring task dependencies changed after preparation or are not ready"
			case errors.Is(completionErr, storage.ErrRunBindingConflict), errors.Is(completionErr, storage.ErrRunBindingNotFound):
				completion.Message = "Approval recorded; authoring responsibility or its current submission has changed"
			case errors.Is(completionErr, storage.ErrInvalidReview), errors.Is(completionErr, storage.ErrReviewForbidden):
				completion.Message = "Approval recorded; the current authoring submission or its prerequisite approvals are not eligible for completion"
			case errors.Is(completionErr, storage.ErrSpecNotApproved), errors.Is(completionErr, storage.ErrSummaryNotExecutable):
				completion.Message = "Approval recorded; the authoring node state no longer permits this completion"
			}
		}
		result.AuthoringCompletion = completion
	}
	return result, nil
}

// ReviewDelivery preserves the original manual opinion receipt, independently of source reviews.
func (s *Store) ReviewDelivery(ctx context.Context, deliveryID, fingerprint, verdict, basis string) (*storage.ReviewResult, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrReviewForbidden
	}
	if !validReviewDecision(verdict, basis) {
		return nil, storage.ErrInvalidReview
	}
	conditions, err := json.Marshal(map[string]string{"review": basis})
	if err != nil {
		return nil, fmt.Errorf("postgres: encode delivery review conditions: %w", err)
	}
	id, err := s.insertAcceptance(ctx, deliveryID, fingerprint, verdict, identity.UserID, conditions, "human", nil, nil)
	if err != nil {
		return nil, err
	}
	return &storage.ReviewResult{Recorded: true, AcceptanceID: id, DeliveryID: deliveryID, Verdict: verdict, Completion: storage.ReviewCompletion{Status: "not_requested"}}, nil
}
