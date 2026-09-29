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

// ScopedExisting selects an existing project without creating or modifying it.
func (s *Store) ScopedExisting(ctx context.Context, project string) (*Store, error) {
	if _, err := s.GetProject(ctx, project); err != nil {
		return nil, err
	}
	return &Store{pool: s.pool, nowFunc: s.nowFunc, project: project, shared: s.shared}, nil
}

// WorkbenchRun is run metadata, not an assertion that its thread is executing.
type WorkbenchRun struct {
	ID                string                  `json:"id"`
	TaskSlug          string                  `json:"taskSlug" db:"task_spec_slug"`
	PackageID         string                  `json:"packageId" db:"package_id"`
	SpecVersion       *int32                  `json:"specVersion" db:"spec_version"`
	Generation        int64                   `json:"generation"`
	ThreadRef         string                  `json:"threadRef" db:"thread_ref"`
	EnvironmentID     string                  `json:"environmentId,omitempty" db:"environment_id"`
	ExecutorKind      string                  `json:"executorKind" db:"executor_kind"`
	NativeProjectID   *string                 `json:"nativeProjectId,omitempty" db:"native_project_id"`
	DispatchMessageID *string                 `json:"dispatchMessageId,omitempty" db:"dispatch_message_id"`
	AssignmentRole    *string                 `json:"assignmentRole,omitempty" db:"assignment_role"`
	WorkPurpose       *string                 `json:"workPurpose,omitempty" db:"work_purpose"`
	GitBaseline       json.RawMessage         `json:"gitBaseline,omitempty" db:"git_baseline"`
	Workspace         string                  `json:"workspace"`
	State             string                  `json:"state"`
	CreatedAt         time.Time               `json:"createdAt" db:"created_at"`
	UpdatedAt         time.Time               `json:"updatedAt" db:"updated_at"`
	ProgramLoop       *WorkbenchProgramLoop   `json:"programLoop,omitempty" db:"-"`
	CandidateLoop     *WorkbenchCandidateLoop `json:"candidateLoop,omitempty" db:"-"`
}

// WorkbenchCandidateLoop omits plans, reports and attempt history.
type WorkbenchCandidateLoop struct {
	Status         string  `json:"status"`
	MaxAttempts    int     `json:"maxAttempts"`
	AttemptID      *string `json:"attemptId"`
	AttemptOrdinal *int    `json:"attemptOrdinal"`
	DeliveryID     *string `json:"deliveryId"`
	ConfiguredBy   string  `json:"configuredBy"`
}

type WorkbenchNodeOwner struct {
	TaskSlug           string  `json:"taskSlug" db:"task_slug"`
	HumanOwnerUserID   *string `json:"humanOwnerUserId" db:"human_owner_user_id"`
	PendingOperationID *string `json:"pendingOperationId" db:"pending_operation_id"`
}

// WorkbenchProgramLoop omits command, context and judgment bodies.
type WorkbenchProgramLoop struct {
	Kind               string                     `json:"kind"`
	MaxAttempts        *int                       `json:"maxAttempts"`
	Status             string                     `json:"status"`
	AttemptID          *string                    `json:"attemptId"`
	AttemptOrdinal     *int                       `json:"attemptOrdinal"`
	AuthorizedByUserID string                     `json:"authorizedByUserId"`
	Completion         *storage.ProgramCompletion `json:"completion,omitempty"`
}

// WorkbenchDelivery omits the potentially large delivery snapshot.
type WorkbenchDelivery struct {
	ID           string    `json:"id"`
	RunBindingID string    `json:"runBindingId" db:"run_binding_id"`
	SubmittedBy  string    `json:"submittedBy" db:"submitted_by"`
	SubmittedAt  time.Time `json:"submittedAt" db:"submitted_at"`
}

// WorkbenchNodeDeliveries pages recorded delivery identities without their snapshots.
type WorkbenchNodeDeliveries struct {
	TaskSlug   string              `json:"taskSlug"`
	Deliveries []WorkbenchDelivery `json:"deliveries"`
	HasMore    bool                `json:"hasMore"`
	NextCursor *string             `json:"nextCursor"`
}

// WorkbenchConversationRun is the original run identity and dispatch message, not a conversation-message match.
type WorkbenchConversationRun struct {
	RunID             string    `json:"runId" db:"run_id"`
	TaskSlug          string    `json:"taskSlug" db:"task_slug"`
	State             string    `json:"state"`
	CreatedAt         time.Time `json:"createdAt" db:"created_at"`
	DispatchMessageID *string   `json:"dispatchMessageId" db:"dispatch_message_id"`
}

// WorkbenchConversationRuns is a page of project- and native-conversation-scoped runs.
type WorkbenchConversationRuns struct {
	ThreadID   string                     `json:"threadId"`
	Runs       []WorkbenchConversationRun `json:"runs"`
	HasMore    bool                       `json:"hasMore"`
	NextCursor *string                    `json:"nextCursor"`
}

// ReadConversationRuns pages only agent runs bound to this native conversation and project.
func (s *Store) ReadConversationRuns(ctx context.Context, environmentID, nativeProjectID, threadID, cursor string) (*WorkbenchConversationRuns, error) {
	page := &WorkbenchConversationRuns{ThreadID: threadID, Runs: []WorkbenchConversationRun{}}
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var before *time.Time
		if cursor != "" {
			var timestamp time.Time
			err := s.queryRow(snapshotCtx, `SELECT rb.created_at FROM run_bindings rb
			 JOIN context_packages cp ON cp.project_slug=rb.project_slug AND cp.id=rb.package_id
			 WHERE rb.project_slug=$1 AND rb.environment_id=$2 AND rb.thread_ref=$3
			 AND cp.body->'dispatch_target'->>'projectId'=$4 AND rb.executor_kind='agent' AND rb.id=$5`,
				s.project, environmentID, threadID, nativeProjectID, cursor).Scan(&timestamp)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrInvalidConversationRunCursor
			}
			if err != nil {
				return fmt.Errorf("postgres: ReadConversationRuns: %w", err)
			}
			before = &timestamp
		}
		rows, err := s.query(snapshotCtx, `SELECT rb.id AS run_id,rb.task_spec_slug AS task_slug,rb.state,rb.created_at,
		 cp.body->'dispatch_target'->>'messageId' AS dispatch_message_id
		 FROM run_bindings rb JOIN context_packages cp ON cp.project_slug=rb.project_slug AND cp.id=rb.package_id
		 WHERE rb.project_slug=$1 AND rb.environment_id=$2 AND rb.thread_ref=$3
		 AND cp.body->'dispatch_target'->>'projectId'=$4 AND rb.executor_kind='agent'
		 AND ($5::timestamptz IS NULL OR (rb.created_at,rb.id)<($5,$6::text))
		 ORDER BY rb.created_at DESC,rb.id DESC LIMIT 51`, s.project, environmentID, threadID, nativeProjectID, before, cursor)
		if err != nil {
			return fmt.Errorf("postgres: ReadConversationRuns: %w", err)
		}
		items, err := pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchConversationRun])
		if err != nil {
			return fmt.Errorf("postgres: ReadConversationRuns: %w", err)
		}
		page.HasMore = len(items) > 50
		if page.HasMore {
			items = items[:50]
			last := items[49].RunID
			page.NextCursor = &last
		}
		page.Runs = items
		return nil
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}

// ReadNodeDeliveries discovers recorded delivery IDs without duplicating their snapshots.
func (s *Store) ReadNodeDeliveries(ctx context.Context, slug, cursor string) (*WorkbenchNodeDeliveries, error) {
	page := &WorkbenchNodeDeliveries{TaskSlug: slug, Deliveries: []WorkbenchDelivery{}}
	err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
		var exists bool
		if err := s.queryRow(snapshotCtx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, slug).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: ReadNodeDeliveries: %w", err)
		}
		if !exists {
			return storage.ErrSpecNotFound
		}
		var before *time.Time
		if cursor != "" {
			var timestamp time.Time
			err := s.queryRow(snapshotCtx, `SELECT d.submitted_at FROM deliveries d
			 JOIN run_bindings rb ON rb.project_slug=d.project_slug AND rb.id=d.run_binding_id
			 WHERE d.project_slug=$1 AND rb.task_spec_slug=$2 AND d.id=$3`, s.project, slug, cursor).Scan(&timestamp)
			if errors.Is(err, pgx.ErrNoRows) {
				return storage.ErrInvalidDeliveryCursor
			}
			if err != nil {
				return fmt.Errorf("postgres: ReadNodeDeliveries: %w", err)
			}
			before = &timestamp
		}
		rows, err := s.query(snapshotCtx, `SELECT d.id,d.run_binding_id,d.submitted_by,d.submitted_at FROM deliveries d
		 JOIN run_bindings rb ON rb.project_slug=d.project_slug AND rb.id=d.run_binding_id
		 WHERE d.project_slug=$1 AND rb.task_spec_slug=$2 AND ($3::timestamptz IS NULL OR (d.submitted_at,d.id)<($3,$4::text))
		 ORDER BY d.submitted_at DESC,d.id DESC LIMIT 51`, s.project, slug, before, cursor)
		if err != nil {
			return err
		}
		items, err := pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchDelivery])
		if err != nil {
			return fmt.Errorf("postgres: ReadNodeDeliveries: %w", err)
		}
		page.HasMore = len(items) > 50
		if page.HasMore {
			items = items[:50]
			last := items[49].ID
			page.NextCursor = &last
		}
		page.Deliveries = items
		return nil
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}

// WorkbenchDeliveryDetail includes the submitted snapshot for explicit review.
type WorkbenchDeliveryDetail struct {
	WorkbenchDelivery
	Snapshot json.RawMessage `json:"snapshot"`
}

// ReadWorkbenchDelivery reads one delivery without expanding aggregate views.
func (s *Store) ReadWorkbenchDelivery(ctx context.Context, id string) (*WorkbenchDeliveryDetail, error) {
	var result WorkbenchDeliveryDetail
	err := s.queryRow(ctx, `SELECT d.id, d.run_binding_id, d.submitted_by, d.submitted_at, d.snapshot
		FROM deliveries d JOIN run_bindings rb ON rb.id=d.run_binding_id AND rb.project_slug=d.project_slug
		WHERE d.project_slug=$1 AND d.id=$2`, s.project, id).
		Scan(&result.ID, &result.RunBindingID, &result.SubmittedBy, &result.SubmittedAt, &result.Snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.ErrDeliveryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench delivery: %w", err)
	}
	return &result, nil
}

// WorkbenchEvidence preserves NULL exit codes rather than implying success.
type WorkbenchEvidence struct {
	ID         string    `json:"id"`
	DeliveryID string    `json:"deliveryId" db:"delivery_id"`
	Kind       string    `json:"kind"`
	Command    string    `json:"command"`
	ExitCode   *int64    `json:"exitCode" db:"exit_code"`
	Verifier   string    `json:"verifier"`
	CreatedAt  time.Time `json:"createdAt" db:"created_at"`
}

// WorkbenchAcceptance records the stored verdict without granting authority.
type WorkbenchAcceptance struct {
	ID                      string          `json:"id"`
	DeliveryID              string          `json:"deliveryId" db:"delivery_id"`
	Verdict                 string          `json:"verdict"`
	Approver                string          `json:"approver"`
	CreatedAt               time.Time       `json:"createdAt" db:"created_at"`
	RequirementsFingerprint string          `json:"requirementsFingerprint" db:"requirements_fingerprint"`
	Conditions              json.RawMessage `json:"conditions"`
	ActorKind               string          `json:"actorKind" db:"actor_kind"`
	ReviewerRunID           *string         `json:"reviewerRunId" db:"reviewer_run_id"`
	ReviewRequestID         *string         `json:"reviewRequestId" db:"review_request_id"`
}

// WorkbenchDeliveryHook links existing task identities without copying execution context.
type WorkbenchDeliveryHook struct {
	storage.DeliveryTestHook
	SourceTaskSlug string `json:"sourceTaskSlug"`
	TargetTaskSlug string `json:"targetTaskSlug"`
}

// WorkbenchMetadata contains project-scoped execution, review and judgment metadata.
type WorkbenchMetadata struct {
	NodeOwners      []WorkbenchNodeOwner     `json:"nodeOwners"`
	Runs            []WorkbenchRun           `json:"runs"`
	Deliveries      []WorkbenchDelivery      `json:"deliveries"`
	Evidence        []WorkbenchEvidence      `json:"evidence"`
	Acceptances     []WorkbenchAcceptance    `json:"acceptances"`
	NodeMarks       []NodeMark               `json:"nodeMarks"`
	ReviewStates    []storage.ReviewStatus   `json:"reviewStates"`
	NodeEventCounts []storage.NodeEventCount `json:"nodeEventCounts"`
	DeliveryHooks   []WorkbenchDeliveryHook  `json:"deliveryHooks"`
}

// ReadWorkbenchMetadata performs bounded-in-query-count reads, never one
// per task. Related rows must belong to the same project at every join.
func (s *Store) ReadWorkbenchMetadata(ctx context.Context) (*WorkbenchMetadata, error) {
	out := &WorkbenchMetadata{}
	ownerRows, err := s.query(ctx, `SELECT s.slug AS task_slug,s.human_owner_user_id,o.id AS pending_operation_id
 FROM specs s LEFT JOIN node_ownership_operations o ON o.project_slug=s.project_slug AND o.task_slug=s.slug AND o.status='pending'
 WHERE s.project_slug=$1 AND s.role='work' ORDER BY s.slug`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench node owners: %w", err)
	}
	out.NodeOwners, err = pgx.CollectRows(ownerRows, pgx.RowToStructByName[WorkbenchNodeOwner])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench node owners: %w", err)
	}
	rows, err := s.query(ctx, `SELECT rb.id, rb.task_spec_slug, rb.package_id, rb.generation,
		rb.thread_ref, rb.environment_id, rb.executor_kind, rb.workspace, rb.state, rb.created_at, rb.updated_at,
		CASE WHEN rb.executor_kind='program' THEN cp.body->'program_target'->>'nativeProjectId' ELSE cp.body->'dispatch_target'->>'projectId' END AS native_project_id,
		(cp.body->>'spec_version')::integer AS spec_version,
		cp.body->'dispatch_target'->>'messageId' AS dispatch_message_id,
		cp.body->'dispatch_target'->>'assignmentRole' AS assignment_role,
		CASE WHEN rb.executor_kind='program' THEN cp.body->'program_target'->>'workPurpose' ELSE cp.body->'dispatch_target'->>'workPurpose' END AS work_purpose,
		cp.body->'dispatch_target'->'gitBaseline' AS git_baseline
		FROM run_bindings rb
		LEFT JOIN context_packages cp ON cp.id = rb.package_id AND cp.project_slug = rb.project_slug
		WHERE rb.project_slug = $1 ORDER BY rb.created_at, rb.id`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench runs: %w", err)
	}
	out.Runs, err = pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchRun])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench runs: %w", err)
	}
	rows, err = s.query(ctx, `SELECT d.id, d.run_binding_id, d.submitted_by, d.submitted_at
		FROM deliveries d
		JOIN run_bindings rb ON rb.id = d.run_binding_id AND rb.project_slug = d.project_slug
		WHERE d.project_slug = $1 ORDER BY d.submitted_at, d.id`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench deliveries: %w", err)
	}
	out.Deliveries, err = pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchDelivery])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench deliveries: %w", err)
	}
	rows, err = s.query(ctx, `SELECT e.id, e.delivery_id, e.kind, e.command, e.exit_code, e.verifier, e.created_at
		FROM evidence e
		JOIN deliveries d ON d.id = e.delivery_id AND d.project_slug = e.project_slug
		JOIN run_bindings rb ON rb.id = d.run_binding_id AND rb.project_slug = d.project_slug
		WHERE e.project_slug = $1 ORDER BY e.created_at, e.id`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench evidence: %w", err)
	}
	out.Evidence, err = pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchEvidence])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench evidence: %w", err)
	}
	rows, err = s.query(ctx, `SELECT a.id, a.delivery_id, a.verdict, a.approver, a.created_at, a.requirements_fingerprint, a.conditions,a.actor_kind,a.reviewer_run_id,a.review_request_id::text
		FROM acceptances a
		JOIN deliveries d ON d.id = a.delivery_id AND d.project_slug = a.project_slug
		JOIN run_bindings rb ON rb.id = d.run_binding_id AND rb.project_slug = d.project_slug
		WHERE a.project_slug = $1 ORDER BY a.created_at, a.id`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench acceptances: %w", err)
	}
	out.Acceptances, err = pgx.CollectRows(rows, pgx.RowToStructByName[WorkbenchAcceptance])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench acceptances: %w", err)
	}
	rows, err = s.query(ctx, `SELECT DISTINCT ON (task_slug,kind) `+nodeMarkColumns+`
		FROM node_marks WHERE project_slug = $1 ORDER BY task_slug,kind,node_marks.id DESC`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench node marks: %w", err)
	}
	out.NodeMarks, err = pgx.CollectRows(rows, pgx.RowToStructByName[NodeMark])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench node marks: %w", err)
	}
	out.ReviewStates, err = s.readReviewStatuses(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench review states: %w", err)
	}
	out.NodeEventCounts, err = s.readNodeEventCounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench node event counts: %w", err)
	}
	rows, err = s.query(ctx, `SELECT `+deliveryHookJSON+` || jsonb_build_object('sourceTaskSlug',hook_source_task_slug,'targetTaskSlug',target_task_slug)
		FROM (SELECT h.*,source.task_spec_slug AS hook_source_task_slug,target.task_spec_slug AS target_task_slug
		 FROM delivery_test_hooks h
		 JOIN run_bindings source ON source.project_slug=h.project_slug AND source.id=h.source_run_id
		 JOIN run_bindings target ON target.project_slug=h.project_slug AND target.id=h.target_run_id
		 WHERE h.hook_kind='delivery_test' AND h.project_slug = $1) hooks ORDER BY created_at,id`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench delivery hooks: %w", err)
	}
	encodedHooks, err := pgx.CollectRows(rows, pgx.RowTo[json.RawMessage])
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench delivery hooks: %w", err)
	}
	out.DeliveryHooks = make([]WorkbenchDeliveryHook, 0, len(encodedHooks))
	for _, encoded := range encodedHooks {
		var hook WorkbenchDeliveryHook
		if decodeErr := json.Unmarshal(encoded, &hook); decodeErr != nil {
			return nil, fmt.Errorf("postgres: workbench delivery hook projection: %w", decodeErr)
		}
		out.DeliveryHooks = append(out.DeliveryHooks, hook)
	}
	rows, err = s.query(ctx, `SELECT rb.id,
		jsonb_build_object('kind',cp.body->'program_target'->'loop'->'kind',
		 'maxAttempts',cp.body->'program_target'->'loop'->'maxAttempts',
		 'termination',jsonb_build_object('kind',cp.body->'program_target'->'loop'->'termination'->'kind',
		  'exitCode',cp.body->'program_target'->'loop'->'termination'->'exitCode')),
		cp.body->'program_target'->>'authorizedByUserId',a.id,a.ordinal,
		CASE WHEN a.result IS NOT NULL THEN jsonb_build_object('outcome',a.result->'outcome',
		 'exitCode',a.result->'exitCode','finishedAt',a.result->'finishedAt',
		 'timedOutAt',a.result->'timedOutAt','cancelRequestedAt',a.result->'cancelRequestedAt') END,
		stop.id,judgment.id,judgment.value,completion.id,completion.program_completion_basis
		FROM run_bindings rb
		JOIN context_packages cp ON cp.id=rb.package_id AND cp.project_slug=rb.project_slug
		LEFT JOIN LATERAL (SELECT id,ordinal,result FROM program_attempts WHERE project_slug=rb.project_slug AND run_id=rb.id ORDER BY ordinal DESC LIMIT 1) a ON true
		LEFT JOIN LATERAL (SELECT id FROM program_loop_events WHERE project_slug=rb.project_slug AND run_id=rb.id AND kind='stop' ORDER BY sequence LIMIT 1) stop ON true
		LEFT JOIN LATERAL (SELECT id,value FROM program_loop_events WHERE project_slug=rb.project_slug AND run_id=rb.id AND attempt_id=a.id AND kind='condition' ORDER BY sequence DESC LIMIT 1) judgment ON true
		LEFT JOIN LATERAL (SELECT id,program_completion_basis FROM execution_events WHERE project_slug=rb.project_slug AND spec_slug=rb.task_spec_slug AND agent=rb.id AND event_type='completion' ORDER BY created_at DESC,id DESC LIMIT 1) completion ON true
		WHERE rb.project_slug = $1 AND rb.executor_kind='program' AND cp.body->'program_target'->'loop' IS NOT NULL
		AND cp.body->'program_target'->'loop'<>'null'::jsonb`, s.project)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench program loops: %w", err)
	}
	loops := make(map[string]*WorkbenchProgramLoop)
	for rows.Next() {
		var runID, authorizedBy string
		var policyBody, resultBody []byte
		var attemptID, stopID, judgmentID, judgmentValue, completionID *string
		var completionBasis []byte
		var ordinal *int
		if scanErr := rows.Scan(&runID, &policyBody, &authorizedBy, &attemptID, &ordinal, &resultBody, &stopID, &judgmentID, &judgmentValue, &completionID, &completionBasis); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: workbench program loop facts: %w", scanErr)
		}
		var policy storage.ProgramLoopConfig
		var result *storage.ProgramRunResult
		if decodeErr := json.Unmarshal(policyBody, &policy); decodeErr != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: workbench program loop policy: %w", decodeErr)
		}
		if len(resultBody) > 0 {
			if decodeErr := json.Unmarshal(resultBody, &result); decodeErr != nil {
				rows.Close()
				return nil, fmt.Errorf("postgres: workbench program loop result: %w", decodeErr)
			}
		}
		var attempt *storage.ProgramAttemptMetadata
		var stop, judgment *storage.ProgramLoopEvent
		if attemptID != nil {
			attempt = &storage.ProgramAttemptMetadata{ID: *attemptID, Ordinal: *ordinal}
		}
		if stopID != nil {
			stop = &storage.ProgramLoopEvent{ID: *stopID}
		}
		if judgmentID != nil {
			judgment = &storage.ProgramLoopEvent{ID: *judgmentID, Value: *judgmentValue}
		}
		var completion *storage.ProgramCompletion
		if completionID != nil {
			completion = &storage.ProgramCompletion{EventID: *completionID}
			if len(completionBasis) != 0 {
				if err := json.Unmarshal(completionBasis, completion); err != nil {
					rows.Close()
					return nil, fmt.Errorf("postgres: workbench program completion basis: %w", err)
				}
			}
		}
		state := deriveProgramLoopState(&policy, attempt, result, stop, judgment, completion)
		loops[runID] = &WorkbenchProgramLoop{Kind: policy.Kind, MaxAttempts: policy.MaxAttempts, Status: state.Status, AttemptID: attemptID, AttemptOrdinal: ordinal, AuthorizedByUserID: authorizedBy, Completion: completion}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench program loops: %w", err)
	}
	for i := range out.Runs {
		out.Runs[i].ProgramLoop = loops[out.Runs[i].ID]
	}
	if len(out.Runs) == 0 {
		return out, nil
	}
	runIDs := make([]string, len(out.Runs))
	for i := range out.Runs {
		runIDs[i] = out.Runs[i].ID
	}
	rows, err = s.query(ctx, `SELECT l.run_id,l.max_attempts,l.configured_by,l.stopped_at IS NOT NULL,l.abandoned_at IS NOT NULL,l.resolved_at IS NOT NULL,
 l.termination_kind,a.id,a.ordinal,a.delivery_id,d.snapshot->'git'->'head'->>'isRepo',d.snapshot->'git'->'head'->>'commitSha',e.status,
 j.id,j.value,i.id,i.resolved_at IS NOT NULL
 FROM candidate_loops l
 LEFT JOIN LATERAL (SELECT id,ordinal,delivery_id FROM candidate_attempts WHERE project_slug=l.project_slug AND run_id=l.run_id ORDER BY ordinal DESC LIMIT 1) a ON true
 LEFT JOIN deliveries d ON d.project_slug=l.project_slug AND d.id=a.delivery_id
 CROSS JOIN LATERAL jsonb_array_elements(l.plan_sources) plan(value)
 LEFT JOIN LATERAL (SELECT result->>'status' AS status FROM evidence WHERE project_slug=l.project_slug AND delivery_id=a.delivery_id
 AND kind='test_report' AND result->>'commitSha'=d.snapshot->'git'->'head'->>'commitSha'
 AND result->'planSources' @> jsonb_build_array(plan.value) ORDER BY record_order DESC LIMIT 1) e ON true
 LEFT JOIN LATERAL (SELECT id,value FROM candidate_satisfaction_judgments WHERE project_slug=l.project_slug AND attempt_id=a.id ORDER BY event_order DESC LIMIT 1) j ON true
 LEFT JOIN LATERAL (SELECT id,resolved_at FROM candidate_interventions WHERE project_slug=l.project_slug AND attempt_id=a.id ORDER BY event_order DESC LIMIT 1) i ON true
 WHERE l.project_slug=$1 AND l.run_id=ANY($2::text[]) ORDER BY l.run_id`, s.project, runIDs)
	if err != nil {
		return nil, fmt.Errorf("postgres: workbench candidate loops: %w", err)
	}
	type candidateSummary struct {
		view                          *WorkbenchCandidateLoop
		stopped, abandoned, completed bool
		statuses                      []string
		satisfaction                  *storage.CandidateSatisfactionState
	}
	candidates := make(map[string]*candidateSummary)
	for rows.Next() {
		var runID, configuredBy, terminationKind string
		var maxAttempts int
		var stopped, abandoned, completed bool
		var attemptID, deliveryID, isRepo, commit, reportStatus, judgmentID, judgmentValue, interventionID *string
		var interventionResolved *bool
		var ordinal *int
		if err := rows.Scan(&runID, &maxAttempts, &configuredBy, &stopped, &abandoned, &completed, &terminationKind,
			&attemptID, &ordinal, &deliveryID, &isRepo, &commit, &reportStatus, &judgmentID, &judgmentValue, &interventionID, &interventionResolved); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan workbench candidate loop: %w", err)
		}
		candidate := candidates[runID]
		if candidate == nil {
			candidate = &candidateSummary{view: &WorkbenchCandidateLoop{MaxAttempts: maxAttempts, AttemptID: attemptID,
				AttemptOrdinal: ordinal, DeliveryID: deliveryID, ConfiguredBy: configuredBy}, stopped: stopped, abandoned: abandoned, completed: completed}
			if terminationKind == "satisfaction" {
				candidate.satisfaction = &storage.CandidateSatisfactionState{Value: "unknown"}
				if judgmentID != nil {
					candidate.satisfaction.Value = *judgmentValue
					candidate.satisfaction.Judgment = &storage.CandidateSatisfactionJudgment{ID: *judgmentID}
				}
				if interventionID != nil {
					intervention := &storage.CandidateIntervention{ID: *interventionID}
					if interventionResolved != nil && *interventionResolved {
						resolved := time.Time{}
						intervention.ResolvedAt = &resolved
					}
					candidate.satisfaction.Intervention = intervention
				}
			}
			candidates[runID] = candidate
		}
		if deliveryID != nil && !abandoned {
			if isRepo == nil || *isRepo != "true" || commit == nil || !reviewCommitPattern.MatchString(*commit) {
				rows.Close()
				return nil, storage.ErrInvalidTestReport
			}
			if reportStatus == nil {
				candidate.statuses = append(candidate.statuses, "")
			} else {
				candidate.statuses = append(candidate.statuses, *reportStatus)
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("postgres: collect workbench candidate loops: %w", err)
	}
	rows.Close()
	for i := range out.Runs {
		candidate := candidates[out.Runs[i].ID]
		if candidate == nil {
			continue
		}
		ordinal := 0
		if candidate.view.AttemptOrdinal != nil {
			ordinal = *candidate.view.AttemptOrdinal
		}
		var condition *storage.CandidateCondition
		if candidate.view.DeliveryID != nil && !candidate.abandoned {
			value, reason := candidateReportValue(candidate.statuses)
			condition = &storage.CandidateCondition{Value: value, Reason: reason}
		}
		candidate.view.Status = candidateLoopStatus(candidate.view.MaxAttempts, ordinal, candidate.view.DeliveryID != nil,
			condition, candidate.satisfaction, candidate.stopped, candidate.abandoned, candidate.completed)
		out.Runs[i].CandidateLoop = candidate.view
	}
	return out, nil
}
