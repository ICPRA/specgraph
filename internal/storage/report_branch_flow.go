// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// ReportBranchCondition fixes the delivery, plan and independently referenced inputs for one predicate.
type ReportBranchCondition struct {
	Key        string                     `json:"key"`
	Kind       string                     `json:"kind"`
	Report     *ReportBranchReportInput   `json:"report,omitempty"`
	Judgment   *ReportBranchJudgmentInput `json:"judgment,omitempty"`
	Evaluation *ReportBranchEvaluation    `json:"evaluation,omitempty"`
}

// ReportBranchReportInput fixes one registered-report predicate.
type ReportBranchReportInput struct {
	DeliveryID   string         `json:"deliveryId"`
	PlanSource   ReviewSource   `json:"planSource"`
	InputSources []ReviewSource `json:"inputSources"`
}

// ReportBranchJudgmentInput fixes the question and source positions, not its eventual value.
type ReportBranchJudgmentInput struct {
	Criterion    string         `json:"criterion"`
	InputSources []ReviewSource `json:"inputSources"`
	DeliveryID   *string        `json:"deliveryId,omitempty"`
}

// ArmReportBranchCondition contains exactly one discriminated predicate payload.
type ArmReportBranchCondition struct {
	Key      string                     `json:"key"`
	Kind     string                     `json:"kind"`
	Report   *ReportBranchReportInput   `json:"report,omitempty"`
	Judgment *ReportBranchJudgmentInput `json:"judgment,omitempty"`
}

// ReportBranchJudgment is one immutable named evaluation, never an architectural Decision.
type ReportBranchJudgment struct {
	ID            string         `json:"id"`
	FlowID        string         `json:"flowId"`
	ConditionKey  string         `json:"conditionKey"`
	Value         string         `json:"value"`
	InputRefs     []ReviewSource `json:"inputRefs"`
	Reason        string         `json:"reason"`
	ActorUserID   string         `json:"actorUserId"`
	ActorRunID    *string        `json:"actorRunId,omitempty"`
	RecordedAt    time.Time      `json:"recordedAt"`
	PredecessorID *string        `json:"predecessorId,omitempty"`
}

// RecordReportBranchJudgmentRequest names the exact predecessor and source revisions.
type RecordReportBranchJudgmentRequest struct {
	FlowID             string         `json:"flowId"`
	ConditionKey       string         `json:"conditionKey"`
	ExpectedJudgmentID *string        `json:"expectedJudgmentId"`
	Value              string         `json:"value"`
	InputRefs          []ReviewSource `json:"inputRefs"`
	Reason             string         `json:"reason"`
}

// ReportBranchJudgmentResult distinguishes a new record from its exact idempotent replay.
type ReportBranchJudgmentResult struct {
	Judgment ReportBranchJudgment `json:"judgment"`
	Replayed bool                 `json:"replayed"`
}

// ReportBranchJudgmentPage pages immutable history newest first.
type ReportBranchJudgmentPage struct {
	FlowID       string                 `json:"flowId"`
	ConditionKey string                 `json:"conditionKey"`
	Judgments    []ReportBranchJudgment `json:"judgments"`
	HasMore      bool                   `json:"hasMore"`
	NextCursor   *string                `json:"nextCursor,omitempty"`
}

// ReportBranchReport is a recorded assertion reference, not proof that tests were rerun.
type ReportBranchReport struct {
	ID          string         `json:"id"`
	DeliveryID  string         `json:"deliveryId"`
	CommitSHA   string         `json:"commitSha"`
	Status      string         `json:"status"`
	PlanSources []ReviewSource `json:"planSources"`
	CreatedAt   time.Time      `json:"createdAt"`
}

// ReportBranchEvaluation is the current predicate projection; unknown is not false.
type ReportBranchEvaluation struct {
	Value    string                `json:"value"`
	Report   *ReportBranchReport   `json:"report,omitempty"`
	Judgment *ReportBranchJudgment `json:"judgment,omitempty"`
	Reason   string                `json:"reason,omitempty"`
}

// ReportBranchProof preserves the exact fact consumed by the first admission.
type ReportBranchProof struct {
	FlowID       string                `json:"flowId"`
	ConditionKey string                `json:"conditionKey"`
	Value        string                `json:"value"`
	Report       *ReportBranchReport   `json:"report,omitempty"`
	Judgment     *ReportBranchJudgment `json:"judgment,omitempty"`
}

// ReportBranchAdmission identifies the original run authorization and its immutable basis.
type ReportBranchAdmission struct {
	ID    string            `json:"id"`
	Proof ReportBranchProof `json:"proof"`
}

// ReportBranchMember binds one original prepared run and package to a named result.
type ReportBranchMember struct {
	RunID        string                 `json:"runId"`
	PackageID    string                 `json:"packageId,omitempty"`
	ConditionKey string                 `json:"conditionKey"`
	When         string                 `json:"when"`
	Eligibility  string                 `json:"eligibility,omitempty"`
	Admission    *ReportBranchAdmission `json:"admission,omitempty"`
}

// ArmReportBranchMember selects an original run; storage resolves its package.
type ArmReportBranchMember struct {
	RunID        string `json:"runId"`
	ConditionKey string `json:"conditionKey"`
	When         string `json:"when"`
}

// ArmReportBranchFlowRequest creates one fixed, idempotent occurrence, not an execution.
type ArmReportBranchFlowRequest struct {
	IdempotencyKey     string                     `json:"idempotencyKey"`
	Conditions         []ArmReportBranchCondition `json:"conditions"`
	Branches           []ArmReportBranchMember    `json:"branches"`
	CandidateAttemptID string                     `json:"candidateAttemptId,omitempty"`
}

// ArmReportFlowJoinRequest binds one original target run to this occurrence's all/any result.
type ArmReportFlowJoinRequest struct {
	FlowID         string `json:"flowId"`
	RunID          string `json:"runId"`
	Mode           string `json:"mode"`
	AllowEmptySkip bool   `json:"allowEmptySkip"`
}

// ReportFlowJoinConfig is the discoverable fixed target, without recursive evaluation.
type ReportFlowJoinConfig struct {
	FlowID         string    `json:"flowId"`
	RunID          string    `json:"runId"`
	PackageID      string    `json:"packageId"`
	Mode           string    `json:"mode"`
	AllowEmptySkip bool      `json:"allowEmptySkip"`
	ConfiguredBy   string    `json:"configuredBy"`
	ConfiguredAt   time.Time `json:"configuredAt"`
}

// ReportFlowCompletionRef identifies one still-current completion for an exact member run.
type ReportFlowCompletionRef struct {
	ID       string `json:"id"`
	RunID    string `json:"runId"`
	TaskSlug string `json:"taskSlug"`
}

// ReportFlowJoinMember records a participating or unknown branch and its exact completion, if any.
type ReportFlowJoinMember struct {
	RunID        string                   `json:"runId"`
	PackageID    string                   `json:"packageId"`
	ConditionKey string                   `json:"conditionKey"`
	When         string                   `json:"when"`
	State        string                   `json:"state"`
	AdmissionID  *string                  `json:"admissionId,omitempty"`
	Basis        *ReportBranchProof       `json:"basis,omitempty"`
	Completion   *ReportFlowCompletionRef `json:"completion,omitempty"`
}

// ReportFlowJoinCondition retains each predicate actually read for this decision, including unselected paths.
type ReportFlowJoinCondition struct {
	Key        string                 `json:"key"`
	Evaluation ReportBranchEvaluation `json:"evaluation"`
}

// ReportFlowJoinEvaluation is the current all/any decision, not a new execution event.
type ReportFlowJoinEvaluation struct {
	Satisfied    bool                      `json:"satisfied"`
	Reason       string                    `json:"reason,omitempty"`
	Participants []ReportFlowJoinMember    `json:"participants"`
	Unknown      []ReportFlowJoinMember    `json:"unknown"`
	Conditions   []ReportFlowJoinCondition `json:"conditions"`
	Witness      *ReportFlowCompletionRef  `json:"witness,omitempty"`
	ExplicitSkip bool                      `json:"explicitSkip"`
}

// ReportFlowJoinProof freezes the result and its exact member/fact references at first admission.
type ReportFlowJoinProof struct {
	FlowID      string                   `json:"flowId"`
	TargetRunID string                   `json:"targetRunId"`
	Mode        string                   `json:"mode"`
	Evaluation  ReportFlowJoinEvaluation `json:"evaluation"`
}

// ReportFlowJoinAdmission identifies the original authorization and immutable join proof.
type ReportFlowJoinAdmission struct {
	ID    string              `json:"id"`
	Proof ReportFlowJoinProof `json:"proof"`
}

// ReportFlowJoin combines fixed configuration with a current, read-only projection.
type ReportFlowJoin struct {
	ReportFlowJoinConfig
	Evaluation ReportFlowJoinEvaluation `json:"evaluation"`
	Admission  *ReportFlowJoinAdmission `json:"admission,omitempty"`
}

// ReportBranchFlow is the fixed occurrence plus current read-only projections.
type ReportBranchFlow struct {
	ID                 string                  `json:"id"`
	Project            string                  `json:"project"`
	IdempotencyKey     string                  `json:"idempotencyKey"`
	CandidateAttemptID *string                 `json:"candidateAttemptId,omitempty"`
	Conditions         []ReportBranchCondition `json:"conditions"`
	Branches           []ReportBranchMember    `json:"branches"`
	Joins              []ReportFlowJoinConfig  `json:"joins"`
	ConfiguredBy       string                  `json:"configuredBy"`
	ConfiguredAt       time.Time               `json:"configuredAt"`
	CancelledBy        *string                 `json:"cancelledBy,omitempty"`
	CancelReason       *string                 `json:"cancelReason,omitempty"`
	CancelledAt        *time.Time              `json:"cancelledAt,omitempty"`
}
