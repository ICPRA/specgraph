// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// ArmCandidateLoopRequest fixes one implementation run's budget and assigned plans before dispatch.
type ArmCandidateLoopRequest struct {
	RunID                 string               `json:"runId"`
	MaxAttempts           int                  `json:"maxAttempts"`
	PlanSources           []ReviewSource       `json:"planSources"`
	Join                  *CandidateJoinConfig `json:"join,omitempty"`
	TerminationKind       string               `json:"terminationKind"`
	SatisfactionCriterion *string              `json:"satisfactionCriterion,omitempty"`
}

// CandidateSatisfactionJudgment is an immutable assertion about one formal candidate delivery.
type CandidateSatisfactionJudgment struct {
	ID            string    `json:"id"`
	RunID         string    `json:"runId"`
	AttemptID     string    `json:"attemptId"`
	DeliveryID    string    `json:"deliveryId"`
	Value         string    `json:"value"`
	Reason        string    `json:"reason"`
	ActorUserID   string    `json:"actorUserId"`
	ActorRunID    *string   `json:"actorRunId,omitempty"`
	RecordedAt    time.Time `json:"recordedAt"`
	PredecessorID *string   `json:"predecessorId,omitempty"`
}

// RecordCandidateSatisfactionJudgmentRequest names the exact predecessor in one run and attempt.
type RecordCandidateSatisfactionJudgmentRequest struct {
	RunID              string  `json:"runId"`
	AttemptID          string  `json:"attemptId"`
	ExpectedJudgmentID *string `json:"expectedJudgmentId"`
	Value              string  `json:"value"`
	Reason             string  `json:"reason"`
}

// RecordOwnCandidateSatisfactionJudgmentRequest cannot select another run.
type RecordOwnCandidateSatisfactionJudgmentRequest struct {
	AttemptID          string  `json:"attemptId"`
	ExpectedJudgmentID *string `json:"expectedJudgmentId"`
	Value              string  `json:"value"`
	Reason             string  `json:"reason"`
}

// CandidateSatisfactionJudgmentResult distinguishes a new fact from an exact CAS replay.
type CandidateSatisfactionJudgmentResult struct {
	Judgment CandidateSatisfactionJudgment `json:"judgment"`
	Replayed bool                          `json:"replayed"`
}

// CandidateIntervention freezes one contradiction and any later named human resolution.
type CandidateIntervention struct {
	ID                   string     `json:"id"`
	RunID                string     `json:"runId"`
	AttemptID            string     `json:"attemptId"`
	JudgmentID           string     `json:"judgmentId"`
	ReportIDs            []string   `json:"reportIds"`
	ConflictKind         string     `json:"conflictKind"`
	Reason               string     `json:"reason"`
	RecordedAt           time.Time  `json:"recordedAt"`
	ResolvedAt           *time.Time `json:"resolvedAt,omitempty"`
	ResolvedByUserID     *string    `json:"resolvedByUserId,omitempty"`
	ResolutionReason     *string    `json:"resolutionReason,omitempty"`
	ResolutionJudgmentID *string    `json:"resolutionJudgmentId,omitempty"`
	ResolutionReportIDs  []string   `json:"resolutionReportIds,omitempty"`
}

// ResolveCandidateInterventionRequest compares the facts the human actually reviewed.
type ResolveCandidateInterventionRequest struct {
	RunID              string   `json:"runId"`
	AttemptID          string   `json:"attemptId"`
	InterventionID     string   `json:"interventionId"`
	ExpectedJudgmentID string   `json:"expectedJudgmentId"`
	ExpectedReportIDs  []string `json:"expectedReportIds"`
	Reason             string   `json:"reason"`
}

// CandidateSatisfactionState separates the current judgment from report facts and the durable hold.
type CandidateSatisfactionState struct {
	Criterion    string                         `json:"criterion"`
	Value        string                         `json:"value"`
	Reason       string                         `json:"reason,omitempty"`
	Judgment     *CandidateSatisfactionJudgment `json:"judgment,omitempty"`
	Intervention *CandidateIntervention         `json:"intervention,omitempty"`
}

// CandidateSatisfactionBasis fixes the facts and resolved intervention consumed by next or completion.
type CandidateSatisfactionBasis struct {
	Value          string   `json:"value"`
	JudgmentID     string   `json:"judgmentId"`
	ReportIDs      []string `json:"reportIds"`
	InterventionID *string  `json:"interventionId,omitempty"`
}

// CandidateSatisfactionHistoryPage bounds both immutable judgments and intervention records.
type CandidateSatisfactionHistoryPage struct {
	RunID                  string                          `json:"runId"`
	AttemptID              string                          `json:"attemptId"`
	Judgments              []CandidateSatisfactionJudgment `json:"judgments"`
	Interventions          []CandidateIntervention         `json:"interventions"`
	NextJudgmentCursor     *string                         `json:"nextJudgmentCursor,omitempty"`
	NextInterventionCursor *string                         `json:"nextInterventionCursor,omitempty"`
}

// CandidateJoinConfig fixes optional all/any completion of this attempt's report flow.
type CandidateJoinConfig struct {
	Mode           string `json:"mode"`
	AllowEmptySkip bool   `json:"allowEmptySkip"`
}

// CandidateJoinState is the current attempt's gate, separate from report status.
type CandidateJoinState struct {
	CandidateJoinConfig
	FlowID     *string                   `json:"flowId,omitempty"`
	Evaluation *ReportFlowJoinEvaluation `json:"evaluation,omitempty"`
	Reason     string                    `json:"reason,omitempty"`
}

// CandidateJoinProof freezes the exact source attempt and flow evaluation consumed by continuation or completion.
type CandidateJoinProof struct {
	SourceAttemptID string                   `json:"sourceAttemptId"`
	FlowID          string                   `json:"flowId"`
	Mode            string                   `json:"mode"`
	Evaluation      ReportFlowJoinEvaluation `json:"evaluation"`
}

// CandidateCondition evaluates registered assertions for one fixed formal delivery.
type CandidateCondition struct {
	Value        string               `json:"value"`
	Reason       string               `json:"reason,omitempty"`
	Reports      []ReportBranchReport `json:"reports"`
	MissingPlans []ReviewSource       `json:"missingPlans"`
}

// CandidateAttempt is one granted formal candidate slot, not a provider turn or tool call.
type CandidateAttempt struct {
	ID                   string                      `json:"id"`
	Number               int                         `json:"number"`
	GrantedAt            time.Time                   `json:"grantedAt"`
	PredecessorID        *string                     `json:"predecessorId,omitempty"`
	DeliveryID           *string                     `json:"deliveryId,omitempty"`
	ConsumedCondition    *CandidateCondition         `json:"consumedCondition,omitempty"`
	FlowID               *string                     `json:"flowId,omitempty"`
	ConsumedJoin         *CandidateJoinProof         `json:"consumedJoin,omitempty"`
	ConsumedSatisfaction *CandidateSatisfactionBasis `json:"consumedSatisfaction,omitempty"`
}

// CandidateLoopEvent is a recorded stop or human budget abandonment, not process shutdown.
type CandidateLoopEvent struct {
	ActorUserID string    `json:"actorUserId"`
	ActorRunID  *string   `json:"actorRunId,omitempty"`
	Reason      string    `json:"reason"`
	RecordedAt  time.Time `json:"recordedAt"`
}

// CandidateLoop projects grants and current facts; only a fresh next grant permits another formal candidate.
type CandidateLoop struct {
	RunID                 string                      `json:"runId"`
	TaskSlug              string                      `json:"taskSlug"`
	PackageID             string                      `json:"packageId"`
	MaxAttempts           int                         `json:"maxAttempts"`
	PlanSources           []ReviewSource              `json:"planSources"`
	TerminationKind       string                      `json:"terminationKind"`
	Satisfaction          *CandidateSatisfactionState `json:"satisfaction,omitempty"`
	Join                  *CandidateJoinState         `json:"join,omitempty"`
	ConfiguredBy          string                      `json:"configuredBy"`
	ConfiguredAt          time.Time                   `json:"configuredAt"`
	Attempts              []CandidateAttempt          `json:"attempts"`
	CurrentAttempt        *CandidateAttempt           `json:"currentAttempt,omitempty"`
	GrantedAttempt        *CandidateAttempt           `json:"grantedAttempt,omitempty"`
	Condition             *CandidateCondition         `json:"condition,omitempty"`
	Status                string                      `json:"status"`
	Stop                  *CandidateLoopEvent         `json:"stop,omitempty"`
	Abandon               *CandidateLoopEvent         `json:"abandon,omitempty"`
	CompletedAt           *time.Time                  `json:"completedAt,omitempty"`
	CompletedJoin         *CandidateJoinProof         `json:"completedJoin,omitempty"`
	CompletedCondition    *CandidateCondition         `json:"completedCondition,omitempty"`
	CompletedSatisfaction *CandidateSatisfactionBasis `json:"completedSatisfaction,omitempty"`
	Replayed              bool                        `json:"replayed,omitempty"`
}

// NextCandidateAttemptRequest identifies the predecessor whose false result is consumed once.
type NextCandidateAttemptRequest struct {
	ExpectedPreviousAttemptID string `json:"expectedPreviousAttemptId"`
}
