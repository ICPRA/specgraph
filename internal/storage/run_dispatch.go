// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"encoding/json"
	"time"
)

// DispatchQABasis keeps the manager's applicability declaration and original source references.
type DispatchQABasis struct {
	RequirementDecisionIDs []string       `json:"requirementDecisionIds"`
	DesignDecisionIDs      []string       `json:"designDecisionIds"`
	DesignSources          []ReviewSource `json:"designSources"`
	TestPlanSources        []ReviewSource `json:"testPlanSources"`
	Applicability          string         `json:"applicability"`
}

// RunSelfCompletion reports the stored outcome of an authorized run-completion operation.
type RunSelfCompletion struct {
	RunID     string `json:"runId"`
	Completed string `json:"completed"`
}

// DeliverySelfContext supplies a bound run's task and prepared workspace for its own delivery submission.
type DeliverySelfContext struct {
	RunID            string  `json:"runId"`
	TaskSlug         string  `json:"taskSlug"`
	Workspace        string  `json:"workspace"`
	CurrentAttemptID *string `json:"currentAttemptId,omitempty"`
}

// DeliveryHead carries the submitter's native HEAD observation, including an explicit non-repository state.
type DeliveryHead struct {
	IsRepo    bool    `json:"isRepo"`
	CommitSHA *string `json:"commitSha"`
}

// DeliverySelfReceipt identifies the recorded delivery and its own run and task; it is not acceptance.
type DeliverySelfReceipt struct {
	DeliveryID string  `json:"deliveryId"`
	RunID      string  `json:"runId"`
	TaskSlug   string  `json:"taskSlug"`
	AttemptID  *string `json:"attemptId,omitempty"`
}

// RunDispatch is business authorization and responsibility, not host-start proof.
type RunDispatch struct {
	ID                 string               `json:"id"`
	RunID              string               `json:"runId"`
	TaskSlug           string               `json:"taskSlug"`
	PackageID          string               `json:"packageId"`
	Actor              string               `json:"actor"`
	AuthorizedAt       time.Time            `json:"authorizedAt"`
	Replayed           bool                 `json:"replayed"`
	ReportFlowBasis    *ReportBranchProof   `json:"reportFlowBasis,omitempty"`
	ReportJoinBasis    *ReportFlowJoinProof `json:"reportJoinBasis,omitempty"`
	CandidateAttemptID *string              `json:"candidateAttemptId,omitempty"`
}

// RunDispatchResolution records an actor's release of business responsibility, not host process shutdown.
type RunDispatchResolution struct {
	Actor      string    `json:"actor"`
	Kind       string    `json:"kind"`
	Note       string    `json:"note"`
	ReleasedAt time.Time `json:"releasedAt"`
}

// RunDispatchStatus separates admission, responsibility release, preparation cancellation and stop observation.
type RunDispatchStatus struct {
	Admission        *RunDispatch                `json:"admission"`
	Resolution       *RunDispatchResolution      `json:"resolution"`
	Cancellation     *RunPreparationCancellation `json:"cancellation"`
	StopConfirmation *RunStopConfirmation        `json:"stopConfirmation"`
}

// RunStopConfirmation records an operator's host observation, not backend process proof.
type RunStopConfirmation struct {
	Actor            string    `json:"actor"`
	Note             string    `json:"note"`
	ConfirmedAt      time.Time `json:"confirmedAt"`
	ProgramAttemptID *string   `json:"programAttemptId,omitempty"`
}

// RunPreparationCancellation records cancellation of a preparation, not proof that an admitted process stopped.
type RunPreparationCancellation struct {
	Actor       string                 `json:"actor"`
	Note        string                 `json:"note"`
	CancelledAt time.Time              `json:"cancelledAt"`
	RawHandoff  *RawPreparationHandoff `json:"rawHandoff,omitempty"`
}

type RawPreparationHandoff struct {
	EnvironmentID string               `json:"environmentId"`
	ThreadID      string               `json:"threadId"`
	Summary       NodeOwnershipSummary `json:"summary"`
}

// PreparationAbortRequest identifies the exact preparation to cancel, including its fixed target and reason.
type PreparationAbortRequest struct {
	TaskSlug       string          `json:"taskSlug"`
	Workspace      string          `json:"workspace"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Target         json.RawMessage `json:"target"`
	Note           string          `json:"note"`
}

// PreparationAbortResult records the cancelled idempotency key and any existing run, including replay status.
type PreparationAbortResult struct {
	IdempotencyKey string  `json:"idempotencyKey"`
	RunID          *string `json:"runId"`
	Replayed       bool    `json:"replayed"`
}
