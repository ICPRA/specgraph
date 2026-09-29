// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// NodeOwnershipSummary names a real run fact, or an explicitly human-authored substitute.
type NodeOwnershipSummary struct {
	Kind             string                   `json:"kind"`
	ProgressEventID  *string                  `json:"eventId,omitempty"`
	ProgramAttemptID *string                  `json:"attemptId,omitempty"`
	HumanSubstitute  *NodeHumanHandoffSummary `json:"humanSubstitute,omitempty"`
}

type NodeHumanHandoffSummary struct {
	Statement   string   `json:"statement"`
	SourceRefs  []string `json:"sourceRefs"`
	MissingInfo []string `json:"missingInfo"`
}

type NodeOwnershipHandoff struct {
	RunID       string               `json:"runId"`
	AdmissionID string               `json:"admissionId"`
	Summary     NodeOwnershipSummary `json:"summary"`
}

// Frozen identities never change as a pending take awaits the old execution's handoff.
type NodeOwnershipDispatch struct {
	RunID           string  `json:"runId"`
	AdmissionID     string  `json:"admissionId"`
	ExecutorKind    string  `json:"executorKind"`
	EnvironmentID   string  `json:"environmentId"`
	NativeProjectID *string `json:"nativeProjectId,omitempty"`
	AttemptID       *string `json:"attemptId,omitempty"`
}

type NodeOwnershipPreparation struct {
	RunID         string `json:"runId"`
	PackageID     string `json:"packageId"`
	ExecutorKind  string `json:"executorKind"`
	State         string `json:"state"`
	EnvironmentID string `json:"environmentId"`
	ThreadID      string `json:"threadId"`
	Raw           bool   `json:"raw"`
}

// A bound raw preparation has no admission; the human records the observed stop in this receipt.
type NodeOwnershipPreparedHandoff struct {
	RunID     string               `json:"runId"`
	PackageID string               `json:"packageId"`
	Summary   NodeOwnershipSummary `json:"summary"`
	StopNote  string               `json:"stopNote"`
}

// Current observations are separate from the identities frozen by begin.
type NodeOwnershipDispatchObservation struct {
	RunID            string            `json:"runId"`
	AdmissionID      string            `json:"admissionId"`
	StopConfirmedAt  *time.Time        `json:"stopConfirmedAt,omitempty"`
	ReleaseKind      *string           `json:"releaseKind,omitempty"`
	CurrentAttemptID *string           `json:"currentAttemptId,omitempty"`
	ProgramResult    *ProgramRunResult `json:"programResult"`
}

type NodeOwnershipPreparationObservation struct {
	RunID       string     `json:"runId"`
	PackageID   string     `json:"packageId"`
	CancelledAt *time.Time `json:"cancelledAt,omitempty"`
}

type NodeOwnershipClaim struct {
	Agent        string    `json:"agent"`
	ClaimedAt    time.Time `json:"claimedAt"`
	LeaseExpires time.Time `json:"leaseExpires"`
}

type NodeOwnershipClaimHandoff struct {
	Agent     string               `json:"agent"`
	ClaimedAt time.Time            `json:"claimedAt"`
	Summary   NodeOwnershipSummary `json:"summary"`
	StopNote  string               `json:"stopNote"`
}

// NodeOwnershipOperation is the single durable receipt for a pending or finished owner change.
type NodeOwnershipOperation struct {
	ID                string                         `json:"id"`
	TaskSlug          string                         `json:"taskSlug"`
	IdempotencyKey    string                         `json:"idempotencyKey"`
	Action            string                         `json:"action"`
	Status            string                         `json:"status"`
	BeforeOwnerUserID *string                        `json:"beforeOwnerUserId"`
	AfterOwnerUserID  *string                        `json:"afterOwnerUserId"`
	FromVersion       int32                          `json:"fromVersion"`
	ToVersion         *int32                         `json:"toVersion,omitempty"`
	ActorUserID       string                         `json:"actorUserId"`
	Reason            string                         `json:"reason"`
	Dispatches        []NodeOwnershipDispatch        `json:"dispatches"`
	Preparations      []NodeOwnershipPreparation     `json:"preparations"`
	Handoffs          []NodeOwnershipHandoff         `json:"handoffs"`
	PreparedHandoffs  []NodeOwnershipPreparedHandoff `json:"preparedHandoffs"`
	FrozenClaim       *NodeOwnershipClaim            `json:"frozenClaim"`
	ClaimHandoff      *NodeOwnershipClaimHandoff     `json:"claimHandoff"`
	CreatedAt         time.Time                      `json:"createdAt"`
	FinishedAt        *time.Time                     `json:"finishedAt,omitempty"`
	CancelActorUserID *string                        `json:"cancelActorUserId,omitempty"`
	CancelReason      *string                        `json:"cancelReason,omitempty"`
	Replayed          bool                           `json:"replayed,omitempty"`
}

type NodeOwnershipState struct {
	TaskSlug                string                                `json:"taskSlug"`
	Version                 int32                                 `json:"version"`
	HumanOwnerUserID        *string                               `json:"humanOwnerUserId"`
	Pending                 *NodeOwnershipOperation               `json:"pending,omitempty"`
	DispatchObservations    []NodeOwnershipDispatchObservation    `json:"dispatchObservations"`
	PreparationObservations []NodeOwnershipPreparationObservation `json:"preparationObservations"`
	CurrentClaim            *NodeOwnershipClaim                   `json:"currentClaim"`
}

type NodeOwnershipPage struct {
	TaskSlug   string                   `json:"taskSlug"`
	Operations []NodeOwnershipOperation `json:"operations"`
	HasMore    bool                     `json:"hasMore"`
	NextCursor *string                  `json:"nextCursor"`
}

type BeginNodeOwnershipTakeRequest struct {
	TaskSlug            string  `json:"taskSlug"`
	ExpectedVersion     int32   `json:"expectedVersion"`
	ExpectedOwnerUserID *string `json:"expectedOwnerUserId"`
	IdempotencyKey      string  `json:"idempotencyKey"`
	Reason              string  `json:"reason"`
}

type CommitNodeOwnershipTakeRequest struct {
	OperationID      string                         `json:"operationId"`
	ExpectedVersion  int32                          `json:"expectedVersion"`
	Handoffs         []NodeOwnershipHandoff         `json:"handoffs"`
	PreparedHandoffs []NodeOwnershipPreparedHandoff `json:"preparedHandoffs"`
	ClaimHandoff     *NodeOwnershipClaimHandoff     `json:"claimHandoff"`
}

type CancelNodeOwnershipTakeRequest struct {
	OperationID string `json:"operationId"`
	Reason      string `json:"reason"`
}

type ReturnNodeOwnershipRequest struct {
	TaskSlug            string  `json:"taskSlug"`
	ExpectedVersion     int32   `json:"expectedVersion"`
	ExpectedOwnerUserID *string `json:"expectedOwnerUserId"`
	IdempotencyKey      string  `json:"idempotencyKey"`
	Reason              string  `json:"reason"`
}

type RecordOwnNodeHandoffSummaryRequest struct {
	EventID string `json:"eventId"`
	Message string `json:"message"`
}

type NodeHandoffSummaryReceipt struct {
	EventID    string    `json:"eventId"`
	RunID      string    `json:"runId"`
	TaskSlug   string    `json:"taskSlug"`
	Message    string    `json:"message"`
	RecordedAt time.Time `json:"recordedAt"`
	Replayed   bool      `json:"replayed"`
}

type ConfirmProgramRunStoppedRequest struct {
	RunID           string `json:"runId"`
	AdmissionID     string `json:"admissionId"`
	AttemptID       string `json:"attemptId"`
	EnvironmentID   string `json:"environmentId"`
	NativeProjectID string `json:"nativeProjectId"`
	Note            string `json:"note"`
}
