// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"time"
)

var (
	// ErrInvalidCompletionHook rejects a malformed completion trigger or host-result report.
	ErrInvalidCompletionHook  = errors.New("invalid completion program hook")
	ErrCompletionHookConflict = errors.New("completion hook source, grant or fixed program preparation is not eligible")
	ErrCompletionHookNotFound = errors.New("completion program hook not found")
	ErrProgramHookRequired    = errors.New("program has an active completion hook; use its trigger admission or cancel the hook before manual start")
)

// CompletionFactRef identifies the recorded completion or summary-acceptance fact used by a trigger.
type CompletionFactRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ArmCompletionHookRequest fixes a source completion trigger and its already prepared program target.
type ArmCompletionHookRequest struct {
	SourceTaskSlug  string             `json:"sourceTaskSlug"`
	SourceSpecID    string             `json:"sourceSpecId"`
	TargetRunID     string             `json:"targetRunId"`
	TargetPackageID string             `json:"targetPackageId"`
	IdempotencyKey  string             `json:"idempotencyKey"`
	ConfirmTrigger  bool               `json:"confirmTrigger"`
	Fact            *CompletionFactRef `json:"fact,omitempty"`
}

// CompletionProgramHook preserves the fixed source, target and grants alongside trigger and dispatch history.
// Its admission authorizes a host start, not observed program execution or completion.
type CompletionProgramHook struct {
	ID                 string             `json:"id"`
	Project            string             `json:"project"`
	SourceTaskSlug     string             `json:"sourceTaskSlug"`
	SourceSpecID       string             `json:"sourceSpecId"`
	SourceRole         string             `json:"sourceRole"`
	TargetRunID        string             `json:"targetRunId"`
	TargetPackageID    string             `json:"targetPackageId"`
	IdempotencyKey     string             `json:"idempotencyKey"`
	RequestedFact      *CompletionFactRef `json:"requestedFact"`
	ConfiguredByUserID string             `json:"configuredByUserId"`
	HostConsumerUserID string             `json:"hostConsumerUserId"`
	CreatedAt          time.Time          `json:"createdAt"`
	Fact               *CompletionFactRef `json:"fact"`
	TriggeredAt        *time.Time         `json:"triggeredAt"`
	CancelledAt        *time.Time         `json:"cancelledAt"`
	CancelledByUserID  *string            `json:"cancelledByUserId"`
	CancellationReason *string            `json:"cancellationReason"`
	State              string             `json:"state"`
	DispatchStatus     *string            `json:"dispatchStatus"`
	DispatchDetail     *string            `json:"dispatchDetail"`
	DispatchRecordedAt *time.Time         `json:"dispatchRecordedAt"`
	RetriedAt          *time.Time         `json:"retriedAt"`
	RetriedByUserID    *string            `json:"retriedByUserId"`
	ProgramAdmissionID *string            `json:"programAdmissionId"`
	Admission          *RunDispatch       `json:"admission"`
}

// CompletionHookPreparation exposes the fixed program and any current readiness failure to the consuming host.
type CompletionHookPreparation struct {
	Hook           *CompletionProgramHook      `json:"hook"`
	Run            *ProgramRun                 `json:"run"`
	ReadinessError *DeliveryHookReadinessError `json:"readinessError"`
}

// HostHook selects one delivery trigger, completion trigger or program loop for host consumption.
type HostHook struct {
	Kind              string                 `json:"kind"`
	DeliveryTest      *DeliveryTestHook      `json:"deliveryTest,omitempty"`
	CompletionProgram *CompletionProgramHook `json:"completionProgram,omitempty"`
	ProgramLoop       *ProgramLoopReference  `json:"programLoop,omitempty"`
}

// ProgramLoopReference identifies a prepared loop for the host to read in its project scope.
type ProgramLoopReference struct {
	RunID   string `json:"runId"`
	Project string `json:"project"`
}

// HostHookPage returns host-scoped work references with a browsing continuation cursor.
type HostHookPage struct {
	Hooks      []HostHook `json:"hooks"`
	NextCursor string     `json:"nextCursor"`
}
