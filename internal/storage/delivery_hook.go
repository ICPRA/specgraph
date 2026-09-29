// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"time"
)

var (
	// ErrInvalidDeliveryHook rejects a malformed delivery trigger or host-result report.
	ErrInvalidDeliveryHook  = errors.New("invalid delivery test hook")
	ErrDeliveryHookConflict = errors.New("delivery test hook conflicts with its fixed configuration or admission")
	ErrDeliveryHookNotFound = errors.New("delivery test hook not found")
)

// ArmDeliveryHookRequest fixes a source run and commit to an already prepared test run.
type ArmDeliveryHookRequest struct {
	SourceRunID    string  `json:"sourceRunId"`
	TargetRunID    string  `json:"targetRunId"`
	CommitSHA      string  `json:"commitSha"`
	IdempotencyKey string  `json:"idempotencyKey"`
	DeliveryID     *string `json:"deliveryId,omitempty"`
}

// DeliveryTestHook preserves fixed delivery-test configuration and recorded trigger and host-dispatch history.
// Triggering or dispatching does not establish a passed test report or task completion.
type DeliveryTestHook struct {
	ID                 string     `json:"id"`
	Project            string     `json:"project"`
	SourceRunID        string     `json:"sourceRunId"`
	TargetRunID        string     `json:"targetRunId"`
	TargetPackageID    string     `json:"targetPackageId"`
	CommitSHA          string     `json:"commitSha"`
	ConfiguredByRunID  *string    `json:"configuredByRunId"`
	ConfiguredByUserID string     `json:"configuredByUserId"`
	HostConsumerUserID string     `json:"hostConsumerUserId"`
	CreatedAt          time.Time  `json:"createdAt"`
	DeliveryID         *string    `json:"deliveryId"`
	TriggeredAt        *time.Time `json:"triggeredAt"`
	CancelledAt        *time.Time `json:"cancelledAt"`
	CancelledByRunID   *string    `json:"cancelledByRunId"`
	CancelledByUserID  *string    `json:"cancelledByUserId"`
	CancellationReason *string    `json:"cancellationReason"`
	State              string     `json:"state"`
	DispatchStatus     *string    `json:"dispatchStatus"`
	DispatchPhase      *string    `json:"dispatchPhase"`
	DispatchDetail     *string    `json:"dispatchDetail"`
	DispatchRecordedAt *time.Time `json:"dispatchRecordedAt"`
	RetriedAt          *time.Time `json:"retriedAt"`
	RetriedByRunID     *string    `json:"retriedByRunId"`
	RetriedByUserID    *string    `json:"retriedByUserId"`
}

// DeliveryHookHostScope identifies the native environment and project checked for host consumption.
type DeliveryHookHostScope struct {
	EnvironmentID   string `json:"environmentId"`
	NativeProjectID string `json:"nativeProjectId"`
}

// DeliveryHookParent identifies the source run's native thread for the host's test dispatch.
type DeliveryHookParent struct {
	RunID         string `json:"runId"`
	EnvironmentID string `json:"environmentId"`
	ThreadID      string `json:"threadId"`
	ProjectID     string `json:"projectId"`
}

// DeliveryHookPreparation returns the fixed test context, dispatch authorization and current readiness failure.
type DeliveryHookPreparation struct {
	Hook           *DeliveryTestHook           `json:"hook"`
	Context        RunContext                  `json:"context"`
	Dispatch       RunDispatchStatus           `json:"dispatch"`
	Parent         *DeliveryHookParent         `json:"parent"`
	ReadinessError *DeliveryHookReadinessError `json:"readinessError"`
}

// DeliveryHookReadinessError exposes a known eligibility failure during a host read; it is not a start grant.
type DeliveryHookReadinessError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// DeliveryHookContext gives a bound test run the delivery and commit selected by its triggering hook.
type DeliveryHookContext struct {
	HookID      string `json:"hookId"`
	DeliveryID  string `json:"deliveryId"`
	SourceRunID string `json:"sourceRunId"`
	TargetRunID string `json:"targetRunId"`
	CommitSHA   string `json:"commitSha"`
}

// DeliveryHookHostResult reports host dispatch status and detail, not a test verdict or completion proof.
type DeliveryHookHostResult struct {
	Status string  `json:"status"`
	Phase  *string `json:"phase,omitempty"`
	Detail *string `json:"detail,omitempty"`
}

// DeliveryTestHookPage returns host-scoped delivery-test triggers and a browsing continuation cursor.
type DeliveryTestHookPage struct {
	Hooks      []DeliveryTestHook `json:"hooks"`
	NextCursor string             `json:"nextCursor"`
}
