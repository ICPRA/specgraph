// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"time"
)

var (
	// ErrInvalidProgramRun rejects a malformed command, preparation or host observation.
	ErrInvalidProgramRun            = errors.New("invalid program run request")
	ErrProgramRunForbidden          = errors.New("program run requires its verified human or recorded host service identity")
	ErrProgramResultConflict        = errors.New("program result conflicts with observed facts for this attempt")
	ErrProgramCompletionUnavailable = errors.New("program completion requires an explicit eligible completion contract and a normal successful exit")
	ErrInvalidProgramLoop           = errors.New("invalid program loop")
	ErrProgramLoopConflict          = errors.New("program loop attempt or judgment conflicts with recorded history")
	ErrProgramLoopWaiting           = errors.New("program loop awaits a definite termination judgment and normal exit")
	ErrProgramLoopStopped           = errors.New("program loop has stopped")
	ErrProgramLoopNeedsHuman        = errors.New("program loop attempts exhausted; human intervention required")
)

// ProgramCommand fixes the native command and optional QA, completion and loop contract at preparation.
type ProgramCommand struct {
	Executable        string             `json:"executable"`
	Args              []string           `json:"args"`
	Cwd               string             `json:"cwd"`
	EnvironmentID     string             `json:"environmentId"`
	NativeProjectID   string             `json:"nativeProjectId"`
	TimeoutMS         int64              `json:"timeoutMs"`
	WorkPurpose       string             `json:"workPurpose"`
	CompleteOnSuccess bool               `json:"completeOnSuccess,omitempty"`
	QABasis           *DispatchQABasis   `json:"qaBasis,omitempty"`
	Loop              *ProgramLoopConfig `json:"loop,omitempty"`
}

// ProgramTarget preserves the prepared command and its authorizing human and consuming host identities.
type ProgramTarget struct {
	ProgramCommand
	AuthorizedByUserID string `json:"authorizedByUserId"`
	HostConsumerUserID string `json:"hostConsumerUserId"`
}

// PrepareProgramRunRequest identifies the task and idempotent command preparation; it does not authorize a start.
type PrepareProgramRunRequest struct {
	TaskSlug       string         `json:"taskSlug"`
	IdempotencyKey string         `json:"idempotencyKey"`
	Command        ProgramCommand `json:"command"`
}

// ProgramHostScope identifies the native environment and project checked against the recorded host grant.
type ProgramHostScope struct {
	EnvironmentID   string `json:"environmentId"`
	NativeProjectID string `json:"nativeProjectId"`
}

// ProgramObservation supplies host-reported attempt facts. Timeout and cancellation
// are observations, never proof that the process tree stopped.
type ProgramObservation struct {
	Outcome           string     `json:"outcome"`
	ExitCode          *int64     `json:"exitCode"`
	ObservedAt        time.Time  `json:"observedAt"`
	StartedAt         *time.Time `json:"startedAt"`
	FinishedAt        *time.Time `json:"finishedAt"`
	TimedOutAt        *time.Time `json:"timedOutAt"`
	CancelRequestedAt *time.Time `json:"cancelRequestedAt"`
	Summary           *string    `json:"summary"`
}

// ProgramRunResult attaches the verified reporter and recording time to a host observation.
type ProgramRunResult struct {
	ProgramObservation
	ReporterUserID string    `json:"reporterUserId"`
	RecordedAt     time.Time `json:"recordedAt"`
}

// ProgramRun is the stored preparation with dispatch authorization and observed results kept separate.
type ProgramRun struct {
	Context        RunContext              `json:"context"`
	ExecutorKind   string                  `json:"executorKind"`
	State          string                  `json:"state"`
	Dispatch       RunDispatchStatus       `json:"dispatch"`
	Result         *ProgramRunResult       `json:"result"`
	Replayed       bool                    `json:"replayed"`
	CurrentAttempt *ProgramAttemptMetadata `json:"currentAttempt,omitempty"`
	LoopState      *ProgramLoopState       `json:"loopState,omitempty"`
	Completion     *ProgramCompletion      `json:"completion,omitempty"`
}

// ProgramCompletion references the original completion event and its consumed attempt/judgment.
type ProgramCompletion struct {
	EventID    string  `json:"eventId"`
	AttemptID  *string `json:"attemptId,omitempty"`
	JudgmentID *string `json:"judgmentId,omitempty"`
}

// ProgramLoopConfig fixes termination rules and an optional attempt ceiling for the prepared command.
type ProgramLoopConfig struct {
	Kind                       string                 `json:"kind"`
	Termination                ProgramLoopTermination `json:"termination"`
	MaxAttempts                *int                   `json:"maxAttempts,omitempty"`
	SynchronousCompletionBasis string                 `json:"synchronousCompletionBasis"`
}

// ProgramLoopTermination selects an exit-code rule or an explicitly recorded judgment criterion.
type ProgramLoopTermination struct {
	Kind     string               `json:"kind"`
	ExitCode *ProgramLoopExitCode `json:"exitCode,omitempty"`
	Judgment *ProgramLoopJudgment `json:"judgment,omitempty"`
}

// ProgramLoopExitCode distinguishes terminating and continuing normal exits; other codes remain unknown.
type ProgramLoopExitCode struct {
	StopCode      int64   `json:"stopCode"`
	ContinueCodes []int64 `json:"continueCodes"`
}

// ProgramLoopJudgment records the criterion used by an authorized actor's termination judgment.
type ProgramLoopJudgment struct {
	Criterion string `json:"criterion"`
}

// ProgramAttemptMetadata identifies a start grant and the predecessor and judgment it consumed.
type ProgramAttemptMetadata struct {
	ID                 string    `json:"id"`
	Ordinal            int       `json:"ordinal"`
	GrantedAt          time.Time `json:"grantedAt"`
	PredecessorID      *string   `json:"predecessorId,omitempty"`
	ConsumedJudgmentID *string   `json:"consumedJudgmentId,omitempty"`
}

// ProgramAttempt pairs a recorded start grant with its optional host-reported result.
type ProgramAttempt struct {
	ProgramAttemptMetadata
	Result *ProgramRunResult `json:"result,omitempty"`
}

// ProgramLoopCondition projects true, false or unknown termination and any supporting judgment ID.
type ProgramLoopCondition struct {
	Value      string `json:"value"`
	JudgmentID string `json:"judgmentId,omitempty"`
}

// ProgramLoopState projects grants, observations and judgments; admitted is not proof of a running process.
// Stopped denotes the loop's termination condition, not process-tree shutdown or task completion.
type ProgramLoopState struct {
	Status     string                `json:"status"`
	Condition  *ProgramLoopCondition `json:"condition,omitempty"`
	Stop       *ProgramLoopEvent     `json:"stop,omitempty"`
	Completion *ProgramCompletion    `json:"completion,omitempty"`
}

// ProgramLoopEvent records an authorized termination judgment or stop request with its actor and reason.
type ProgramLoopEvent struct {
	ID            string    `json:"id"`
	RunID         string    `json:"runId"`
	AttemptID     string    `json:"attemptId,omitempty"`
	Kind          string    `json:"kind"`
	Value         string    `json:"value,omitempty"`
	ActorUserID   string    `json:"actorUserId"`
	ActorRunID    string    `json:"actorRunId,omitempty"`
	Reason        string    `json:"reason"`
	RecordedAt    time.Time `json:"recordedAt"`
	PredecessorID *string   `json:"predecessorId,omitempty"`
}

// ProgramLoopHistoryPage returns stored attempts and events with independent continuation cursors.
type ProgramLoopHistoryPage struct {
	Attempts          []ProgramAttempt   `json:"attempts"`
	Events            []ProgramLoopEvent `json:"events"`
	NextAttemptCursor string             `json:"nextAttemptCursor"`
	NextEventCursor   string             `json:"nextEventCursor"`
}

// ProgramAdmission tells the host whether this authorization grants a new start;
// replayed admissions do not permit another start and do not prove completion.
type ProgramAdmission struct {
	Run      *ProgramRun `json:"run"`
	MayStart bool        `json:"mayStart"`
}
