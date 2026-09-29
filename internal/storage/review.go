// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// ReviewRequest preserves assigned sources, writer responsibility and reviewer identity with recorded decisions.
type ReviewRequest struct {
	ID                     string           `json:"id"`
	TaskSlug               string           `json:"taskSlug"`
	Kind                   string           `json:"kind"`
	Sources                []ReviewSource   `json:"sources"`
	AuthorResponsibility   ReviewAuthor     `json:"authorResponsibility"`
	RequirementDecisionIDs []string         `json:"requirementDecisionIds"`
	ReviewerRunID          string           `json:"reviewerRunId"`
	CompletionRunID        *string          `json:"completionRunId"`
	ResponsibleUserID      string           `json:"responsibleUserId"`
	MaxReviewRounds        int32            `json:"maxReviewRounds"`
	CreatedBy              string           `json:"createdBy"`
	CreatedAt              time.Time        `json:"createdAt"`
	DecidedAt              *time.Time       `json:"decidedAt"`
	Decisions              []ReviewDecision `json:"decisions"`
}

// ReviewStatus returns a task's current review states grouped by review kind.
type ReviewStatus struct {
	TaskSlug string        `json:"taskSlug"`
	Reviews  []ReviewState `json:"reviews"`
}

// ReviewState projects the assigned request, rejection count and any hold requiring human intervention.
type ReviewState struct {
	Kind              string         `json:"kind"`
	Request           *ReviewRequest `json:"request"`
	AgentRejections   int32          `json:"agentRejections"`
	MaxReviewRounds   int32          `json:"maxReviewRounds"`
	HumanHold         bool           `json:"humanHold"`
	ResponsibleUserID *string        `json:"responsibleUserId"`
}

// ReviewSource selects original spec-field revisions or native repository content, not caller-authored quotations.
type ReviewSource struct {
	Kind           string `json:"kind"`
	SpecSlug       string `json:"specSlug,omitempty"`
	Field          string `json:"field,omitempty"`
	ChangeID       string `json:"changeId,omitempty"`
	EnvironmentID  string `json:"environmentId,omitempty"`
	RepositoryRoot string `json:"repositoryRoot,omitempty"`
	CommitSHA      string `json:"commitSha,omitempty"`
	Path           string `json:"path,omitempty"`
	Entry          string `json:"entry,omitempty"`
}

// ReviewAuthor identifies the responsible writer checked to prevent self-review.
type ReviewAuthor struct {
	Kind   string `json:"kind"`
	RunID  string `json:"runId,omitempty"`
	UserID string `json:"userId,omitempty"`
}

// AssignReviewRequest fixes the sources, writer and separate reviewer for a task's review round.
type AssignReviewRequest struct {
	TaskSlug               string         `json:"taskSlug"`
	Kind                   string         `json:"kind"`
	Sources                []ReviewSource `json:"sources"`
	AuthorResponsibility   ReviewAuthor   `json:"authorResponsibility"`
	RequirementDecisionIDs []string       `json:"requirementDecisionIds"`
	ReviewerRunID          string         `json:"reviewerRunId"`
	CompletionRunID        *string        `json:"completionRunId,omitempty"`
	MaxReviewRounds        *int32         `json:"maxReviewRounds,omitempty"`
}

// ReviewDecision preserves an actor's verdict and basis for one assigned request; it is not execution proof.
type ReviewDecision struct {
	ID            string    `json:"id"`
	RequestID     string    `json:"requestId"`
	Verdict       string    `json:"verdict"`
	Basis         string    `json:"basis"`
	ActorKind     string    `json:"actorKind"`
	Actor         string    `json:"actor"`
	ReviewerRunID *string   `json:"reviewerRunId"`
	CreatedAt     time.Time `json:"createdAt"`
}

// SourceReviewResult separates the recorded source verdict from any resulting authoring-completion outcome.
type SourceReviewResult struct {
	Recorded            bool                 `json:"recorded"`
	Decision            ReviewDecision       `json:"decision"`
	Status              *ReviewStatus        `json:"status"`
	AuthoringCompletion *AuthoringCompletion `json:"authoringCompletion,omitempty"`
}

// AuthoringCompletion reports whether source review completed the specified authoring run or why it could not.
type AuthoringCompletion struct {
	RunID    string `json:"runId"`
	TaskSlug string `json:"taskSlug"`
	Status   string `json:"status"`
	Message  string `json:"message,omitempty"`
}

// ReviewCompletion reports the separate task-completion outcome after a delivery verdict, including not-requested status.
type ReviewCompletion struct {
	Status  string `json:"status"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// ReviewResult returns a recorded delivery verdict separately from review state and task completion.
type ReviewResult struct {
	Recorded     bool             `json:"recorded"`
	AcceptanceID string           `json:"acceptanceId"`
	DeliveryID   string           `json:"deliveryId"`
	Verdict      string           `json:"verdict"`
	RequestID    *string          `json:"requestId"`
	Status       *ReviewStatus    `json:"status"`
	Completion   ReviewCompletion `json:"completion"`
}
