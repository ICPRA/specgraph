// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"errors"
	"time"
)

var (
	// ErrInvalidSummary rejects malformed disposition, acceptance or revocation input.
	ErrInvalidSummary            = errors.New("invalid summary request")
	ErrSummaryForbidden          = errors.New("summary management forbidden")
	ErrSummaryConflict           = errors.New("summary request conflicts with prior record")
	ErrSummaryNotAcceptable      = errors.New("summary has unresolved obligations")
	ErrSummaryAcceptanceNotFound = errors.New("summary acceptance not found")
)

// SummaryDispositionRequest proposes a reviewed withdrawal or replacement of an obligation within a goal.
type SummaryDispositionRequest struct {
	GoalSlug         string         `json:"goalSlug"`
	AffectedSlug     string         `json:"affectedSlug"`
	Disposition      string         `json:"disposition"`
	ReplacementSlug  string         `json:"replacementSlug,omitempty"`
	ReviewDecisionID string         `json:"reviewDecisionId"`
	BeforeSources    []ReviewSource `json:"beforeSources"`
	AfterSources     []ReviewSource `json:"afterSources"`
	Reason           string         `json:"reason"`
	IdempotencyKey   string         `json:"idempotencyKey"`
}

// SummaryDisposition preserves an obligation disposition with its recorded actor and source-review basis.
type SummaryDisposition struct {
	SummaryDispositionRequest
	ID          string    `json:"id"`
	ActorKind   string    `json:"actorKind"`
	ActorUserID string    `json:"actorUserId"`
	ActorRunID  *string   `json:"actorRunId"`
	CreatedAt   time.Time `json:"createdAt"`
}

// SummaryAcceptRequest declares goal satisfaction against an expected graph snapshot and explicit impact review.
type SummaryAcceptRequest struct {
	GoalSlug           string                `json:"goalSlug"`
	Basis              string                `json:"basis"`
	EvidenceSources    []ReviewSource        `json:"evidenceSources"`
	GoalsSatisfied     bool                  `json:"goalsSatisfied"`
	IdempotencyKey     string                `json:"idempotencyKey"`
	ExpectedReferences SummaryReferences     `json:"expectedReferences"`
	ImpactReview       []SummaryImpactReview `json:"impactReview"`
}

// SummaryImpactReview records the acceptance actor's basis for an affected node and any applicable disposition.
type SummaryImpactReview struct {
	Slug          string `json:"slug"`
	Basis         string `json:"basis"`
	DispositionID string `json:"dispositionId,omitempty"`
}

// SummaryNodeReference captures node sources and lifecycle identity for detecting changes since acceptance.
type SummaryNodeReference struct {
	ID                   string            `json:"id"`
	Slug                 string            `json:"slug"`
	Role                 SpecRole          `json:"role"`
	SourceRefs           map[string]string `json:"sourceRefs"`
	CompletionGeneration *int64            `json:"completionGeneration,omitempty"`
	LifecycleChangeID    string            `json:"lifecycleChangeId"`
}

// SummaryRelationReference captures a graph relationship and its change record for acceptance comparison.
type SummaryRelationReference struct {
	FromSlug string `json:"fromSlug"`
	ToSlug   string `json:"toSlug"`
	Type     string `json:"type"`
	ChangeID string `json:"changeId"`
	Present  bool   `json:"present"`
}

// SummaryDecisionReference preserves a linked decision's source identities for acceptance comparison.
type SummaryDecisionReference struct {
	ID         string            `json:"id"`
	Slug       string            `json:"slug"`
	SourceRefs map[string]string `json:"sourceRefs"`
}

// SummaryReferences is the graph and source snapshot checked when recording or evaluating goal acceptance.
type SummaryReferences struct {
	Nodes           []SummaryNodeReference     `json:"nodes"`
	Relations       []SummaryRelationReference `json:"relations"`
	DispositionIDs  []string                   `json:"dispositionIds"`
	DecisionSources []SummaryDecisionReference `json:"decisionSources"`
}

// SummarySourceChange exposes a source-field difference between the accepted snapshot and current graph.
type SummarySourceChange struct {
	Slug   string `json:"slug"`
	Field  string `json:"field"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// SummaryAcceptance preserves an actor's goal verdict and any revocation; Current is evaluated against current sources.
type SummaryAcceptance struct {
	SummaryAcceptRequest
	ID               string     `json:"id"`
	ActorKind        string     `json:"actorKind"`
	ActorUserID      string     `json:"actorUserId"`
	ActorRunID       *string    `json:"actorRunId"`
	CreatedAt        time.Time  `json:"createdAt"`
	RevokedAt        *time.Time `json:"revokedAt"`
	RevokedByUserID  *string    `json:"revokedByUserId"`
	RevokedByRunID   *string    `json:"revokedByRunId"`
	RevocationReason *string    `json:"revocationReason"`
	Current          bool       `json:"current"`
}

// SummaryObligation projects a goal's reachable work, applicable disposition and pending-review status.
type SummaryObligation struct {
	Slug          string   `json:"slug"`
	Role          SpecRole `json:"role"`
	Stage         string   `json:"stage"`
	Disposition   string   `json:"disposition"`
	DispositionID string   `json:"dispositionId,omitempty"`
	Effective     bool     `json:"effective"`
	PendingReview bool     `json:"pendingReview"`
}

// SummaryBlocker identifies a node and the reason current goal acceptance is unavailable.
type SummaryBlocker struct {
	Slug string `json:"slug"`
	Code string `json:"code"`
}

// SummaryState evaluates a goal's current graph and sources, separating acceptance eligibility from recorded acceptance.
type SummaryState struct {
	GoalSlug         string                `json:"goalSlug"`
	Acceptable       bool                  `json:"acceptable"`
	Accepted         bool                  `json:"accepted"`
	Blockers         []SummaryBlocker      `json:"blockers"`
	Obligations      []SummaryObligation   `json:"obligations"`
	LatestAcceptance *SummaryAcceptance    `json:"latestAcceptance"`
	Dispositions     []SummaryDisposition  `json:"dispositions"`
	References       SummaryReferences     `json:"references"`
	ChangedSources   []SummarySourceChange `json:"changedSources"`
	ScopeChanged     bool                  `json:"scopeChanged"`
	ReviewCandidates []string              `json:"reviewCandidates"`
}

// SummaryHistory is one record kind's project-scoped history, newest first.
type SummaryHistory struct {
	GoalSlug     string               `json:"goalSlug"`
	Kind         string               `json:"kind"`
	Dispositions []SummaryDisposition `json:"dispositions,omitempty"`
	Acceptances  []SummaryAcceptance  `json:"acceptances,omitempty"`
	HasMore      bool                 `json:"hasMore"`
	NextCursor   string               `json:"nextCursor"`
}
