// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"errors"
	"time"
)

var (
	ErrInvalidNodeMerge   = errors.New("invalid node merge request")
	ErrNodeMergeConflict  = errors.New("node merge conflicts with current graph or prior receipt")
	ErrNodeMergeForbidden = errors.New("node merge forbidden")
	ErrNodeMergeNotFound  = errors.New("node merge receipt not found")
)

type MergeSourceRef struct {
	ID      string   `json:"id"`
	Slug    string   `json:"slug"`
	Version int32    `json:"version"`
	Stage   string   `json:"stage"`
	Role    SpecRole `json:"role"`
}

type MergeRelationRef struct {
	FromSlug string `json:"fromSlug"`
	ToSlug   string `json:"toSlug"`
	Type     string `json:"type"`
	ChangeID string `json:"changeId"`
}

type MergeAncestorRef struct {
	GoalSlug   string            `json:"goalSlug"`
	References SummaryReferences `json:"references"`
}

type MergePreview struct {
	Sources   []MergeSourceRef   `json:"sources"`
	Contexts  []MergeSourceRef   `json:"contexts"`
	Relations []MergeRelationRef `json:"relations"`
	Lineage   []MergeRelationRef `json:"lineage"`
	Ancestors []MergeAncestorRef `json:"ancestors"`
}

type MergeTarget struct {
	Slug          string         `json:"slug"`
	Intent        string         `json:"intent"`
	Role          SpecRole       `json:"role"`
	Priority      SpecPriority   `json:"priority"`
	Complexity    SpecComplexity `json:"complexity"`
	Notes         string         `json:"notes,omitempty"`
	SparkOutput   *SparkOutput   `json:"sparkOutput,omitempty"`
	ShapeOutput   *ShapeOutput   `json:"shapeOutput,omitempty"`
	SpecifyOutput *SpecifyOutput `json:"specifyOutput,omitempty"`
}

type MergeRelationDisposition struct {
	Before       MergeRelationRef   `json:"before"`
	Action       string             `json:"action"`
	Replacements []MergeRelationRef `json:"replacements,omitempty"`
}

type MergeRequest struct {
	Expected       MergePreview                `json:"expected"`
	Target         MergeTarget                 `json:"target"`
	Relations      []MergeRelationDisposition  `json:"relations"`
	AddedRelations []MergeRelationRef          `json:"addedRelations"`
	Dispositions   []SummaryDispositionRequest `json:"dispositions"`
	Reason         string                      `json:"reason"`
	IdempotencyKey string                      `json:"idempotencyKey"`
}

type MergeSourceResult struct {
	ID            string `json:"id"`
	Slug          string `json:"slug"`
	BeforeVersion int32  `json:"beforeVersion"`
	AfterVersion  int32  `json:"afterVersion"`
}

type MergeTargetResult struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
}

type MergeReceipt struct {
	ID                string              `json:"id"`
	IdempotencyKey    string              `json:"idempotencyKey"`
	Request           MergeRequest        `json:"request"`
	ActorUserID       string              `json:"actorUserId"`
	ActorRunID        *string             `json:"actorRunId,omitempty"`
	Sources           []MergeSourceResult `json:"sources"`
	Target            MergeTargetResult   `json:"target"`
	InsertedRelations []MergeRelationRef  `json:"insertedRelations"`
	DeletedRelations  []MergeRelationRef  `json:"deletedRelations"`
	DispositionIDs    []string            `json:"dispositionIds"`
	CreatedAt         time.Time           `json:"createdAt"`
	Replayed          bool                `json:"replayed"`
}

type MergeHistory struct {
	Items      []MergeReceipt `json:"items"`
	HasMore    bool           `json:"hasMore"`
	NextCursor string         `json:"nextCursor"`
}
