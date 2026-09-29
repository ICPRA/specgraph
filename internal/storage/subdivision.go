// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"encoding/json"
	"time"
)

// ChildSpecDraft is new authored work, not inherited execution or acceptance.
type ChildSpecDraft struct {
	Slug       string `json:"slug"`
	Intent     string `json:"intent"`
	Priority   string `json:"priority"`
	Complexity string `json:"complexity"`
}

// SubdivideRequest carries the parent version and proposed new work.
type SubdivideRequest struct {
	ExpectedVersion int32            `json:"expectedVersion"`
	Reason          string           `json:"reason"`
	Children        []ChildSpecDraft `json:"children"`
}

// SubdivisionResult records creation, not approval or completion.
type SubdivisionResult struct {
	ID            string   `json:"id"`
	Parent        string   `json:"parent"`
	ParentVersion int32    `json:"parentVersion"`
	ChildSlugs    []string `json:"childSlugs"`
}

// SubdivisionState separates the recorded source identity from current scope.
type SubdivisionState struct {
	Receipt         SubdivisionResult          `json:"receipt"`
	SourceReference SubdivisionSourceReference `json:"sourceReference"`
	Parent          *Spec                      `json:"parent"`
	ParentScope     ScopeContext               `json:"parentScope"`
	Children        []SubdivisionChildState    `json:"children"`
}

// SubdivisionChildState returns a created child's current spec, source scope and attachment to the parent.
type SubdivisionChildState struct {
	Spec     *Spec        `json:"spec"`
	Scope    ScopeContext `json:"scope"`
	Attached bool         `json:"attached"`
}

// SubdivisionSourceReference identifies the parent revision used by the recorded subdivision operation.
type SubdivisionSourceReference struct {
	ID      string `json:"id"`
	Slug    string `json:"slug"`
	Version int32  `json:"version"`
}

// SpecScopeBaseline is observed content, not approval or inherited constraints.
type SpecScopeBaseline struct {
	ID       string          `json:"id"`
	Slug     string          `json:"slug"`
	Revision string          `json:"revision"`
	Contract json.RawMessage `json:"contract"`
}

// ScopeContext preserves source boundaries rather than merging inherited text.
type ScopeContext struct {
	Sources   []SpecScopeBaseline `json:"sources"`
	Relations []ScopeRelation     `json:"relations"`
	Decisions []*Decision         `json:"decisions"`
}

// ScopeRelation preserves a graph edge between scope sources without merging their contracts.
type ScopeRelation struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

// SubdivisionReference identifies a recorded subdivision and its initiating actor and reason.
type SubdivisionReference struct {
	ID        string    `json:"id"`
	Parent    string    `json:"parent"`
	Actor     string    `json:"actor"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"createdAt"`
}

// SubdivisionList returns recorded operations and whether more matching history exists.
type SubdivisionList struct {
	Items   []SubdivisionReference `json:"items"`
	HasMore bool                   `json:"hasMore"`
}
