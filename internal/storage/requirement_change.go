// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

// RequirementChangePreview reports structural candidates, not semantic impact or withdrawal.
type RequirementChangePreview struct {
	SpecSlug  string             `json:"specSlug"`
	Scope     ScopeContext       `json:"scope"`
	Nodes     []ChangeImpactNode `json:"nodes"`
	Relations []ScopeRelation    `json:"relations"`
}

// ChangeImpactNode preserves all direct Spec parents, including unaffected goals.
type ChangeImpactNode struct {
	ID            string   `json:"id"`
	Slug          string   `json:"slug"`
	Intent        string   `json:"intent"`
	Stage         string   `json:"stage"`
	Role          string   `json:"role"`
	Version       int32    `json:"version"`
	ScopeRevision string   `json:"scopeRevision"`
	ParentSlugs   []string `json:"parentSlugs"`
}
