// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// DependencyEditRequest describes an operator's dependency command against a read baseline.
type DependencyEditRequest struct {
	Prerequisite                string
	ExpectedVersion             int32
	ExpectedPrerequisiteVersion int32
	ExpectedRevision            int64
	Reason                      string
	IdempotencyKey              string
}

// DependencyEditResult distinguishes a topology change from an acknowledged no-op.
type DependencyEditResult struct {
	Operation    string `json:"operation"`
	Dependent    string `json:"dependent"`
	Prerequisite string `json:"prerequisite"`
	Revision     int64  `json:"revision,string"`
	Changed      bool   `json:"changed"`
	Replayed     bool   `json:"replayed"`
}

// DependencyEditRecord is an operator command receipt, not an execution result.
type DependencyEditRecord struct {
	ID           string    `json:"id"`
	Operation    string    `json:"operation"`
	Actor        string    `json:"actor"`
	Reason       string    `json:"reason"`
	Prerequisite string    `json:"prerequisite"`
	Revision     int64     `json:"revision,string"`
	Changed      bool      `json:"changed"`
	CreatedAt    time.Time `json:"createdAt"`
}

// DependencyEditState is a coherent baseline and the latest 20 operator receipts.
type DependencyEditState struct {
	SpecVersion int32                  `json:"specVersion"`
	Revision    int64                  `json:"revision,string"`
	Operations  []DependencyEditRecord `json:"operations"`
}
