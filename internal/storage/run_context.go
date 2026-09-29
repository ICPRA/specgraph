// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"encoding/json"
	"time"
)

// RunContext preserves the prepared package body for a run, independently of admission and completion.
type RunContext struct {
	RunID     string          `json:"runId"`
	TaskSlug  string          `json:"taskSlug"`
	PackageID string          `json:"packageId"`
	Body      json.RawMessage `json:"body"`
	CreatedAt time.Time       `json:"createdAt"`
}
