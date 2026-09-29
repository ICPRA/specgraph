// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"errors"
	"time"
)

// ErrManualCompletionConflict indicates an already completed spec or a reused key with different input.
var ErrManualCompletionConflict = errors.New("manual completion conflicts with existing state")

// ManualCompletion is an operator assertion, not execution or verification evidence.
type ManualCompletion struct {
	ID             string    `json:"id"`
	Slug           string    `json:"slug"`
	Actor          string    `json:"actor"`
	SourceVersion  int32     `json:"sourceVersion"`
	ResultVersion  int32     `json:"resultVersion"`
	Note           string    `json:"note"`
	IdempotencyKey string    `json:"idempotencyKey"`
	CreatedAt      time.Time `json:"createdAt"`
}
