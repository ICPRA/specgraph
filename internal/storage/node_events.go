// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// RecordNodeEventRequest supplies an operator's retry or rework occurrence with explicit node-related references.
type RecordNodeEventRequest struct {
	EventID       string       `json:"eventId"`
	Kind          string       `json:"kind"`
	Reason        string       `json:"reason"`
	PreviousRunID *string      `json:"previousRunId,omitempty"`
	RunID         *string      `json:"runId,omitempty"`
	DeliveryID    *string      `json:"deliveryId,omitempty"`
	GitUndo       *NodeGitUndo `json:"gitUndo,omitempty"`
}

// NodeGitUndo records declared Git references; it does not run or verify an undo.
type NodeGitUndo struct {
	Operation    string `json:"operation"`
	SourceCommit string `json:"sourceCommit"`
	ResultCommit string `json:"resultCommit"`
}

// RecordOwnNodeEventRequest can only describe an event of the trusted bound run's task.
type RecordOwnNodeEventRequest struct {
	EventID       string       `json:"eventId"`
	Kind          string       `json:"kind"`
	Reason        string       `json:"reason"`
	PreviousRunID *string      `json:"previousRunId,omitempty"`
	DeliveryID    *string      `json:"deliveryId,omitempty"`
	GitUndo       *NodeGitUndo `json:"gitUndo,omitempty"`
	ExpectedRunID string       `json:"expectedRunId,omitempty"`
}

// NodeExecutionEvent is an operator-recorded occurrence, not inferred execution proof.
type NodeExecutionEvent struct {
	ID            string       `json:"id"`
	TaskSlug      string       `json:"taskSlug"`
	Kind          string       `json:"kind"`
	Reason        string       `json:"reason"`
	Actor         string       `json:"actor"`
	PreviousRunID *string      `json:"previousRunId"`
	RunID         *string      `json:"runId"`
	DeliveryID    *string      `json:"deliveryId"`
	GitUndo       *NodeGitUndo `json:"gitUndo,omitempty"`
	RecordedAt    time.Time    `json:"recordedAt"`
}

// NodeEventPage returns a task's recorded operator events with a history continuation cursor.
type NodeEventPage struct {
	TaskSlug   string               `json:"taskSlug"`
	Events     []NodeExecutionEvent `json:"events"`
	HasMore    bool                 `json:"hasMore"`
	NextCursor *string              `json:"nextCursor"`
}

// NodeEventCount totals recorded retry and rework events for a task, not inferred execution attempts.
type NodeEventCount struct {
	TaskSlug string `json:"taskSlug" db:"task_slug"`
	Retries  int64  `json:"retries"`
	Reworks  int64  `json:"reworks"`
	GitUndos int64  `json:"gitUndos" db:"git_undos"`
}
