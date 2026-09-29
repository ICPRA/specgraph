// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// TestReportFields records execution assertions and references, never copied tool output.
type TestReportFields struct {
	DeliveryID  string         `json:"deliveryId"`
	CommitSHA   string         `json:"commitSha"`
	PlanSources []ReviewSource `json:"planSources"`
	Status      string         `json:"status"`
	Command     string         `json:"command"`
	ExitCode    *int64         `json:"exitCode"`
	Summary     string         `json:"summary"`
	OutputRefs  []string       `json:"outputRefs"`
}

// RecordTestReportRequest submits execution assertions for a fixed delivery and plan, optionally from a test run.
type RecordTestReportRequest struct {
	TestReportFields
	TestRunID *string `json:"testRunId,omitempty"`
}

// TestReport preserves a reporter's recorded assertions and source references for one delivery; it is not tool output.
type TestReport struct {
	ID string `json:"id"`
	TestReportFields
	TestRunID *string   `json:"testRunId"`
	Reporter  string    `json:"reporter"`
	CreatedAt time.Time `json:"createdAt"`
}

// TestReportPage returns a delivery's recorded test assertions with a history continuation cursor.
type TestReportPage struct {
	DeliveryID string       `json:"deliveryId"`
	Reports    []TestReport `json:"reports"`
	HasMore    bool         `json:"hasMore"`
	NextCursor *string      `json:"nextCursor"`
}
