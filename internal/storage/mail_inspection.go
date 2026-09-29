// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// MailNodeLink counts actual message deliveries between task nodes, including self-links.
type MailNodeLink struct {
	SenderTaskSlug    string `json:"sender_task_slug"`
	RecipientTaskSlug string `json:"recipient_task_slug"`
	MessageCount      int64  `json:"message_count"`
	PendingAckCount   int64  `json:"pending_ack_count"`
}

// MailThreadSummary is a project-scoped operator view, not a run inbox.
type MailThreadSummary struct {
	MailThread
	MessageCount      int64
	PendingAckCount   int64
	LastMessageAt     *time.Time
	ParticipantRunIDs []string
	NodeLinks         []MailNodeLink
}

// MailInspectionThreads returns project-scoped operator thread summaries and a browsing continuation cursor.
type MailInspectionThreads struct {
	Threads    []MailThreadSummary
	NextCursor string
}

// MailInspectionReceipt exposes one recipient's recorded read and acknowledgment times to an authorized operator.
type MailInspectionReceipt struct {
	RecipientRunID string     `json:"recipient_run_id"`
	ReadAt         *time.Time `json:"read_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at"`
}

// MailInspectionItem combines a stored message with all recipient receipts for operator inspection.
type MailInspectionItem struct {
	Message  MailMessage
	Receipts []MailInspectionReceipt
}

// MailInspectionPage returns an operator's thread history with all receipts and a browsing continuation cursor.
type MailInspectionPage struct {
	Thread     MailThread
	Items      []MailInspectionItem
	NextCursor string
}
