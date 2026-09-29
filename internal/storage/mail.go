// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import (
	"errors"
	"time"
)

var (
	// ErrMailInvalid rejects malformed mail content, references or pagination input.
	ErrMailInvalid   = errors.New("invalid mail request")
	ErrMailNotFound  = errors.New("mail not found")
	ErrMailForbidden = errors.New("run is not a mail participant or recipient")
	ErrMailConflict  = errors.New("mail idempotency or thread context conflict")
	ErrMailClosed    = errors.New("mail thread is closed")
)

// SendMailRequest preserves raw text. Limits are bytes: body/resolution 64 KiB,
// subject 512, task/run/thread IDs and idempotency key 256; 1..32 recipients.
// ThreadID empty opens a thread; continuation repeats its task and subject exactly.
// Up to eight distinct references, at most one reply, with 64 KiB total source bodies.
// The caller must authenticate and authorize the run principal beforehand:
// these storage methods enforce membership, not identity authentication.
type SendMailRequest struct {
	ThreadID        string
	TaskSlug        string
	Subject         string
	Body            string
	RecipientRunIDs []string
	IdempotencyKey  string
	References      []MailReferenceRequest
	HandoffToRun    string // Internal handoff intent; never decoded by ordinary send.
}

// MailReferenceRequest selects immutable source content, never caller-authored quotes.
type MailReferenceRequest struct {
	MessageID string `json:"message_id"`
	Kind      string `json:"kind"`
}

// MailReference preserves immutable source-message content selected for a reply or forward.
type MailReference struct {
	MessageID   string    `json:"message_id"`
	Kind        string    `json:"kind"`
	ThreadID    string    `json:"thread_id"`
	TaskSlug    string    `json:"task_slug"`
	SenderRunID string    `json:"sender_run_id"`
	Subject     string    `json:"subject"`
	Body        string    `json:"body"`
	CreatedAt   time.Time `json:"created_at"`
}

// MailThread records the task conversation, current owner and any explicit closure.
type MailThread struct {
	ID          string
	TaskSlug    string
	OpenedByRun string
	OwnerRun    string
	Subject     string
	CreatedAt   time.Time
	ClosedAt    *time.Time
	ClosedByRun *string
	ClosureNote *string
}

// MailMessage preserves sender-authored text, recipient deliveries and immutable source references.
type MailMessage struct {
	ID              string
	ThreadID        string
	SenderRun       string
	IdempotencyKey  string
	Body            string
	CreatedAt       time.Time
	RecipientRunIDs []string
	References      []MailReference
	HandoffToRun    *string
}

// MailReceipt is for the requesting recipient only. Reading never changes it;
// marking read and acknowledging are separate, timestamp-preserving operations.
type MailReceipt struct {
	ReadAt         *time.Time
	AcknowledgedAt *time.Time
}

// MailItem pairs a message with the requesting recipient's receipt, absent when that run is not a recipient.
type MailItem struct {
	Message MailMessage
	Receipt *MailReceipt
}

// SentMail returns the thread and stored message after an authorized send or idempotent replay.
type SentMail struct {
	Thread  MailThread
	Message MailMessage
}

// MailPage uses ascending message-ID keyset pagination. Limit must be 1..100;
// pass NextCursor to the next call, empty means exhausted at query time.
// Pages are live views, not a snapshot across requests.
// NextCursor is for browsing, never a durable delivery offset: concurrent sends
// can commit older IDs. Inbox consumers restart at the first page after ack;
// unacknowledged receipt state, not the cursor, determines pending delivery.
type MailPage struct {
	Items      []MailItem
	NextCursor string
}

// MailThreadPage returns a participant's thread view with paginated messages and their own receipts.
type MailThreadPage struct {
	Thread MailThread
	MailPage
}

// MailThreadsPage returns filtered ownership or retirement views with a browsing continuation cursor.
type MailThreadsPage struct {
	Threads    []MailThread
	NextCursor string
}

// TakeoverMailRequest identifies an operator-directed owner transfer and its notification to the new run.
type TakeoverMailRequest struct {
	ThreadID       string `json:"mail_thread_id"`
	RecipientRunID string `json:"recipient_run_id"`
	Body           string `json:"body"`
	IdempotencyKey string `json:"idempotency_key"`
}

// MailOwnerEvent records an explicit ownership transfer with its actor and reason, not inferred responsibility.
type MailOwnerEvent struct {
	ID          string    `json:"id"`
	ThreadID    string    `json:"thread_id"`
	FromRun     string    `json:"from_run"`
	ToRun       string    `json:"to_run"`
	ActorKind   string    `json:"actor_kind"`
	ActorUserID string    `json:"actor_user_id"`
	ActorRunID  *string   `json:"actor_run_id"`
	Reason      string    `json:"reason"`
	CreatedAt   time.Time `json:"created_at"`
}

// MailOwnerEventsPage returns recorded owner transfers with a history continuation cursor.
type MailOwnerEventsPage struct {
	Events     []MailOwnerEvent `json:"events"`
	NextCursor string           `json:"next_cursor"`
}

// TakenOverMail returns the updated thread and recorded owner-transfer event.
type TakenOverMail struct {
	Thread MailThread
	Event  MailOwnerEvent
}
