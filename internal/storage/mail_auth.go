// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "time"

// MailScope is the explicitly approved provider session, not a caller-selected run.
type MailScope struct {
	EnvironmentID      string `json:"environment_id"`
	ThreadID           string `json:"thread_id"`
	ProviderSessionID  string `json:"provider_session_id"`
	ProviderInstanceID string `json:"provider_instance_id"`
}

// MailGrant records a human-approved bridge subject's environment access and any explicit revocation.
type MailGrant struct {
	ID            string     `json:"id"`
	Project       string     `json:"project"`
	EnvironmentID string     `json:"environment_id"`
	BridgeSubject string     `json:"bridge_subject"`
	GrantedBy     string     `json:"granted_by"`
	GrantedAt     time.Time  `json:"granted_at"`
	RevokedBy     *string    `json:"revoked_by"`
	RevokedAt     *time.Time `json:"revoked_at"`
}

// MailBinding ties an approved provider session to one run under a recorded grant, preserving revocation history.
type MailBinding struct {
	ID      string `json:"id"`
	GrantID string `json:"grant_id"`
	Project string `json:"project"`
	MailScope
	RunID      string     `json:"run_id"`
	ApprovedBy string     `json:"approved_by"`
	ApprovedAt time.Time  `json:"approved_at"`
	RevokedBy  *string    `json:"revoked_by"`
	RevokedAt  *time.Time `json:"revoked_at"`
}

// MailActor is the run and task resolved from a verified bridge identity and approved session binding.
type MailActor struct {
	RunID    string
	TaskSlug string
}

// MailContact is an enrolled project contact, not an online or unique role owner.
type MailContact struct {
	RunID          string  `json:"run_id"`
	TaskSlug       string  `json:"task_slug"`
	AssignmentRole *string `json:"assignment_role"`
}

// MailDirectoryPage returns enrolled project contacts with a browsing continuation cursor, not presence status.
type MailDirectoryPage struct {
	Contacts   []MailContact `json:"contacts"`
	NextCursor string        `json:"next_cursor"`
}
