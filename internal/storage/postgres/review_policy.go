// SPDX-License-Identifier: MIT
// Copyright (c) 2025 Paperclip AI
// Go adaptation: Copyright 2026 Sean Brandt

package postgres

// Adapted from issue-execution-policy.ts at Paperclip commit
// 7b7c4d4172d6aac14919e2682b702ae87bc17653. See PAPERCLIP-NOTICE.
// The caller admits only a current assigned decision and enforces an existing hold.
func reviewRoundsAfterDecision(actorKind, verdict string, rounds, maximum int32) (int32, bool) {
	if actorKind == "human" || verdict == "accepted" {
		return 0, false
	}
	next := rounds + 1
	return next, next >= maximum
}

const defaultMaxReviewRounds int32 = 3
