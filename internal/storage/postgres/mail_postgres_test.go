// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
)

func TestMailInputLimits(t *testing.T) {
	for _, value := range []string{"", " \n", "a\x00b", string([]byte{0xff}), strings.Repeat("x", 257)} {
		if validMailText(value, 256) {
			t.Errorf("accepted invalid text of length %d", len(value))
		}
	}
	if !validMailText(strings.Repeat("x", 65536), 65536) || validMailText(strings.Repeat("x", 65537), 65536) {
		t.Fatal("body byte limit")
	}
	for _, limit := range []int{-1, 0, 101} {
		if validMailPage(limit, "") {
			t.Errorf("accepted page limit %d", limit)
		}
	}
	if !validMailPage(1, "") || !validMailPage(100, "msg-cursor") {
		t.Fatal("valid page rejected")
	}
	store := &Store{}
	base := storage.SendMailRequest{TaskSlug: "task", Subject: "subject", Body: "body", RecipientRunIDs: []string{"receiver"}, IdempotencyKey: "key"}
	for name, change := range map[string]func(*storage.SendMailRequest){
		"body":               func(r *storage.SendMailRequest) { r.Body = strings.Repeat("x", 65537) },
		"subject":            func(r *storage.SendMailRequest) { r.Subject = strings.Repeat("x", 513) },
		"key":                func(r *storage.SendMailRequest) { r.IdempotencyKey = "" },
		"recipients":         func(r *storage.SendMailRequest) { r.RecipientRunIDs = slices.Repeat([]string{"receiver"}, 33) },
		"empty recipients":   func(r *storage.SendMailRequest) { r.RecipientRunIDs = nil },
		"handoff new thread": func(r *storage.SendMailRequest) { r.HandoffToRun = "receiver" },
		"handoff self": func(r *storage.SendMailRequest) {
			r.ThreadID, r.HandoffToRun, r.RecipientRunIDs = "thread", "sender", []string{"sender"}
		},
		"handoff mismatched recipient": func(r *storage.SendMailRequest) {
			r.ThreadID, r.HandoffToRun = "thread", "other"
		},
		"handoff multiple recipients": func(r *storage.SendMailRequest) {
			r.ThreadID, r.HandoffToRun, r.RecipientRunIDs = "thread", "receiver", []string{"receiver", "other"}
		},
		"reference count": func(r *storage.SendMailRequest) { r.References = make([]storage.MailReferenceRequest, 9) },
		"reference id": func(r *storage.SendMailRequest) {
			r.References = []storage.MailReferenceRequest{{MessageID: "", Kind: "reply"}}
		},
		"reference kind": func(r *storage.SendMailRequest) {
			r.References = []storage.MailReferenceRequest{{MessageID: "source", Kind: "quote"}}
		},
		"duplicate source": func(r *storage.SendMailRequest) {
			r.References = []storage.MailReferenceRequest{{MessageID: "source", Kind: "reply"}, {MessageID: "source", Kind: "forward"}}
		},
		"two replies": func(r *storage.SendMailRequest) {
			r.References = []storage.MailReferenceRequest{{MessageID: "one", Kind: "reply"}, {MessageID: "two", Kind: "reply"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			change(&request)
			if _, err := store.SendMail(context.Background(), "sender", &request); !errors.Is(err, storage.ErrMailInvalid) {
				t.Fatalf("invalid request reached database: %v", err)
			}
		})
	}
}
