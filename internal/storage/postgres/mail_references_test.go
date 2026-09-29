// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestMailReferences(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("mail-refs"))
	_, err := s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES('mail-refs','task');
		INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES
		('mail-refs','refs-a','task'),('mail-refs','refs-b','task'),('mail-refs','refs-c','task'),('mail-refs','refs-d','task'),('mail-refs','refs-e','task')`)
	require.NoError(t, err)
	base := storage.SendMailRequest{TaskSlug: "task", Subject: "Original", Body: "  original source\n", RecipientRunIDs: []string{"refs-b"}, IdempotencyKey: "original"}
	original, err := s.SendMail(ctx, "refs-a", &base)
	require.NoError(t, err)
	require.NotNil(t, original.Message.References)
	require.Empty(t, original.Message.References)
	forward := storage.SendMailRequest{TaskSlug: "task", Subject: "Selected context", Body: "My explanation", RecipientRunIDs: []string{"refs-c", "refs-d"}, IdempotencyKey: "forward",
		References: []storage.MailReferenceRequest{{MessageID: original.Message.ID, Kind: "forward"}}}
	sent, err := s.SendMail(ctx, "refs-b", &forward)
	require.NoError(t, err)
	require.NotEqual(t, original.Thread.ID, sent.Thread.ID)
	want := storage.MailReference{MessageID: original.Message.ID, Kind: "forward", ThreadID: original.Thread.ID, TaskSlug: "task", SenderRunID: "refs-a", Subject: base.Subject, Body: base.Body, CreatedAt: original.Message.CreatedAt}
	require.Len(t, sent.Message.References, 1)
	require.True(t, want.CreatedAt.Equal(sent.Message.References[0].CreatedAt))
	want.CreatedAt = sent.Message.References[0].CreatedAt
	require.Equal(t, []storage.MailReference{want}, sent.Message.References)
	for _, run := range []string{"refs-c", "refs-d"} {
		inbox, inboxErr := s.Inbox(ctx, run, 10, "")
		require.NoError(t, inboxErr)
		require.Len(t, inbox.Items, 1)
		require.Equal(t, sent.Message.References, inbox.Items[0].Message.References)
		require.NotNil(t, inbox.Items[0].Receipt)
		_, readErr := s.ReadMailThread(ctx, run, original.Thread.ID, 10, "")
		require.ErrorIs(t, readErr, storage.ErrMailForbidden)
	}
	_, err = s.AcknowledgeMail(ctx, "refs-c", sent.Message.ID)
	require.NoError(t, err)
	pending, err := s.Inbox(ctx, "refs-d", 10, "")
	require.NoError(t, err)
	require.Len(t, pending.Items, 1)
	require.Nil(t, pending.Items[0].Receipt.AcknowledgedAt)
	history, err := s.ReadMailThread(ctx, "refs-c", sent.Thread.ID, 10, "")
	require.NoError(t, err)
	require.Equal(t, sent.Message.References, history.Items[0].Message.References)
	retry, err := s.SendMail(ctx, "refs-b", &forward)
	require.NoError(t, err)
	require.Equal(t, sent.Message.ID, retry.Message.ID)
	changed := forward
	changed.References = []storage.MailReferenceRequest{{MessageID: original.Message.ID, Kind: "reply"}}
	_, err = s.SendMail(ctx, "refs-b", &changed)
	require.ErrorIs(t, err, storage.ErrMailConflict)
	changed.References = nil
	_, err = s.SendMail(ctx, "refs-b", &changed)
	require.ErrorIs(t, err, storage.ErrMailConflict)

	reply := forward
	reply.ThreadID, reply.IdempotencyKey = sent.Thread.ID, "reply"
	reply.RecipientRunIDs = []string{"refs-b"}
	reply.References = []storage.MailReferenceRequest{{MessageID: sent.Message.ID, Kind: "reply"}}
	replied, err := s.SendMail(ctx, "refs-c", &reply)
	require.NoError(t, err)
	require.Len(t, replied.Message.References, 1)
	require.Equal(t, sent.Message.Body, replied.Message.References[0].Body)
	require.Equal(t, "refs-b", replied.Message.References[0].SenderRunID)
	require.Equal(t, "reply", replied.Message.References[0].Kind)
	require.NotContains(t, replied.Message.References[0].Body, base.Body)
	reshare := forward
	reshare.IdempotencyKey, reshare.RecipientRunIDs = "reshare", []string{"refs-d"}
	reshared, err := s.SendMail(ctx, "refs-c", &reshare)
	require.NoError(t, err)
	require.Equal(t, []storage.MailReference{want}, reshared.Message.References)
	_, err = s.ReadMailThread(ctx, "refs-c", original.Thread.ID, 10, "")
	require.ErrorIs(t, err, storage.ErrMailForbidden)
	unshared := base
	unshared.ThreadID, unshared.IdempotencyKey = original.Thread.ID, "unshared"
	private, err := s.SendMail(ctx, "refs-a", &unshared)
	require.NoError(t, err)
	reshare.IdempotencyKey = "unshared-source"
	reshare.References = []storage.MailReferenceRequest{{MessageID: private.Message.ID, Kind: "context"}}
	_, err = s.SendMail(ctx, "refs-c", &reshare)
	require.ErrorIs(t, err, storage.ErrMailForbidden)

	// Permission and aggregate checks precede all writes even in a caller-owned transaction.
	var before, after int
	countSQL := `SELECT (SELECT count(*) FROM mail_threads)+(SELECT count(*) FROM mail_messages)+(SELECT count(*) FROM mail_recipients)+(SELECT count(*) FROM mail_references)`
	require.NoError(t, s.Pool().QueryRow(ctx, countSQL).Scan(&before))
	unauthorized := forward
	unauthorized.IdempotencyKey = "unauthorized"
	require.NoError(t, s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, sendErr := s.SendMail(txCtx, "refs-e", &unauthorized)
		require.ErrorIs(t, sendErr, storage.ErrMailForbidden)
		return nil
	}))
	require.NoError(t, s.Pool().QueryRow(ctx, countSQL).Scan(&after))
	require.Equal(t, before, after)
	foreign := newStore(t, postgres.WithProject("mail-refs-foreign"))
	_, err = foreign.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES('mail-refs-foreign','task');
		INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES('mail-refs-foreign','refs-foreign','task')`)
	require.NoError(t, err)
	cross := forward
	cross.RecipientRunIDs = []string{"refs-foreign"}
	_, err = foreign.SendMail(ctx, "refs-foreign", &cross)
	require.ErrorIs(t, err, storage.ErrMailNotFound)

	contextRequest := forward
	contextRequest.IdempotencyKey = "context"
	contextRequest.References = []storage.MailReferenceRequest{{MessageID: sent.Message.ID, Kind: "reply"}, {MessageID: original.Message.ID, Kind: "context"}}
	contextMail, err := s.SendMail(ctx, "refs-b", &contextRequest)
	require.NoError(t, err)
	require.Equal(t, []storage.MailReferenceRequest{{MessageID: sent.Message.ID, Kind: "reply"}, {MessageID: original.Message.ID, Kind: "context"}}, contextRequest.References, "send must preserve the caller's reference order")
	slices.Reverse(contextRequest.References)
	contextRetry, err := s.SendMail(ctx, "refs-b", &contextRequest)
	require.NoError(t, err)
	require.Equal(t, contextMail.Message.ID, contextRetry.Message.ID)
	maximum := forward
	maximum.IdempotencyKey = "eight-sources"
	maximum.References = []storage.MailReferenceRequest{{MessageID: original.Message.ID, Kind: "reply"}, {MessageID: private.Message.ID, Kind: "context"}}
	for i := range 6 {
		source := base
		source.IdempotencyKey = "extra-" + string(rune('a'+i))
		extra, sendErr := s.SendMail(ctx, "refs-a", &source)
		require.NoError(t, sendErr)
		maximum.References = append(maximum.References, storage.MailReferenceRequest{MessageID: extra.Message.ID, Kind: "context"})
	}
	maxMail, err := s.SendMail(ctx, "refs-b", &maximum)
	require.NoError(t, err)
	require.Len(t, maxMail.Message.References, 8)

	large := base
	large.Body, large.IdempotencyKey = strings.Repeat("x", 65536), "large"
	largeMail, err := s.SendMail(ctx, "refs-a", &large)
	require.NoError(t, err)
	boundary := forward
	boundary.IdempotencyKey = "boundary"
	boundary.References = []storage.MailReferenceRequest{{MessageID: largeMail.Message.ID, Kind: "context"}}
	_, err = s.SendMail(ctx, "refs-b", &boundary)
	require.NoError(t, err)
	boundary.IdempotencyKey = "overflow"
	boundary.References = append(boundary.References, storage.MailReferenceRequest{MessageID: original.Message.ID, Kind: "context"})
	require.NoError(t, s.Pool().QueryRow(ctx, countSQL).Scan(&before))
	_, err = s.SendMail(ctx, "refs-b", &boundary)
	require.ErrorIs(t, err, storage.ErrMailInvalid)
	require.NoError(t, s.Pool().QueryRow(ctx, countSQL).Scan(&after))
	require.Equal(t, before, after)

	rollback := errors.New("rollback mail and references")
	rolled := forward
	rolled.IdempotencyKey = "rollback"
	require.ErrorIs(t, s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, sendErr := s.SendMail(txCtx, "refs-b", &rolled)
		require.NoError(t, sendErr)
		return rollback
	}), rollback)
	require.NoError(t, s.Pool().QueryRow(ctx, countSQL).Scan(&after))
	require.Equal(t, before, after)
}
