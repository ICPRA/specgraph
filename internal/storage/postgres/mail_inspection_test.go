// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestMailInspection(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, postgres.WithProject("mail-inspection"))
	other := newStore(t, postgres.WithProject("mail-inspection-other"))
	_, err := s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES ('mail-inspection','task'),('mail-inspection','other-task');
		INSERT INTO specs(project_slug,slug) VALUES ('mail-inspection','recipient-task'),('mail-inspection','third-task'),('mail-inspection','unrelated-task');
		INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES
		('mail-inspection','inspect-a','task'),('mail-inspection','inspect-b','recipient-task'),('mail-inspection','inspect-c','third-task')`)
	require.NoError(t, err)
	base := storage.SendMailRequest{TaskSlug: "task", Subject: "Review", Body: "source", RecipientRunIDs: []string{"inspect-b", "inspect-c"}, IdempotencyKey: "first"}
	first, err := s.SendMail(ctx, "inspect-a", &base)
	require.NoError(t, err)
	base.ThreadID, base.IdempotencyKey, base.Body = first.Thread.ID, "second", "reply"
	base.References = []storage.MailReferenceRequest{{MessageID: first.Message.ID, Kind: "reply"}}
	base.RecipientRunIDs = []string{"inspect-a", "inspect-c"}
	second, err := s.SendMail(ctx, "inspect-b", &base)
	require.NoError(t, err)
	_, err = s.MarkMailRead(ctx, "inspect-b", first.Message.ID)
	require.NoError(t, err)
	_, err = s.AcknowledgeMail(ctx, "inspect-c", second.Message.ID)
	require.NoError(t, err)
	base.ThreadID, base.IdempotencyKey, base.TaskSlug, base.References = "", "closed", "other-task", nil
	base.RecipientRunIDs = []string{"inspect-b", "inspect-c"}
	closed, err := s.SendMail(ctx, "inspect-a", &base)
	require.NoError(t, err)
	_, err = s.CloseMailThread(ctx, "inspect-a", closed.Thread.ID, "resolved")
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='handed_off' WHERE project_slug='mail-inspection'`)
	require.NoError(t, err)

	// Compare complete mail-table contents, not hashes or row counts alone.
	const snapshot = `SELECT jsonb_build_array(
	 (SELECT jsonb_agg(to_jsonb(t) ORDER BY project_slug,id) FROM mail_threads t),
	 (SELECT jsonb_agg(to_jsonb(m) ORDER BY project_slug,id) FROM mail_messages m),
	 (SELECT jsonb_agg(to_jsonb(r) ORDER BY project_slug,message_id,recipient_run) FROM mail_recipients r),
	 (SELECT jsonb_agg(to_jsonb(r) ORDER BY project_slug,message_id,source_message_id) FROM mail_references r))::text`
	var before, after string
	require.NoError(t, s.Pool().QueryRow(ctx, snapshot).Scan(&before))
	open, err := s.InspectMailThreads(ctx, "", "open", 50, "", false)
	require.NoError(t, err)
	require.Len(t, open.Threads, 1)
	require.Equal(t, first.Thread.ID, open.Threads[0].ID)
	require.EqualValues(t, 2, open.Threads[0].MessageCount)
	require.EqualValues(t, 3, open.Threads[0].PendingAckCount)
	require.Equal(t, []string{"inspect-a", "inspect-b", "inspect-c"}, open.Threads[0].ParticipantRunIDs)
	require.NotNil(t, open.Threads[0].LastMessageAt)
	require.Equal(t, []storage.MailNodeLink{
		{SenderTaskSlug: "recipient-task", RecipientTaskSlug: "task", MessageCount: 1, PendingAckCount: 1},
		{SenderTaskSlug: "recipient-task", RecipientTaskSlug: "third-task", MessageCount: 1, PendingAckCount: 0},
		{SenderTaskSlug: "task", RecipientTaskSlug: "recipient-task", MessageCount: 1, PendingAckCount: 1},
		{SenderTaskSlug: "task", RecipientTaskSlug: "third-task", MessageCount: 1, PendingAckCount: 1},
	}, open.Threads[0].NodeLinks, "count delivered recipients, not a participant clique or distinct messages")
	all, err := s.InspectMailThreads(ctx, "", "all", 1, "", false)
	require.NoError(t, err)
	require.Equal(t, closed.Thread.ID, all.Threads[0].ID)
	require.Equal(t, []storage.MailNodeLink{
		{SenderTaskSlug: "task", RecipientTaskSlug: "recipient-task", MessageCount: 1, PendingAckCount: 1},
		{SenderTaskSlug: "task", RecipientTaskSlug: "third-task", MessageCount: 1, PendingAckCount: 1},
	}, all.Threads[0].NodeLinks, "closed thread links retain the actual sender node, not the filing task")
	require.Equal(t, closed.Thread.ID, all.NextCursor)
	next, err := s.InspectMailThreads(ctx, "", "all", 1, all.NextCursor, false)
	require.NoError(t, err)
	require.Equal(t, first.Thread.ID, next.Threads[0].ID)
	require.Empty(t, next.NextCursor)
	filtered, err := s.InspectMailThreads(ctx, "other-task", "closed", 50, "", false)
	require.NoError(t, err)
	require.Len(t, filtered.Threads, 1)
	filtered, err = s.InspectMailThreads(ctx, "task", "closed", 50, "", false)
	require.NoError(t, err)
	require.Len(t, filtered.Threads, 1, "sender node must include a thread filed under another task")
	filtered, err = s.InspectMailThreads(ctx, "recipient-task", "open", 50, "", false)
	require.NoError(t, err)
	require.Len(t, filtered.Threads, 1)
	require.Equal(t, first.Thread.ID, filtered.Threads[0].ID)
	filtered, err = s.InspectMailThreads(ctx, "unrelated-task", "all", 50, "", false)
	require.NoError(t, err)
	require.Empty(t, filtered.Threads)
	filtered, err = other.InspectMailThreads(ctx, "", "all", 50, "", false)
	require.NoError(t, err)
	require.Empty(t, filtered.Threads)
	_, err = other.InspectMailThread(ctx, first.Thread.ID, 50, "")
	require.ErrorIs(t, err, storage.ErrMailNotFound)
	page, err := s.InspectMailThread(ctx, first.Thread.ID, 1, "")
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, first.Message.ID, page.NextCursor)
	require.Len(t, page.Items[0].Receipts, 2)
	require.Equal(t, "inspect-b", page.Items[0].Receipts[0].RecipientRunID)
	require.NotNil(t, page.Items[0].Receipts[0].ReadAt)
	require.Nil(t, page.Items[0].Receipts[0].AcknowledgedAt)
	page, err = s.InspectMailThread(ctx, first.Thread.ID, 1, page.NextCursor)
	require.NoError(t, err)
	require.Equal(t, second.Message.ID, page.Items[0].Message.ID)
	require.Empty(t, page.NextCursor)
	require.Equal(t, "source", page.Items[0].Message.References[0].Body)
	require.NotNil(t, page.Items[0].Receipts[1].AcknowledgedAt)
	require.Nil(t, page.Items[0].Receipts[1].ReadAt)
	require.NoError(t, s.Pool().QueryRow(ctx, snapshot).Scan(&after))
	require.Equal(t, before, after, "inspection changed stored mail")
	for _, limit := range []int{0, 101} {
		_, err = s.InspectMailThreads(ctx, "", "all", limit, "", false)
		require.ErrorIs(t, err, storage.ErrMailInvalid)
		_, err = s.InspectMailThread(ctx, first.Thread.ID, limit, "")
		require.ErrorIs(t, err, storage.ErrMailInvalid)
	}
	_, err = s.InspectMailThreads(ctx, "", "invalid", 50, "", false)
	require.ErrorIs(t, err, storage.ErrMailInvalid)
	_, err = s.InspectMailThreads(ctx, "", "all", 50, "", true)
	require.ErrorIs(t, err, storage.ErrMailInvalid)

	_, err = s.Pool().Exec(ctx, `INSERT INTO specs(project_slug,slug) VALUES
	 ('mail-inspection','root'),('mail-inspection','branch'),('mail-inspection','sibling-root'),('mail-inspection','blocked-task');
	 INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES
	 ('mail-inspection','root','branch','COMPOSES'),('mail-inspection','branch','task','COMPOSES'),
	 ('mail-inspection','root','recipient-task','COMPOSES'),('mail-inspection','sibling-root','third-task','COMPOSES'),
	 ('mail-inspection','root','unrelated-task','DEPENDS_ON'),('mail-inspection','root','blocked-task','BLOCKS');
	 INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES
	 ('mail-inspection','inspect-unrelated','unrelated-task'),('mail-inspection','inspect-blocked','blocked-task');
	 INSERT INTO specs(project_slug,slug) VALUES ('mail-inspection-other','root'),('mail-inspection-other','third-task');
	 INSERT INTO edges(project_slug,from_slug,to_slug,edge_type) VALUES ('mail-inspection-other','root','third-task','COMPOSES')`)
	require.NoError(t, err)
	require.NoError(t, s.CreateSlice(ctx, &storage.Slice{Slug: "unrelated-task/native", ParentSlug: "unrelated-task", SliceID: "native", Intent: "Native slice"}))
	rootMail, err := s.SendMail(ctx, "inspect-c", &storage.SendMailRequest{TaskSlug: "root", Subject: "Filed at root", Body: "Root filing", RecipientRunIDs: []string{"inspect-c"}, IdempotencyKey: "root-filing"})
	require.NoError(t, err)
	recipientMail, err := s.SendMail(ctx, "inspect-c", &storage.SendMailRequest{TaskSlug: "third-task", Subject: "Cross submap", Body: "Recipient in selected submap", RecipientRunIDs: []string{"inspect-b"}, IdempotencyKey: "submap-recipient"})
	require.NoError(t, err)
	for _, outside := range []struct{ run, task string }{{"inspect-c", "third-task"}, {"inspect-unrelated", "unrelated-task"}, {"inspect-blocked", "blocked-task"}} {
		_, err := s.SendMail(ctx, outside.run, &storage.SendMailRequest{TaskSlug: outside.task, Subject: "Outside scope", Body: "Unrelated mail", RecipientRunIDs: []string{outside.run}, IdempotencyKey: "outside"})
		require.NoError(t, err)
	}
	exact, err := s.InspectMailThreads(ctx, "root", "all", 50, "", false)
	require.NoError(t, err)
	require.Len(t, exact.Threads, 1, "default exact-node scope stays unchanged")
	require.Equal(t, rootMail.Thread.ID, exact.Threads[0].ID)
	submap, err := s.InspectMailThreads(ctx, "root", "all", 50, "", true)
	require.NoError(t, err)
	want := []string{recipientMail.Thread.ID, rootMail.Thread.ID, closed.Thread.ID, first.Thread.ID}
	ids := make([]string, 0, len(submap.Threads))
	for _, thread := range submap.Threads {
		ids = append(ids, thread.ID)
	}
	require.Equal(t, want, ids, "include nested sender, descendant recipient and root filing once; exclude sibling/dependency/block/native/cross-project edges")
	sibling, err := s.InspectMailThreads(ctx, "sibling-root", "all", 50, "", true)
	require.NoError(t, err)
	shared := 0
	for _, thread := range sibling.Threads {
		if thread.ID == recipientMail.Thread.ID {
			shared++
		}
	}
	require.Equal(t, 1, shared, "shared thread is not copied across submaps")
	cursor := ""
	for _, id := range want {
		page, err := s.InspectMailThreads(ctx, "root", "all", 1, cursor, true)
		require.NoError(t, err)
		require.Len(t, page.Threads, 1, "scope filter precedes limit even when newer unrelated mail exists")
		require.Equal(t, id, page.Threads[0].ID)
		cursor = page.NextCursor
	}
	require.Empty(t, cursor)
	native, err := s.InspectMailThreads(ctx, "unrelated-task/native", "all", 50, "", true)
	require.NoError(t, err)
	require.Empty(t, native.Threads, "native Slice-to-Spec COMPOSES is not a Spec submap")
	foreign, err := other.InspectMailThreads(ctx, "root", "all", 50, "", true)
	require.NoError(t, err)
	require.Empty(t, foreign.Threads)
}
