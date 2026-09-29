// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
)

// Synthetic fixture runs exercise the real store and migrated PostgreSQL. This
// test owns only a newly created fixed-name DB; it refuses an existing DB and
// verifies deletion. No live run or production mail is read or modified.
func TestMailStoragePostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	containerURL, setupErr := postgrestest.ConnString(ctx)
	if setupErr != nil {
		t.Fatal("could not start isolated test container")
	}
	connection, setupErr := pgx.ParseConfig(containerURL)
	if setupErr != nil {
		t.Fatal("cannot parse test database configuration")
	}
	if connection.Host != "127.0.0.1" && connection.Host != "localhost" && connection.Host != "::1" {
		t.Fatal("only a loopback test database is allowed")
	}
	admin, setupErr := pgx.ConnectConfig(ctx, connection)
	if setupErr != nil {
		t.Fatal("cannot connect to the local database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := admin.Close(cleanup); err != nil {
			t.Errorf("close control connection: %v", err)
		}
	}()
	const database = "specgraph_workbench_mail_check"
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, database).Scan(&exists); err != nil || exists {
		t.Fatalf("refusing existing or unverified test database: exists=%v err=%v", exists, err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE specgraph_workbench_mail_check`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, `DROP DATABASE specgraph_workbench_mail_check`); err != nil {
			t.Errorf("drop owned test database: %v", err)
			return
		}
		if err := admin.QueryRow(cleanup, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, database).Scan(&exists); err != nil || exists {
			t.Errorf("test database cleanup: exists=%v err=%v", exists, err)
		}
	}()
	testURL, setupErr := url.Parse(containerURL)
	if setupErr != nil || (testURL.Scheme != "postgres" && testURL.Scheme != "postgresql") {
		t.Fatal("integration test requires a PostgreSQL URL")
	}
	testURL.Path, testURL.RawPath = "/"+database, ""
	query := testURL.Query()
	query.Del("database")
	query.Del("dbname")
	testURL.RawQuery = query.Encode()
	testConnection, setupErr := pgx.ParseConfig(testURL.String())
	if setupErr != nil || testConnection.Database != database || testConnection.Host != connection.Host {
		t.Fatal("isolated database configuration did not resolve exactly")
	}
	store, setupErr := New(ctx, testURL.String(), WithProject("alpha"))
	if setupErr != nil {
		t.Fatal("could not initialize isolated test store")
	}
	defer store.Close(ctx)
	fixture, setupErr := pgx.ConnectConfig(ctx, testConnection)
	if setupErr != nil {
		t.Fatal("could not connect to isolated fixture database")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := fixture.Close(cleanup); err != nil {
			t.Errorf("close fixture connection: %v", err)
		}
	}()
	if _, err := fixture.Exec(ctx, `INSERT INTO projects(slug) VALUES('beta');
		INSERT INTO specs(project_slug,slug) VALUES('alpha','task-a'),('alpha','task-b'),('beta','foreign-task');
		INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES
		('alpha','sender','task-a'),('alpha','receiver','task-b'),('alpha','third','task-b'),
		('alpha','outsider','task-b'),('beta','foreign-run','foreign-task')`); err != nil {
		t.Fatal(err)
	}
	request := storage.SendMailRequest{TaskSlug: "task-a", Subject: "Raw context", Body: "  keep\nraw source\n", RecipientRunIDs: []string{"third", "receiver", "receiver"}, IdempotencyKey: "concurrent-key"}
	const workers = 8
	results := make([]storage.SentMail, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = store.SendMail(ctx, "sender", &request)
		}()
	}
	close(start)
	wg.Wait()
	first := results[0]
	for i := range workers {
		if errs[i] != nil || results[i].Message.ID != first.Message.ID || results[i].Thread.ID != first.Thread.ID {
			t.Fatalf("concurrent send %d: result=%+v err=%v", i, results[i], errs[i])
		}
	}
	if first.Message.Body != request.Body || !slices.Equal(first.Message.RecipientRunIDs, []string{"receiver", "third"}) {
		t.Fatal("raw body or canonical recipients changed")
	}
	if !slices.Equal(request.RecipientRunIDs, []string{"third", "receiver", "receiver"}) {
		t.Fatal("send changed the caller's recipient order or duplicate entries")
	}
	countRows := func(wantThreads, wantMessages, wantRecipients int) {
		t.Helper()
		var threads, messages, recipients int
		if err := fixture.QueryRow(ctx, `SELECT (SELECT count(*) FROM mail_threads),(SELECT count(*) FROM mail_messages),(SELECT count(*) FROM mail_recipients)`).Scan(&threads, &messages, &recipients); err != nil || threads != wantThreads || messages != wantMessages || recipients != wantRecipients {
			t.Fatalf("rows threads=%d messages=%d recipients=%d err=%v", threads, messages, recipients, err)
		}
	}
	countRows(1, 1, 2)
	for name, change := range map[string]func(*storage.SendMailRequest){
		"body":       func(r *storage.SendMailRequest) { r.Body += "changed" },
		"subject":    func(r *storage.SendMailRequest) { r.Subject += "changed" },
		"task":       func(r *storage.SendMailRequest) { r.TaskSlug = "task-b" },
		"intent":     func(r *storage.SendMailRequest) { r.ThreadID = first.Thread.ID },
		"recipients": func(r *storage.SendMailRequest) { r.RecipientRunIDs = []string{"receiver"} },
	} {
		t.Run("conflict-"+name, func(t *testing.T) {
			changed := request
			change(&changed)
			if _, err := store.SendMail(ctx, "sender", &changed); !errors.Is(err, storage.ErrMailConflict) {
				t.Fatalf("expected content conflict: %v", err)
			}
		})
	}
	for _, recipient := range []string{"missing", "foreign-run"} {
		bad := request
		bad.IdempotencyKey, bad.RecipientRunIDs = recipient, []string{recipient}
		if _, err := store.SendMail(ctx, "sender", &bad); !errors.Is(err, storage.ErrRunBindingNotFound) {
			t.Fatalf("invalid recipient: %v", err)
		}
		countRows(1, 1, 2)
	}
	if err := store.RunInTransaction(ctx, func(txCtx context.Context) error {
		bad := request
		bad.IdempotencyKey, bad.RecipientRunIDs = "nested-invalid", []string{"missing"}
		if _, err := store.SendMail(txCtx, "sender", &bad); !errors.Is(err, storage.ErrRunBindingNotFound) {
			t.Fatalf("nested invalid recipient: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("outer transaction: %v", err)
	}
	countRows(1, 1, 2)
	bad := request
	bad.IdempotencyKey, bad.TaskSlug = "foreign-task", "foreign-task"
	if _, err := store.SendMail(ctx, "sender", &bad); !errors.Is(err, storage.ErrSpecNotFound) {
		t.Fatalf("cross-project task: %v", err)
	}
	if _, err := store.SendMail(ctx, "foreign-run", &request); !errors.Is(err, storage.ErrRunBindingNotFound) {
		t.Fatalf("cross-project sender: %v", err)
	}
	if _, err := store.ReadMailThread(ctx, "outsider", first.Thread.ID, 10, ""); !errors.Is(err, storage.ErrMailForbidden) {
		t.Fatalf("nonparticipant read: %v", err)
	}
	if _, err := store.ReadMailThread(ctx, "foreign-run", first.Thread.ID, 10, ""); !errors.Is(err, storage.ErrRunBindingNotFound) {
		t.Fatalf("foreign read: %v", err)
	}
	beta := *store
	beta.project = "beta"
	if _, err := beta.ReadMailThread(ctx, "foreign-run", first.Thread.ID, 10, ""); !errors.Is(err, storage.ErrMailNotFound) {
		t.Fatalf("cross-project thread: %v", err)
	}
	missingProject := *store
	missingProject.project = "missing"
	if _, err := missingProject.Inbox(ctx, "sender", 10, ""); !errors.Is(err, storage.ErrProjectNotFound) {
		t.Fatalf("missing project: %v", err)
	}
	reply := request
	reply.ThreadID, reply.IdempotencyKey, reply.RecipientRunIDs = first.Thread.ID, "reply", []string{"sender"}
	if _, err := store.SendMail(ctx, "outsider", &reply); !errors.Is(err, storage.ErrMailForbidden) {
		t.Fatalf("nonparticipant reply: %v", err)
	}
	wrongContext := reply
	wrongContext.TaskSlug = "task-b"
	if _, err := store.SendMail(ctx, "receiver", &wrongContext); !errors.Is(err, storage.ErrMailConflict) {
		t.Fatalf("reply context mutation: %v", err)
	}
	replyResult, mailErr := store.SendMail(ctx, "receiver", &reply)
	if mailErr != nil || replyResult.Thread.ID != first.Thread.ID {
		t.Fatalf("cross-task participant reply: %v", mailErr)
	}
	page, mailErr := store.ReadMailThread(ctx, "receiver", first.Thread.ID, 1, "")
	if mailErr != nil || len(page.Items) != 1 || page.NextCursor == "" || page.Items[0].Receipt == nil || page.Items[0].Receipt.ReadAt != nil || page.Items[0].Receipt.AcknowledgedAt != nil {
		t.Fatalf("first page/read receipt: %+v err=%v", page, mailErr)
	}
	lastPage, mailErr := store.ReadMailThread(ctx, "receiver", first.Thread.ID, 1, page.NextCursor)
	if mailErr != nil || len(lastPage.Items) != 1 || lastPage.NextCursor != "" || lastPage.Items[0].Message.ID == page.Items[0].Message.ID || lastPage.Items[0].Receipt != nil {
		t.Fatalf("last page: %+v err=%v", lastPage, mailErr)
	}
	read, mailErr := store.MarkMailRead(ctx, "receiver", first.Message.ID)
	if mailErr != nil || read.ReadAt == nil || read.AcknowledgedAt != nil {
		t.Fatalf("mark read: %+v err=%v", read, mailErr)
	}
	readAgain, mailErr := store.MarkMailRead(ctx, "receiver", first.Message.ID)
	if mailErr != nil || readAgain.ReadAt == nil || !readAgain.ReadAt.Equal(*read.ReadAt) {
		t.Fatalf("repeat read timestamp: %+v err=%v", readAgain, mailErr)
	}
	inbox, mailErr := store.Inbox(ctx, "receiver", 10, "")
	if mailErr != nil || len(inbox.Items) != 1 {
		t.Fatalf("read must stay pending: %+v err=%v", inbox, mailErr)
	}
	for _, run := range []string{"sender", "outsider"} {
		if _, err := store.MarkMailRead(ctx, run, first.Message.ID); !errors.Is(err, storage.ErrMailForbidden) {
			t.Fatalf("nonrecipient read marker: %v", err)
		}
		if _, err := store.AcknowledgeMail(ctx, run, first.Message.ID); !errors.Is(err, storage.ErrMailForbidden) {
			t.Fatalf("nonrecipient ack: %v", err)
		}
	}
	ack, mailErr := store.AcknowledgeMail(ctx, "receiver", first.Message.ID)
	if mailErr != nil || ack.AcknowledgedAt == nil || ack.ReadAt == nil || !ack.ReadAt.Equal(*read.ReadAt) {
		t.Fatalf("ack: %+v err=%v", ack, mailErr)
	}
	again, mailErr := store.AcknowledgeMail(ctx, "receiver", first.Message.ID)
	if mailErr != nil || again.AcknowledgedAt == nil || !again.AcknowledgedAt.Equal(*ack.AcknowledgedAt) {
		t.Fatalf("repeat ack timestamp: %+v err=%v", again, mailErr)
	}
	if inbox, err := store.Inbox(ctx, "receiver", 10, ""); err != nil || len(inbox.Items) != 0 {
		t.Fatalf("ack inbox exclusion: %+v err=%v", inbox, err)
	}
	ackWithoutRead, mailErr := store.AcknowledgeMail(ctx, "third", first.Message.ID)
	if mailErr != nil || ackWithoutRead.ReadAt != nil || ackWithoutRead.AcknowledgedAt == nil {
		t.Fatalf("ack must not imply read: %+v err=%v", ackWithoutRead, mailErr)
	}
	history, mailErr := store.ReadMailThread(ctx, "receiver", first.Thread.ID, 10, "")
	if mailErr != nil || history.Thread.ClosedAt != nil || history.Items[0].Receipt.AcknowledgedAt == nil {
		t.Fatalf("ack must not close/delete history: %+v err=%v", history, mailErr)
	}
	if _, err := store.CloseMailThread(ctx, "receiver", first.Thread.ID, "resolved"); !errors.Is(err, storage.ErrMailForbidden) {
		t.Fatalf("recipient cannot close: %v", err)
	}
	if _, err := store.CloseMailThread(ctx, "sender", first.Thread.ID, " \n"); !errors.Is(err, storage.ErrMailInvalid) {
		t.Fatalf("empty resolution: %v", err)
	}
	_, mailErr = fixture.Exec(ctx, `UPDATE mail_threads SET closed_at=now(),closure_note='invalid null closer' WHERE project_slug='alpha' AND id=$1`, first.Thread.ID)
	var constraint *pgconn.PgError
	if !errors.As(mailErr, &constraint) || constraint.Code != "23514" {
		t.Fatalf("SQL must reject null closer: %v", mailErr)
	}
	closed, mailErr := store.CloseMailThread(ctx, "sender", first.Thread.ID, "  resolved\n")
	if mailErr != nil || closed.ClosedAt == nil || closed.ClosureNote == nil || *closed.ClosureNote != "  resolved\n" {
		t.Fatalf("close: %+v err=%v", closed, mailErr)
	}
	closedAgain, mailErr := store.CloseMailThread(ctx, "sender", first.Thread.ID, "  resolved\n")
	if mailErr != nil || closedAgain.ClosedAt == nil || !closedAgain.ClosedAt.Equal(*closed.ClosedAt) {
		t.Fatalf("repeat close: %+v err=%v", closedAgain, mailErr)
	}
	if _, err := store.CloseMailThread(ctx, "sender", first.Thread.ID, "different"); !errors.Is(err, storage.ErrMailConflict) {
		t.Fatalf("resolution overwrite: %v", err)
	}
	request.RecipientRunIDs = []string{"receiver", "third"}
	retry, mailErr := store.SendMail(ctx, "sender", &request)
	if mailErr != nil || retry.Message.ID != first.Message.ID || retry.Thread.ClosedAt == nil {
		t.Fatalf("idempotent original intent after ack/close: %+v err=%v", retry, mailErr)
	}
	retriedReply, mailErr := store.SendMail(ctx, "receiver", &reply)
	if mailErr != nil || retriedReply.Message.ID != replyResult.Message.ID {
		t.Fatalf("idempotent reply after close: %v", mailErr)
	}
	reply.IdempotencyKey = "new-after-close"
	if _, err := store.SendMail(ctx, "receiver", &reply); !errors.Is(err, storage.ErrMailClosed) {
		t.Fatalf("new mail after close: %v", err)
	}
	if inbox, err := store.Inbox(ctx, "sender", 10, ""); err != nil || len(inbox.Items) != 1 {
		t.Fatalf("closure must not ack pending reply: %+v err=%v", inbox, err)
	}
	countRows(1, 2, 3)
	var altered int
	if err := fixture.QueryRow(ctx, `SELECT (SELECT count(*) FROM specs WHERE stage<>'spark') + (SELECT count(*) FROM claims) + (SELECT count(*) FROM acceptances)`).Scan(&altered); err != nil || altered != 0 {
		t.Fatalf("mail altered spec, claim or acceptance: %d err=%v", altered, err)
	}
	t.Run("concurrent-cross-send", func(t *testing.T) {
		crossCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		// Both outer transactions hold their sender locks before sending. FK
		// checks on the opposite recipient must coexist with those locks.
		ready := make(chan struct{}, 2)
		proceed := make(chan struct{})
		failures := make(chan error, 2)
		for _, sender := range []string{"sender", "receiver"} {
			go func() {
				failures <- store.RunInTransaction(crossCtx, func(txCtx context.Context) error {
					err := store.mailRun(txCtx, sender, true)
					ready <- struct{}{}
					if err != nil {
						return err
					}
					select {
					case <-proceed:
					case <-crossCtx.Done():
						return crossCtx.Err()
					}
					recipient := "receiver"
					if sender == "receiver" {
						recipient = "sender"
					}
					cross := storage.SendMailRequest{TaskSlug: "task-a", Subject: "cross send", Body: "message", RecipientRunIDs: []string{recipient}, IdempotencyKey: "cross-send"}
					_, err = store.SendMail(txCtx, sender, &cross)
					return err
				})
			}()
		}
		for range 2 {
			select {
			case <-ready:
			case <-crossCtx.Done():
				t.Error("cross-send sender lock barrier timed out")
			}
		}
		close(proceed)
		for range 2 {
			if err := <-failures; err != nil {
				t.Errorf("cross-send failed: %v", err)
			}
		}
	})
	countRows(3, 4, 5)
}
