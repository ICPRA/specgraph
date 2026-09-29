// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

func validMailText(value string, maxBytes int) bool {
	return len(value) <= maxBytes && strings.TrimSpace(value) != "" && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

// mailRun checks project membership, not the caller's identity. Send locks only
// the sender, using NO KEY UPDATE so cross-run recipient FK checks cannot form
// a lock cycle. Send then locks the thread; close locks only the thread.
func (s *Store) mailRun(ctx context.Context, run string, lock bool) error {
	if !validMailText(run, 256) {
		return storage.ErrMailInvalid
	}
	sql := `SELECT id FROM run_bindings WHERE project_slug=$1 AND id=$2`
	if lock {
		sql += ` FOR NO KEY UPDATE`
	}
	var id string
	err := s.queryRow(ctx, sql, s.project, run).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if projectErr := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE slug=$1)`, s.project).Scan(&exists); projectErr != nil {
			return fmt.Errorf("postgres: mail project: %w", projectErr)
		}
		if !exists {
			return storage.ErrProjectNotFound
		}
		return storage.ErrRunBindingNotFound
	}
	if err != nil {
		return fmt.Errorf("postgres: mail run: %w", err)
	}
	return nil
}

func (s *Store) mailThread(ctx context.Context, id string, lock bool) (storage.MailThread, error) {
	var thread storage.MailThread
	sql := `SELECT id, task_slug, opened_by_run, owner_run, subject, created_at, closed_at, closed_by_run, closure_note
		FROM mail_threads WHERE project_slug=$1 AND id=$2`
	if lock {
		sql += ` FOR UPDATE`
	}
	err := s.queryRow(ctx, sql, s.project, id).Scan(&thread.ID, &thread.TaskSlug, &thread.OpenedByRun,
		&thread.OwnerRun, &thread.Subject, &thread.CreatedAt, &thread.ClosedAt, &thread.ClosedByRun, &thread.ClosureNote)
	if errors.Is(err, pgx.ErrNoRows) {
		return thread, storage.ErrMailNotFound
	}
	if err != nil {
		return thread, fmt.Errorf("postgres: mail thread: %w", err)
	}
	return thread, nil
}

func (s *Store) mailParticipant(ctx context.Context, run string, thread *storage.MailThread) error {
	// PRODUCT-DESIGN section 21 retains historical takeover targets as members;
	// CloseMailThread independently requires the current owner, never membership.
	if run == thread.OpenedByRun || run == thread.OwnerRun {
		return nil
	}
	var participant bool
	err := s.queryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_recipients r
		JOIN mail_messages m ON m.project_slug=r.project_slug AND m.id=r.message_id
		WHERE m.project_slug=$1 AND m.thread_id=$2 AND r.recipient_run=$3)
		OR EXISTS(SELECT 1 FROM mail_owner_events WHERE project_slug=$1 AND thread_id=$2 AND to_run=$3)`, s.project, thread.ID, run).Scan(&participant)
	if err != nil {
		return fmt.Errorf("postgres: mail participant: %w", err)
	}
	if !participant {
		return storage.ErrMailForbidden
	}
	return nil
}

func (s *Store) mailMessage(ctx context.Context, id string) (storage.MailMessage, string, error) {
	var message storage.MailMessage
	var intent string
	err := s.queryRow(ctx, `SELECT m.id, m.thread_id, m.sender_run, m.idempotency_key, m.body, m.created_at,
		ARRAY(SELECT recipient_run FROM mail_recipients WHERE project_slug=m.project_slug AND message_id=m.id ORDER BY recipient_run COLLATE "C"),
		m.requested_thread_id, m.handoff_to_run, `+mailReferencesSQL+` FROM mail_messages m WHERE m.project_slug=$1 AND m.id=$2`, s.project, id).
		Scan(&message.ID, &message.ThreadID, &message.SenderRun, &message.IdempotencyKey, &message.Body,
			&message.CreatedAt, &message.RecipientRunIDs, &intent, &message.HandoffToRun, &message.References)
	if err != nil {
		return message, intent, fmt.Errorf("postgres: mail message: %w", err)
	}
	return message, intent, nil
}

// Read only the explicitly selected messages, not their own references.
const mailReferencesSQL = `COALESCE((SELECT jsonb_agg(jsonb_build_object(
	'message_id', source.id, 'kind', ref.kind, 'thread_id', source.thread_id,
	'task_slug', thread.task_slug, 'sender_run_id', source.sender_run,
	'subject', thread.subject, 'body', source.body, 'created_at', source.created_at)
	ORDER BY source.id COLLATE "C", ref.kind COLLATE "C")
	FROM mail_references ref
	JOIN mail_messages source ON source.project_slug=ref.project_slug AND source.id=ref.source_message_id
	JOIN mail_threads thread ON thread.project_slug=source.project_slug AND thread.id=source.thread_id
	WHERE ref.project_slug=m.project_slug AND ref.message_id=m.id), '[]'::jsonb)`

// SendMail accepts only a previously authenticated and authorized run principal.
// Idempotent retries compare original intent and raw content, even after closure.
// It does not dispatch work, authenticate an agent, or alter a spec/claim.
func (s *Store) SendMail(ctx context.Context, senderRun string, request *storage.SendMailRequest) (storage.SentMail, error) {
	var result storage.SentMail
	if request.HandoffToRun != "" && (request.ThreadID == "" || request.HandoffToRun == senderRun ||
		len(request.RecipientRunIDs) != 1 || request.RecipientRunIDs[0] != request.HandoffToRun || len(request.References) != 0) {
		return result, storage.ErrMailInvalid
	}
	if !validMailText(senderRun, 256) || !validMailText(request.TaskSlug, 256) ||
		!validMailText(request.Subject, 512) || !validMailText(request.Body, 65536) ||
		!validMailText(request.IdempotencyKey, 256) ||
		(request.ThreadID != "" && !validMailText(request.ThreadID, 256)) ||
		len(request.RecipientRunIDs) < 1 || len(request.RecipientRunIDs) > 32 {
		return result, storage.ErrMailInvalid
	}
	recipients := slices.Clone(request.RecipientRunIDs)
	for _, run := range recipients {
		if !validMailText(run, 256) {
			return result, storage.ErrMailInvalid
		}
	}
	slices.Sort(recipients)
	recipients = slices.Compact(recipients)
	if len(request.References) > 8 {
		return result, storage.ErrMailInvalid
	}
	references := slices.Clone(request.References)
	slices.SortFunc(references, func(a, b storage.MailReferenceRequest) int { return strings.Compare(a.MessageID, b.MessageID) })
	replies := 0
	for i, ref := range references {
		if !validMailText(ref.MessageID, 256) || (ref.Kind != "reply" && ref.Kind != "forward" && ref.Kind != "context") ||
			(i > 0 && references[i-1].MessageID == ref.MessageID) {
			return result, storage.ErrMailInvalid
		}
		if ref.Kind == "reply" {
			replies++
		}
	}
	if replies > 1 {
		return result, storage.ErrMailInvalid
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.mailRun(txCtx, senderRun, true); err != nil {
			return err
		}
		var previous string
		messageErr := s.queryRow(txCtx, `SELECT id FROM mail_messages WHERE project_slug=$1 AND sender_run=$2 AND idempotency_key=$3`,
			s.project, senderRun, request.IdempotencyKey).Scan(&previous)
		if messageErr == nil {
			var intent string
			result.Message, intent, messageErr = s.mailMessage(txCtx, previous)
			if messageErr != nil {
				return messageErr
			}
			result.Thread, messageErr = s.mailThread(txCtx, result.Message.ThreadID, false)
			if messageErr != nil {
				return messageErr
			}
			handoff := ""
			if result.Message.HandoffToRun != nil {
				handoff = *result.Message.HandoffToRun
			}
			if handoff != request.HandoffToRun || intent != request.ThreadID || result.Thread.TaskSlug != request.TaskSlug || result.Thread.Subject != request.Subject ||
				result.Message.Body != request.Body || !slices.Equal(result.Message.RecipientRunIDs, recipients) || len(result.Message.References) != len(references) {
				return storage.ErrMailConflict
			}
			for i, ref := range references {
				if result.Message.References[i].MessageID != ref.MessageID || result.Message.References[i].Kind != ref.Kind {
					return storage.ErrMailConflict
				}
			}
			return nil
		}
		if !errors.Is(messageErr, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: mail idempotency: %w", messageErr)
		}
		// Validate before any write, including when joining a caller-owned
		// transaction whose caller may handle this error and still commit.
		sourceBytes := 0
		for _, ref := range references {
			var threadID string
			var bodyBytes int
			if err := s.queryRow(txCtx, `SELECT thread_id,octet_length(body) FROM mail_messages WHERE project_slug=$1 AND id=$2`, s.project, ref.MessageID).Scan(&threadID, &bodyBytes); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return storage.ErrMailNotFound
				}
				return fmt.Errorf("postgres: mail source: %w", err)
			}
			sourceThread, threadErr := s.mailThread(txCtx, threadID, false)
			if threadErr != nil {
				return threadErr
			}
			if err := s.mailParticipant(txCtx, senderRun, &sourceThread); err != nil {
				if !errors.Is(err, storage.ErrMailForbidden) {
					return err
				}
				// A received quote may be shared again, without granting access
				// to any other message in its original thread.
				var quoted bool
				if quotedErr := s.queryRow(txCtx, `SELECT EXISTS(
					SELECT 1 FROM mail_references ref
					JOIN mail_messages m ON m.project_slug=ref.project_slug AND m.id=ref.message_id
					JOIN mail_threads t ON t.project_slug=m.project_slug AND t.id=m.thread_id
					WHERE ref.project_slug=$1 AND ref.source_message_id=$2 AND
					(t.opened_by_run=$3 OR t.owner_run=$3 OR EXISTS(SELECT 1 FROM mail_owner_events e WHERE e.project_slug=t.project_slug AND e.thread_id=t.id AND e.to_run=$3) OR EXISTS(SELECT 1 FROM mail_recipients r
					JOIN mail_messages received ON received.project_slug=r.project_slug AND received.id=r.message_id
					WHERE received.project_slug=t.project_slug AND received.thread_id=t.id AND r.recipient_run=$3)))`,
					s.project, ref.MessageID, senderRun).Scan(&quoted); quotedErr != nil {
					return fmt.Errorf("postgres: mail quoted source permission: %w", quotedErr)
				}
				if !quoted {
					return storage.ErrMailForbidden
				}
			}
			sourceBytes += bodyBytes
			if sourceBytes > 65536 {
				return storage.ErrMailInvalid
			}
		}
		for _, run := range recipients {
			if err := s.mailRun(txCtx, run, false); err != nil {
				return err
			}
		}
		if request.ThreadID == "" {
			var exists bool
			if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM specs WHERE project_slug=$1 AND slug=$2)`, s.project, request.TaskSlug).Scan(&exists); err != nil {
				return fmt.Errorf("postgres: mail task: %w", err)
			}
			if !exists {
				return storage.ErrSpecNotFound
			}
			result.Thread = storage.MailThread{ID: newID("mth"), TaskSlug: request.TaskSlug, OpenedByRun: senderRun, OwnerRun: senderRun, Subject: request.Subject}
			if err := s.queryRow(txCtx, `INSERT INTO mail_threads(project_slug,id,task_slug,opened_by_run,owner_run,subject,created_at)
				VALUES($1,$2,$3,$4,$4,$5,$6) RETURNING created_at`, s.project, result.Thread.ID, request.TaskSlug, senderRun, request.Subject, s.now()).Scan(&result.Thread.CreatedAt); err != nil {
				return fmt.Errorf("postgres: open mail thread: %w", err)
			}
		} else {
			result.Thread, messageErr = s.mailThread(txCtx, request.ThreadID, true)
			if messageErr != nil {
				return messageErr
			}
			if err := s.mailParticipant(txCtx, senderRun, &result.Thread); err != nil {
				return err
			}
			if request.TaskSlug != result.Thread.TaskSlug || request.Subject != result.Thread.Subject {
				return storage.ErrMailConflict
			}
			if result.Thread.ClosedAt != nil {
				return storage.ErrMailClosed
			}
			if request.HandoffToRun != "" {
				if result.Thread.OwnerRun != senderRun {
					return storage.ErrMailForbidden
				}
				var enrolled bool
				if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_bindings rb WHERE rb.project_slug=$1 AND rb.id=$2 AND `+mailEnrolledRunSQL+`)`, s.project, request.HandoffToRun).Scan(&enrolled); err != nil {
					return fmt.Errorf("postgres: mail handoff recipient: %w", err)
				}
				if !enrolled {
					return storage.ErrMailForbidden
				}
			}
		}
		id := newID("msg")
		if _, err := s.exec(txCtx, `INSERT INTO mail_messages(project_slug,id,thread_id,sender_run,idempotency_key,requested_thread_id,body,created_at,handoff_to_run)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''))`, s.project, id, result.Thread.ID, senderRun, request.IdempotencyKey, request.ThreadID, request.Body, s.now(), request.HandoffToRun); err != nil {
			return fmt.Errorf("postgres: send mail: %w", err)
		}
		if _, err := s.exec(txCtx, `INSERT INTO mail_recipients(project_slug,message_id,recipient_run)
			SELECT $1,$2,unnest($3::text[])`, s.project, id, recipients); err != nil {
			return fmt.Errorf("postgres: mail recipients: %w", err)
		}
		for _, ref := range references {
			if _, err := s.exec(txCtx, `INSERT INTO mail_references(project_slug,message_id,source_message_id,kind) VALUES($1,$2,$3,$4)`, s.project, id, ref.MessageID, ref.Kind); err != nil {
				return fmt.Errorf("postgres: mail reference: %w", err)
			}
		}
		if request.HandoffToRun != "" {
			if _, err := s.exec(txCtx, `UPDATE mail_threads SET owner_run=$3 WHERE project_slug=$1 AND id=$2`, s.project, result.Thread.ID, request.HandoffToRun); err != nil {
				return fmt.Errorf("postgres: mail handoff owner: %w", err)
			}
			result.Thread.OwnerRun = request.HandoffToRun
		}
		result.Message, _, messageErr = s.mailMessage(txCtx, id)
		return messageErr
	})
	if err != nil {
		return storage.SentMail{}, err
	}
	return result, nil
}

func validMailPage(limit int, cursor string) bool {
	return limit >= 1 && limit <= 100 && (cursor == "" || validMailText(cursor, 256))
}

// OwnedMailThreads lists only the actor's open threads, regardless of receipts.
func (s *Store) OwnedMailThreads(ctx context.Context, run string, limit int, cursor string) (storage.MailThreadsPage, error) {
	page := storage.MailThreadsPage{Threads: []storage.MailThread{}}
	if !validMailPage(limit, cursor) {
		return page, storage.ErrMailInvalid
	}
	if err := s.mailRun(ctx, run, false); err != nil {
		return page, err
	}
	rows, err := s.query(ctx, `SELECT id,task_slug,opened_by_run,owner_run,subject,created_at,closed_at,closed_by_run,closure_note
		FROM mail_threads WHERE project_slug=$1 AND owner_run=$2 AND closed_at IS NULL
		AND ($3='' OR id<$3) ORDER BY id DESC LIMIT $4`, s.project, run, cursor, limit+1)
	if err != nil {
		return page, fmt.Errorf("postgres: owned mail threads: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var thread storage.MailThread
		if err := rows.Scan(&thread.ID, &thread.TaskSlug, &thread.OpenedByRun, &thread.OwnerRun, &thread.Subject,
			&thread.CreatedAt, &thread.ClosedAt, &thread.ClosedByRun, &thread.ClosureNote); err != nil {
			return page, fmt.Errorf("postgres: owned mail threads: %w", err)
		}
		page.Threads = append(page.Threads, thread)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: owned mail threads: %w", err)
	}
	if len(page.Threads) > limit {
		page.NextCursor = page.Threads[limit-1].ID
		page.Threads = page.Threads[:limit]
	}
	return page, nil
}

func (s *Store) mailPage(ctx context.Context, run, thread string, limit int, cursor string) (storage.MailPage, error) {
	page := storage.MailPage{Items: []storage.MailItem{}}
	rows, err := s.query(ctx, `SELECT m.id,m.thread_id,m.sender_run,m.idempotency_key,m.body,m.created_at,
		ARRAY(SELECT recipient_run FROM mail_recipients WHERE project_slug=m.project_slug AND message_id=m.id ORDER BY recipient_run COLLATE "C"),
		r.recipient_run,r.read_at,r.acknowledged_at,m.handoff_to_run, `+mailReferencesSQL+`
		FROM mail_messages m LEFT JOIN mail_recipients r ON r.project_slug=m.project_slug AND r.message_id=m.id AND r.recipient_run=$2
		WHERE m.project_slug=$1 AND m.id>$4 AND
		(($3<>'' AND m.thread_id=$3) OR ($3='' AND r.recipient_run=$2 AND r.acknowledged_at IS NULL))
		ORDER BY m.id LIMIT $5`, s.project, run, thread, cursor, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var item storage.MailItem
		var recipient *string
		var receipt storage.MailReceipt
		m := &item.Message
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.SenderRun, &m.IdempotencyKey, &m.Body, &m.CreatedAt,
			&m.RecipientRunIDs, &recipient, &receipt.ReadAt, &receipt.AcknowledgedAt, &m.HandoffToRun, &m.References); err != nil {
			return page, fmt.Errorf("postgres: mail page: %w", err)
		}
		if recipient != nil {
			item.Receipt = &receipt
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: mail page: %w", err)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].Message.ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// ReadMailThread returns a participant-only page without modifying receipts.
func (s *Store) ReadMailThread(ctx context.Context, run, threadID string, limit int, cursor string) (storage.MailThreadPage, error) {
	var result storage.MailThreadPage
	if !validMailText(threadID, 256) || !validMailPage(limit, cursor) {
		return result, storage.ErrMailInvalid
	}
	if err := s.mailRun(ctx, run, false); err != nil {
		return result, err
	}
	var threadErr error
	result.Thread, threadErr = s.mailThread(ctx, threadID, false)
	if threadErr != nil {
		return result, threadErr
	}
	if err := s.mailParticipant(ctx, run, &result.Thread); err != nil {
		return storage.MailThreadPage{}, err
	}
	result.MailPage, threadErr = s.mailPage(ctx, run, threadID, limit, cursor)
	return result, threadErr
}

// Inbox returns only the run's unacknowledged mail, including read messages and
// messages in closed threads. ReadMailThread also includes acknowledged mail.
func (s *Store) Inbox(ctx context.Context, run string, limit int, cursor string) (storage.MailPage, error) {
	if !validMailPage(limit, cursor) {
		return storage.MailPage{}, storage.ErrMailInvalid
	}
	if err := s.mailRun(ctx, run, false); err != nil {
		return storage.MailPage{}, err
	}
	return s.mailPage(ctx, run, "", limit, cursor)
}

func (s *Store) mailReceipt(ctx context.Context, run, messageID string, acknowledge bool) (storage.MailReceipt, error) {
	var receipt storage.MailReceipt
	if !validMailText(messageID, 256) {
		return receipt, storage.ErrMailInvalid
	}
	if err := s.mailRun(ctx, run, false); err != nil {
		return receipt, err
	}
	column := "read_at"
	if acknowledge {
		column = "acknowledged_at"
	}
	err := s.queryRow(ctx, `UPDATE mail_recipients SET `+column+`=COALESCE(`+column+`,$4)
		WHERE project_slug=$1 AND message_id=$2 AND recipient_run=$3 RETURNING read_at,acknowledged_at`,
		s.project, messageID, run, s.now()).Scan(&receipt.ReadAt, &receipt.AcknowledgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return receipt, storage.ErrMailForbidden
	}
	if err != nil {
		return receipt, fmt.Errorf("postgres: mail receipt: %w", err)
	}
	return receipt, nil
}

// MarkMailRead records only the recipient's read timestamp, not acknowledgment.
func (s *Store) MarkMailRead(ctx context.Context, run, messageID string) (storage.MailReceipt, error) {
	return s.mailReceipt(ctx, run, messageID, false)
}

// AcknowledgeMail removes mail from the recipient's pending inbox without implying a read.
func (s *Store) AcknowledgeMail(ctx context.Context, run, messageID string) (storage.MailReceipt, error) {
	return s.mailReceipt(ctx, run, messageID, true)
}

// CloseMailThread records the current owner's resolution, not acceptance of a task.
// Repeating the same resolution preserves the original closure timestamp.
func (s *Store) CloseMailThread(ctx context.Context, run, threadID, resolution string) (storage.MailThread, error) {
	var thread storage.MailThread
	if !validMailText(threadID, 256) || !validMailText(resolution, 65536) {
		return thread, storage.ErrMailInvalid
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		if err := s.mailRun(txCtx, run, false); err != nil {
			return err
		}
		var threadErr error
		thread, threadErr = s.mailThread(txCtx, threadID, true)
		if threadErr != nil {
			return threadErr
		}
		if thread.OwnerRun != run {
			return storage.ErrMailForbidden
		}
		if thread.ClosedAt != nil {
			if *thread.ClosureNote != resolution {
				return storage.ErrMailConflict
			}
			return nil
		}
		if _, err := s.exec(txCtx, `UPDATE mail_threads SET closed_at=$3,closed_by_run=$4,closure_note=$5 WHERE project_slug=$1 AND id=$2`,
			s.project, threadID, s.now(), run, resolution); err != nil {
			return fmt.Errorf("postgres: close mail thread: %w", err)
		}
		thread, threadErr = s.mailThread(txCtx, threadID, false)
		return threadErr
	})
	if err != nil {
		return storage.MailThread{}, err
	}
	return thread, nil
}
