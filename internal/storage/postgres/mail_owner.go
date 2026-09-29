// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
)

const mailOwnerEventColumns = `id,thread_id,from_run,to_run,actor_kind,actor_user_id,actor_run_id,reason,created_at`
const retiredMailOwnerSQL = `state IN ('handed_off','completed','preparation_cancelled')`

// TakeoverMail derives the actor from authenticated identity and, for agents,
// current PM responsibility plus mailbox authorization. No caller selects actorRun.
// The human transport separately authorizes workbench.manage.
func (s *Store) TakeoverMail(ctx context.Context, req storage.TakeoverMailRequest, scope *storage.MailScope) (storage.TakenOverMail, error) {
	var result storage.TakenOverMail
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" {
		return result, auth.ErrUnauthenticated
	}
	if scope == nil && identity.UserKind != storage.KindHuman {
		return result, storage.ErrMailForbidden
	}
	if !validMailText(req.ThreadID, 256) || !validMailText(req.RecipientRunID, 256) || !validMailText(req.Body, 65536) || !validMailText(req.IdempotencyKey, 256) {
		return result, storage.ErrMailInvalid
	}
	takeover := func(txCtx context.Context, actorRun string) error {
		e := &result.Event
		err := s.queryRow(txCtx, `SELECT `+mailOwnerEventColumns+` FROM mail_owner_events
			WHERE project_slug=$1 AND actor_user_id=$2 AND actor_run_id IS NOT DISTINCT FROM NULLIF($3,'') AND idempotency_key=$4`,
			s.project, identity.UserID, actorRun, req.IdempotencyKey).Scan(&e.ID, &e.ThreadID, &e.FromRun, &e.ToRun, &e.ActorKind, &e.ActorUserID, &e.ActorRunID, &e.Reason, &e.CreatedAt)
		if err == nil {
			if e.ThreadID != req.ThreadID || e.ToRun != req.RecipientRunID || e.Reason != req.Body {
				return storage.ErrMailConflict
			}
			result.Thread, err = s.mailThread(txCtx, e.ThreadID, false)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: mail takeover idempotency: %w", err)
		}
		result.Thread, err = s.mailThread(txCtx, req.ThreadID, true)
		if err != nil {
			return err
		}
		if result.Thread.ClosedAt != nil {
			return storage.ErrMailClosed
		}
		var retired bool
		if err := s.queryRow(txCtx, `SELECT `+retiredMailOwnerSQL+` FROM run_bindings WHERE project_slug=$1 AND id=$2`, s.project, result.Thread.OwnerRun).Scan(&retired); err != nil {
			return fmt.Errorf("postgres: mail takeover retired owner: %w", err)
		}
		if !retired {
			return storage.ErrMailConflict
		}
		if err := s.mailRun(txCtx, req.RecipientRunID, false); err != nil {
			return err
		}
		var enrolled bool
		if err := s.queryRow(txCtx, `SELECT EXISTS(SELECT 1 FROM run_bindings rb WHERE rb.project_slug=$1 AND rb.id=$2 AND `+mailEnrolledRunSQL+`)`, s.project, req.RecipientRunID).Scan(&enrolled); err != nil {
			return fmt.Errorf("postgres: mail takeover recipient: %w", err)
		}
		if !enrolled {
			return storage.ErrMailForbidden
		}
		kind := "human"
		if actorRun != "" {
			kind = "agent"
		}
		if err := s.queryRow(txCtx, `INSERT INTO mail_owner_events(project_slug,id,thread_id,from_run,to_run,actor_kind,actor_user_id,actor_run_id,reason,idempotency_key,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,$10,$11) RETURNING `+mailOwnerEventColumns,
			s.project, newID("moe"), req.ThreadID, result.Thread.OwnerRun, req.RecipientRunID, kind, identity.UserID, actorRun, req.Body, req.IdempotencyKey, s.now()).
			Scan(&e.ID, &e.ThreadID, &e.FromRun, &e.ToRun, &e.ActorKind, &e.ActorUserID, &e.ActorRunID, &e.Reason, &e.CreatedAt); err != nil {
			return fmt.Errorf("postgres: mail takeover event: %w", err)
		}
		if _, err := s.exec(txCtx, `UPDATE mail_threads SET owner_run=$3 WHERE project_slug=$1 AND id=$2`, s.project, req.ThreadID, req.RecipientRunID); err != nil {
			return err
		}
		result.Thread.OwnerRun = req.RecipientRunID
		return nil
	}
	err := s.RunInTransaction(ctx, func(txCtx context.Context) error {
		// Reuse project-before-run ordering and serialize idempotency across threads.
		if err := s.lockDependencyState(txCtx); err != nil {
			return err
		}
		if scope == nil {
			return takeover(txCtx, "")
		}
		manager, err := s.PlanningManagerRun(txCtx, *scope)
		if err != nil {
			return err
		}
		return s.WithMailActor(txCtx, identity.Subject, *scope, func(actorCtx context.Context, actor storage.MailActor) error {
			if manager != actor.RunID {
				return storage.ErrMailForbidden
			}
			return takeover(actorCtx, manager)
		})
	})
	if err != nil {
		return storage.TakenOverMail{}, err
	}
	return result, nil
}

// RetiredMailThreads is PM-authorized metadata discovery, not history access.
func (s *Store) RetiredMailThreads(ctx context.Context, limit int, cursor string) (storage.MailThreadsPage, error) {
	page := storage.MailThreadsPage{Threads: []storage.MailThread{}}
	if !validMailPage(limit, cursor) {
		return page, storage.ErrMailInvalid
	}
	rows, err := s.query(ctx, `SELECT t.id,t.task_slug,t.opened_by_run,t.owner_run,t.subject,t.created_at,t.closed_at,t.closed_by_run,t.closure_note
		FROM mail_threads t JOIN run_bindings r ON r.project_slug=t.project_slug AND r.id=t.owner_run
		WHERE t.project_slug=$1 AND t.closed_at IS NULL AND `+retiredMailOwnerSQL+` AND ($2='' OR t.id<$2) ORDER BY t.id DESC LIMIT $3`, s.project, cursor, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var thread storage.MailThread
		if err := rows.Scan(&thread.ID, &thread.TaskSlug, &thread.OpenedByRun, &thread.OwnerRun, &thread.Subject, &thread.CreatedAt, &thread.ClosedAt, &thread.ClosedByRun, &thread.ClosureNote); err != nil {
			return page, fmt.Errorf("postgres: scan retired mail thread: %w", err)
		}
		page.Threads = append(page.Threads, thread)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: retired mail threads: %w", err)
	}
	if len(page.Threads) > limit {
		page.NextCursor = page.Threads[limit-1].ID
		page.Threads = page.Threads[:limit]
	}
	return page, nil
}

// MailOwnerHistory is participant-only when run is nonempty. Empty run is
// reserved for the separately authorized human inspection transport.
func (s *Store) MailOwnerHistory(ctx context.Context, run, threadID string, limit int, cursor string) (storage.MailOwnerEventsPage, error) {
	page := storage.MailOwnerEventsPage{Events: []storage.MailOwnerEvent{}}
	if !validMailText(threadID, 256) || !validMailPage(limit, cursor) {
		return page, storage.ErrMailInvalid
	}
	thread, threadErr := s.mailThread(ctx, threadID, false)
	if threadErr != nil {
		return page, threadErr
	}
	if run != "" {
		if err := s.mailRun(ctx, run, false); err != nil {
			return page, err
		}
		if err := s.mailParticipant(ctx, run, &thread); err != nil {
			return page, err
		}
	}
	rows, err := s.query(ctx, `SELECT `+mailOwnerEventColumns+` FROM mail_owner_events WHERE project_slug=$1 AND thread_id=$2
		AND ($3='' OR id<$3) ORDER BY id DESC LIMIT $4`, s.project, threadID, cursor, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var e storage.MailOwnerEvent
		if err := rows.Scan(&e.ID, &e.ThreadID, &e.FromRun, &e.ToRun, &e.ActorKind, &e.ActorUserID, &e.ActorRunID, &e.Reason, &e.CreatedAt); err != nil {
			return page, fmt.Errorf("postgres: scan mail owner event: %w", err)
		}
		page.Events = append(page.Events, e)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: mail owner history: %w", err)
	}
	if len(page.Events) > limit {
		page.NextCursor = page.Events[limit-1].ID
		page.Events = page.Events[:limit]
	}
	return page, nil
}
