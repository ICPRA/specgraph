// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"fmt"

	"github.com/specgraph/specgraph/internal/storage"
)

// InspectMailThreads lists descending thread IDs without changing receipt state.
// The caller must authorize the operator before using this project-wide view.
func (s *Store) InspectMailThreads(ctx context.Context, task, state string, limit int, cursor string, includeDescendants bool) (storage.MailInspectionThreads, error) {
	page := storage.MailInspectionThreads{Threads: []storage.MailThreadSummary{}}
	if !validMailPage(limit, cursor) || (task != "" && !validMailText(task, 256)) || (includeDescendants && task == "") || (state != "open" && state != "closed" && state != "all") {
		return page, storage.ErrMailInvalid
	}
	rows, err := s.query(ctx, `WITH RECURSIVE task_scope(slug) AS (
		SELECT $2::text WHERE $2<>''
		UNION
		SELECT child.slug FROM task_scope parent
		JOIN specs parent_spec ON parent_spec.project_slug=$1 AND parent_spec.slug=parent.slug
		JOIN edges e ON e.project_slug=$1 AND e.from_slug=parent.slug AND e.edge_type='COMPOSES'
		JOIN specs child ON child.project_slug=e.project_slug AND child.slug=e.to_slug WHERE $6
	) SELECT t.id,t.task_slug,t.opened_by_run,t.owner_run,t.subject,t.created_at,t.closed_at,t.closed_by_run,t.closure_note,
		(SELECT count(*) FROM mail_messages m WHERE m.project_slug=t.project_slug AND m.thread_id=t.id),
		(SELECT count(*) FROM mail_recipients r JOIN mail_messages m ON m.project_slug=r.project_slug AND m.id=r.message_id
		 WHERE m.project_slug=t.project_slug AND m.thread_id=t.id AND r.acknowledged_at IS NULL),
		(SELECT max(m.created_at) FROM mail_messages m WHERE m.project_slug=t.project_slug AND m.thread_id=t.id),
		ARRAY(SELECT participant FROM (
		 SELECT t.opened_by_run AS participant UNION
		 SELECT t.owner_run UNION
		 SELECT e.to_run FROM mail_owner_events e WHERE e.project_slug=t.project_slug AND e.thread_id=t.id UNION
		 SELECT m.sender_run FROM mail_messages m WHERE m.project_slug=t.project_slug AND m.thread_id=t.id UNION
		 SELECT r.recipient_run FROM mail_recipients r JOIN mail_messages m ON m.project_slug=r.project_slug AND m.id=r.message_id
		 WHERE m.project_slug=t.project_slug AND m.thread_id=t.id) p ORDER BY participant COLLATE "C"),
		COALESCE((SELECT jsonb_agg(jsonb_build_object(
		 'sender_task_slug',links.sender_task_slug,'recipient_task_slug',links.recipient_task_slug,
		 'message_count',links.message_count,'pending_ack_count',links.pending_ack_count)
		 ORDER BY links.sender_task_slug COLLATE "C",links.recipient_task_slug COLLATE "C") FROM (
		 SELECT sender.task_spec_slug AS sender_task_slug,recipient.task_spec_slug AS recipient_task_slug,
		 count(*) AS message_count,count(*) FILTER (WHERE r.acknowledged_at IS NULL) AS pending_ack_count
		 FROM mail_messages m
		 JOIN mail_recipients r ON r.project_slug=m.project_slug AND r.message_id=m.id
		 JOIN run_bindings sender ON sender.project_slug=m.project_slug AND sender.id=m.sender_run
		 JOIN run_bindings recipient ON recipient.project_slug=r.project_slug AND recipient.id=r.recipient_run
		 WHERE m.project_slug=t.project_slug AND m.thread_id=t.id
		 GROUP BY sender.task_spec_slug,recipient.task_spec_slug) links),'[]'::jsonb)
		FROM mail_threads t WHERE t.project_slug=$1 AND ($2='' OR t.task_slug IN (SELECT slug FROM task_scope) OR EXISTS (
		 SELECT 1 FROM run_bindings b WHERE b.project_slug=t.project_slug AND b.task_spec_slug IN (SELECT slug FROM task_scope)
		 AND (b.id=t.opened_by_run OR b.id=t.owner_run OR EXISTS (SELECT 1 FROM mail_owner_events e WHERE e.project_slug=t.project_slug AND e.thread_id=t.id AND e.to_run=b.id) OR EXISTS (
		  SELECT 1 FROM mail_messages m WHERE m.project_slug=t.project_slug AND m.thread_id=t.id
		  AND (m.sender_run=b.id OR EXISTS (
		   SELECT 1 FROM mail_recipients r WHERE r.project_slug=m.project_slug AND r.message_id=m.id AND r.recipient_run=b.id))))))
		AND ($3='all' OR ($3='open' AND t.closed_at IS NULL) OR ($3='closed' AND t.closed_at IS NOT NULL))
		AND ($4='' OR t.id<$4) ORDER BY t.id DESC LIMIT $5`, s.project, task, state, cursor, limit+1, includeDescendants)
	if err != nil {
		return page, fmt.Errorf("postgres: inspect mail threads: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t storage.MailThreadSummary
		if err := rows.Scan(&t.ID, &t.TaskSlug, &t.OpenedByRun, &t.OwnerRun, &t.Subject, &t.CreatedAt, &t.ClosedAt, &t.ClosedByRun, &t.ClosureNote,
			&t.MessageCount, &t.PendingAckCount, &t.LastMessageAt, &t.ParticipantRunIDs, &t.NodeLinks); err != nil {
			return page, fmt.Errorf("postgres: inspect mail threads: %w", err)
		}
		page.Threads = append(page.Threads, t)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: inspect mail threads: %w", err)
	}
	if len(page.Threads) > limit {
		page.NextCursor = page.Threads[limit-1].ID
		page.Threads = page.Threads[:limit]
	}
	return page, nil
}

// InspectMailThread returns every recipient's receipts; it never impersonates a run.
func (s *Store) InspectMailThread(ctx context.Context, threadID string, limit int, cursor string) (storage.MailInspectionPage, error) {
	page := storage.MailInspectionPage{Items: []storage.MailInspectionItem{}}
	if !validMailText(threadID, 256) || !validMailPage(limit, cursor) {
		return page, storage.ErrMailInvalid
	}
	var err error
	page.Thread, err = s.mailThread(ctx, threadID, false)
	if err != nil {
		return page, err
	}
	rows, err := s.query(ctx, `SELECT m.id,m.thread_id,m.sender_run,m.idempotency_key,m.body,m.created_at,
		ARRAY(SELECT recipient_run FROM mail_recipients WHERE project_slug=m.project_slug AND message_id=m.id ORDER BY recipient_run COLLATE "C"),
		m.handoff_to_run, `+mailReferencesSQL+`,
		COALESCE((SELECT jsonb_agg(jsonb_build_object('recipient_run_id',r.recipient_run,'read_at',r.read_at,'acknowledged_at',r.acknowledged_at)
		ORDER BY r.recipient_run COLLATE "C") FROM mail_recipients r WHERE r.project_slug=m.project_slug AND r.message_id=m.id),'[]'::jsonb)
		FROM mail_messages m WHERE m.project_slug=$1 AND m.thread_id=$2 AND m.id>$3 ORDER BY m.id LIMIT $4`, s.project, threadID, cursor, limit+1)
	if err != nil {
		return page, fmt.Errorf("postgres: inspect mail thread: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item storage.MailInspectionItem
		m := &item.Message
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.SenderRun, &m.IdempotencyKey, &m.Body, &m.CreatedAt, &m.RecipientRunIDs, &m.HandoffToRun, &m.References, &item.Receipts); err != nil {
			return page, fmt.Errorf("postgres: inspect mail thread: %w", err)
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: inspect mail thread: %w", err)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].Message.ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}
