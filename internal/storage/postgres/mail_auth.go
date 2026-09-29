// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/specgraph/specgraph/internal/storage"
)

const mailGrantColumns = `id,project_slug,environment_id,bridge_subject,granted_by,granted_at,revoked_by,revoked_at`
const mailBindingColumns = `id,grant_id,project_slug,environment_id,thread_id,provider_session_id,provider_instance_id,run_id,approved_by,approved_at,revoked_by,revoked_at`

func scanMailGrant(row pgx.Row) (storage.MailGrant, error) {
	var g storage.MailGrant
	err := row.Scan(&g.ID, &g.Project, &g.EnvironmentID, &g.BridgeSubject, &g.GrantedBy, &g.GrantedAt, &g.RevokedBy, &g.RevokedAt)
	if err != nil {
		return g, fmt.Errorf("postgres: scan mail grant: %w", err)
	}
	return g, nil
}

func scanMailBinding(row pgx.Row) (storage.MailBinding, error) {
	var b storage.MailBinding
	err := row.Scan(&b.ID, &b.GrantID, &b.Project, &b.EnvironmentID, &b.ThreadID, &b.ProviderSessionID, &b.ProviderInstanceID, &b.RunID, &b.ApprovedBy, &b.ApprovedAt, &b.RevokedBy, &b.RevokedAt)
	if err != nil {
		return b, fmt.Errorf("postgres: scan mail binding: %w", err)
	}
	return b, nil
}

func validMailScope(scope storage.MailScope) bool {
	return validMailText(scope.EnvironmentID, 256) && validMailText(scope.ThreadID, 256) && validMailText(scope.ProviderSessionID, 256) && validMailText(scope.ProviderInstanceID, 256)
}

// GrantMailBridge requires a separately authenticated mail.manage approver.
func (s *Store) GrantMailBridge(ctx context.Context, environment, subject, approver string) (storage.MailGrant, error) {
	var result storage.MailGrant
	if !validMailText(environment, 256) || !strings.HasPrefix(subject, "apikey:") || !validMailText(strings.TrimPrefix(subject, "apikey:"), 256) || !validMailText(approver, 256) {
		return result, storage.ErrMailInvalid
	}
	err := s.RunInTransaction(ctx, func(ctx context.Context) error {
		_, err := s.exec(ctx, `INSERT INTO mail_grants(project_slug,id,environment_id,bridge_subject,granted_by,granted_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, s.project, newID("mgr"), environment, subject, approver, s.now())
		if err != nil {
			return err
		}
		result, err = scanMailGrant(s.queryRow(ctx, `SELECT `+mailGrantColumns+` FROM mail_grants WHERE project_slug=$1 AND environment_id=$2 AND bridge_subject=$3 AND revoked_at IS NULL FOR SHARE`, s.project, environment, subject))
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailConflict
		}
		return err
	})
	return result, err
}

// ApproveMailBinding records only an explicit, currently bound provider identity.
// Identical active approval is idempotent; uniqueness conflicts never overwrite it.
func (s *Store) ApproveMailBinding(ctx context.Context, grantID, runID string, scope storage.MailScope, approver string) (storage.MailBinding, error) {
	var result storage.MailBinding
	if !validMailScope(scope) || !validMailText(grantID, 256) || !validMailText(runID, 256) || !validMailText(approver, 256) {
		return result, storage.ErrMailInvalid
	}
	err := s.RunInTransaction(ctx, func(ctx context.Context) error {
		var id string
		err := s.queryRow(ctx, `SELECT id FROM mail_grants WHERE project_slug=$1 AND id=$2 AND environment_id=$3 AND revoked_at IS NULL FOR SHARE`, s.project, grantID, scope.EnvironmentID).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailForbidden
		}
		if err != nil {
			return fmt.Errorf("postgres: mail binding grant: %w", err)
		}
		var thread, state, environment string
		err = s.queryRow(ctx, `SELECT thread_ref,state,environment_id FROM run_bindings WHERE project_slug=$1 AND id=$2 FOR NO KEY UPDATE`, s.project, runID).Scan(&thread, &state, &environment)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrRunBindingNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: mail binding run: %w", err)
		}
		if thread != scope.ThreadID || state != "bound" || (environment != "" && environment != scope.EnvironmentID) {
			return storage.ErrMailConflict
		}
		_, err = s.exec(ctx, `INSERT INTO mail_bindings(project_slug,id,grant_id,environment_id,thread_id,provider_session_id,provider_instance_id,run_id,approved_by,approved_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`, s.project, newID("mbd"), grantID, scope.EnvironmentID, scope.ThreadID, scope.ProviderSessionID, scope.ProviderInstanceID, runID, approver, s.now())
		if err != nil {
			return err
		}
		result, err = scanMailBinding(s.queryRow(ctx, `SELECT `+mailBindingColumns+` FROM mail_bindings WHERE project_slug=$1 AND run_id=$2 AND revoked_at IS NULL FOR SHARE`, s.project, runID))
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailConflict
		}
		if err != nil {
			return err
		}
		if result.GrantID != grantID || result.MailScope != scope {
			return storage.ErrMailConflict
		}
		return nil
	})
	return result, err
}

// RevokeMailGrant updates only the grant: operations lock grant before binding.
func (s *Store) RevokeMailGrant(ctx context.Context, id, approver string) (storage.MailGrant, error) {
	if !validMailText(id, 256) || !validMailText(approver, 256) {
		return storage.MailGrant{}, storage.ErrMailInvalid
	}
	g, err := scanMailGrant(s.queryRow(ctx, `UPDATE mail_grants SET revoked_by=COALESCE(revoked_by,$3),revoked_at=COALESCE(revoked_at,$4) WHERE project_slug=$1 AND id=$2 RETURNING `+mailGrantColumns, s.project, id, approver, s.now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return g, storage.ErrMailNotFound
	}
	return g, err
}

// RevokeMailBinding preserves the first revocation identity and timestamp.
func (s *Store) RevokeMailBinding(ctx context.Context, id, approver string) (storage.MailBinding, error) {
	if !validMailText(id, 256) || !validMailText(approver, 256) {
		return storage.MailBinding{}, storage.ErrMailInvalid
	}
	b, err := scanMailBinding(s.queryRow(ctx, `UPDATE mail_bindings SET revoked_by=COALESCE(revoked_by,$3),revoked_at=COALESCE(revoked_at,$4) WHERE project_slug=$1 AND id=$2 RETURNING `+mailBindingColumns, s.project, id, approver, s.now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return b, storage.ErrMailNotFound
	}
	return b, err
}

// WithMailActor holds grant/binding share locks through the mailbox operation,
// so a successful revocation cannot race past an already authorized operation.
// Run locking also pins the current bound state and serializes sends per run.
func (s *Store) WithMailActor(ctx context.Context, subject string, scope storage.MailScope, fn func(context.Context, storage.MailActor) error) error {
	if !validMailScope(scope) {
		return storage.ErrMailInvalid
	}
	return s.RunInTransaction(ctx, func(ctx context.Context) error {
		var grantID, runID string
		err := s.queryRow(ctx, `SELECT id FROM mail_grants WHERE project_slug=$1 AND environment_id=$2 AND bridge_subject=$3 AND revoked_at IS NULL FOR SHARE`, s.project, scope.EnvironmentID, subject).Scan(&grantID)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailForbidden
		}
		if err != nil {
			return fmt.Errorf("postgres: resolve mail grant: %w", err)
		}
		err = s.queryRow(ctx, `SELECT run_id FROM mail_bindings WHERE project_slug=$1 AND grant_id=$2 AND environment_id=$3 AND thread_id=$4 AND provider_session_id=$5 AND provider_instance_id=$6 AND revoked_at IS NULL FOR SHARE`, s.project, grantID, scope.EnvironmentID, scope.ThreadID, scope.ProviderSessionID, scope.ProviderInstanceID).Scan(&runID)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailForbidden
		}
		if err != nil {
			return fmt.Errorf("postgres: resolve mail binding: %w", err)
		}
		actor := storage.MailActor{RunID: runID}
		err = s.queryRow(ctx, `SELECT task_spec_slug FROM run_bindings WHERE project_slug=$1 AND id=$2 AND thread_ref=$3 AND (environment_id='' OR environment_id=$4) AND state='bound' FOR NO KEY UPDATE`, s.project, runID, scope.ThreadID, scope.EnvironmentID).Scan(&actor.TaskSlug)
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrMailForbidden
		}
		if err != nil {
			return fmt.Errorf("postgres: resolve mail run: %w", err)
		}
		return fn(ctx, actor)
	})
}

// Shared snapshot eligibility for directory and handoff; not an online/liveness guarantee.
const mailEnrolledRunSQL = `rb.state='bound' AND EXISTS (SELECT 1 FROM mail_bindings b
	JOIN mail_grants g ON g.project_slug=b.project_slug AND g.id=b.grant_id AND g.environment_id=b.environment_id
	WHERE b.project_slug=rb.project_slug AND b.run_id=rb.id AND b.revoked_at IS NULL AND g.revoked_at IS NULL
	AND b.thread_id=rb.thread_ref AND (rb.environment_id='' OR rb.environment_id=b.environment_id))`

// MailDirectory reads already enrolled contacts after the caller's WithMailActor authorization.
// It neither enrolls recipients nor infers online status from their stored bindings.
func (s *Store) MailDirectory(ctx context.Context, limit int, cursor, assignmentRole string) (storage.MailDirectoryPage, error) {
	page := storage.MailDirectoryPage{Contacts: []storage.MailContact{}}
	if !validMailPage(limit, cursor) || (assignmentRole != "" && !validMailText(assignmentRole, 256)) {
		return page, storage.ErrMailInvalid
	}
	rows, err := s.query(ctx, `SELECT rb.id,rb.task_spec_slug,cp.body->'dispatch_target'->>'assignmentRole'
		FROM run_bindings rb
		LEFT JOIN context_packages cp ON cp.project_slug=rb.project_slug AND cp.id=rb.package_id
		WHERE rb.project_slug=$1 AND rb.id COLLATE "C">$2
		AND ($3='' OR cp.body->'dispatch_target'->>'assignmentRole'=$3)
		AND `+mailEnrolledRunSQL+`
		ORDER BY rb.id COLLATE "C" LIMIT $4`, s.project, cursor, assignmentRole, limit+1)
	if err != nil {
		return page, fmt.Errorf("postgres: mail directory: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var contact storage.MailContact
		if err := rows.Scan(&contact.RunID, &contact.TaskSlug, &contact.AssignmentRole); err != nil {
			return page, fmt.Errorf("postgres: mail directory: %w", err)
		}
		page.Contacts = append(page.Contacts, contact)
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("postgres: mail directory: %w", err)
	}
	if len(page.Contacts) > limit {
		page.NextCursor = page.Contacts[limit-1].RunID
		page.Contacts = page.Contacts[:limit]
	}
	return page, nil
}
