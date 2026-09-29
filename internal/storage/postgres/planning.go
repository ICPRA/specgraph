// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"

	"github.com/specgraph/specgraph/internal/storage"
)

// PlanningManagerRun is called inside the planning command's transaction, so
// the original business write shares the project lock and resolved responsibility.
func (s *Store) PlanningManagerRun(ctx context.Context, scope storage.MailScope) (string, error) {
	if err := s.lockDependencyState(ctx); err != nil {
		return "", err
	}
	run, role, err := s.reviewActorRun(ctx, scope)
	if err != nil {
		return "", err
	}
	if role != "manager" {
		return "", storage.ErrPlanningForbidden
	}
	return run, nil
}
