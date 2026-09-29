// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"context"
	"encoding/json"
)

// InsertAcceptance exists only in storage tests to seed synthetic prior decisions
// for original completion-gate fixtures. No production caller can use this method.
func (s *Store) InsertAcceptance(ctx context.Context, delivery, fingerprint, verdict, actor string, conditions json.RawMessage) error {
	_, err := s.insertAcceptance(ctx, delivery, fingerprint, verdict, actor, conditions, "human", nil, nil)
	return err
}
