// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/specgraph/specgraph/internal/storage"
)

// checkPreparedContract requires a caller-owned transaction serialized on the
// project and spec. It does not require an accepted delivery.
func (s *Store) checkPreparedContract(ctx context.Context, slug, runID string) error {
	return s.checkPreparedContractWithOutputs(ctx, slug, runID, nil)
}

func (s *Store) checkPreparedContractWithOutputs(ctx context.Context, slug, runID string, approvedOutputs map[string]bool) error {
	// Compare the recorded primary contract, not the general version or
	// hash: stage changes bump versions, while authoring writes may not.
	var body []byte
	err := s.queryRow(ctx, `SELECT cp.body FROM context_packages cp
		JOIN run_bindings rb ON rb.package_id = cp.id AND rb.project_slug = cp.project_slug
		WHERE rb.project_slug = $1 AND rb.id = $2 AND cp.task_spec_slug = $3`,
		s.project, runID, slug).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrCompletionRequiresRequirementReview
	}
	if err != nil {
		return fmt.Errorf("postgres: completion requirement baseline: %w", err)
	}
	var prepared struct {
		Bundle             struct{ Spec *storage.Spec }
		DependencyRevision *int64 `json:"dependency_revision"`
	}
	if decodeErr := json.Unmarshal(body, &prepared); decodeErr != nil {
		return fmt.Errorf("postgres: decode completion requirement baseline: %w", decodeErr)
	}
	currentRevision, err := s.dependencyRevision(ctx, slug)
	if err != nil {
		return err
	}
	if prepared.DependencyRevision == nil || *prepared.DependencyRevision != currentRevision {
		return storage.ErrExecutionDependenciesChanged
	}
	if checkPrerequisitesErr := s.checkPrerequisites(ctx, slug); checkPrerequisitesErr != nil {
		return checkPrerequisitesErr
	}
	current, err := s.GetSpec(ctx, slug)
	if err != nil {
		return err
	}
	baseline := prepared.Bundle.Spec
	if baseline == nil || baseline.ID != current.ID || baseline.Slug != current.Slug {
		return storage.ErrCompletionRequiresRequirementReview
	}
	// These exceptions are supplied only after exact current source approval was checked.
	if approvedOutputs["intent"] {
		baseline.Intent = current.Intent
	}
	if approvedOutputs["spark_output"] {
		baseline.SparkOutput = current.SparkOutput
	}
	if approvedOutputs["shape_output"] {
		baseline.ShapeOutput = current.ShapeOutput
	}
	if approvedOutputs["specify_output"] {
		baseline.SpecifyOutput = current.SpecifyOutput
	}
	if approvedOutputs["decompose_output"] {
		baseline.DecomposeOutput = current.DecomposeOutput
	}
	if baseline.Intent != current.Intent {
		return storage.ErrCompletionRequiresRequirementReview
	}
	// Use the same typed JSON representation as the persisted package:
	// omitted empty slices and explicit empty slices are equivalent.
	before, err := json.Marshal([4]any{baseline.SparkOutput, baseline.ShapeOutput, baseline.SpecifyOutput, baseline.DecomposeOutput})
	if err != nil {
		return fmt.Errorf("postgres: encode prepared contract: %w", err)
	}
	after, err := json.Marshal([4]any{current.SparkOutput, current.ShapeOutput, current.SpecifyOutput, current.DecomposeOutput})
	if err != nil {
		return fmt.Errorf("postgres: encode current contract: %w", err)
	}
	if !bytes.Equal(before, after) {
		return storage.ErrCompletionRequiresRequirementReview
	}
	return nil
}
