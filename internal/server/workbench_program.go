// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteHumanProgramRun accepts only the local transport's separately verified
// human grant and host service identity. It does not execute the saved command.
func ExecuteHumanProgramRun(ctx context.Context, root *postgres.Store, project string, body io.Reader, consumer *auth.Identity) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman || consumer == nil || consumer.UserID == "" || consumer.UserKind != storage.KindServiceAccount || consumer.Source != "apikey" {
		return nil, storage.ErrProgramRunForbidden
	}
	var req struct {
		storage.PrepareProgramRunRequest
		HostCredential string `json:"hostCredential"`
	}
	if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.HostCredential) == "" {
		return nil, storage.ErrInvalidProgramRun
	}
	if strings.TrimSpace(project) == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	result, err := s.PrepareProgramRun(ctx, &req.PrepareProgramRunRequest, consumer)
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench program: encode response: %w", encodeErr)
	}
	return encoded, nil
}

// ExecuteHostProgramRun uses the explicit project. Storage checks the recorded
// service and native scope against the frozen human-authorized target.
func ExecuteHostProgramRun(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindServiceAccount || identity.Source != "apikey" {
		return nil, storage.ErrProgramRunForbidden
	}
	var req struct {
		storage.ProgramHostScope
		RunID string `json:"runId"`
	}
	var observation storage.ProgramObservation
	var attemptID, expectedPreviousAttemptID, reason, attemptCursor, eventCursor string
	switch operation {
	case "host-read-program-run", "host-authorize-program-run", "host-complete-program-run":
		if decodeMailRequest(body, &req) != nil {
			return nil, storage.ErrInvalidProgramRun
		}
	case "host-result-program-run":
		var resultReq struct {
			storage.ProgramHostScope
			RunID       string                     `json:"runId"`
			Observation storage.ProgramObservation `json:"observation"`
		}
		if decodeMailRequest(body, &resultReq) != nil {
			return nil, storage.ErrInvalidProgramRun
		}
		req.ProgramHostScope, req.RunID, observation = resultReq.ProgramHostScope, resultReq.RunID, resultReq.Observation
	case "host-authorize-next-program-attempt":
		var next struct {
			storage.ProgramHostScope
			RunID                     string `json:"runId"`
			ExpectedPreviousAttemptID string `json:"expectedPreviousAttemptId"`
		}
		if decodeMailRequest(body, &next) != nil || strings.TrimSpace(next.ExpectedPreviousAttemptID) == "" {
			return nil, storage.ErrInvalidProgramLoop
		}
		req.ProgramHostScope, req.RunID, expectedPreviousAttemptID = next.ProgramHostScope, next.RunID, next.ExpectedPreviousAttemptID
	case "host-result-program-attempt":
		var resultReq struct {
			storage.ProgramHostScope
			RunID       string                     `json:"runId"`
			AttemptID   string                     `json:"attemptId"`
			Observation storage.ProgramObservation `json:"observation"`
		}
		if decodeMailRequest(body, &resultReq) != nil || strings.TrimSpace(resultReq.AttemptID) == "" {
			return nil, storage.ErrInvalidProgramLoop
		}
		req.ProgramHostScope, req.RunID, attemptID, observation = resultReq.ProgramHostScope, resultReq.RunID, resultReq.AttemptID, resultReq.Observation
	case "host-stop-program-loop":
		var stop struct {
			storage.ProgramHostScope
			RunID  string `json:"runId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(body, &stop) != nil || strings.TrimSpace(stop.Reason) == "" {
			return nil, storage.ErrInvalidProgramLoop
		}
		req.ProgramHostScope, req.RunID, reason = stop.ProgramHostScope, stop.RunID, stop.Reason
	case "host-read-program-loop-history":
		var history struct {
			storage.ProgramHostScope
			RunID         string `json:"runId"`
			AttemptCursor string `json:"attemptCursor"`
			EventCursor   string `json:"eventCursor"`
		}
		if decodeMailRequest(body, &history) != nil {
			return nil, storage.ErrInvalidProgramLoop
		}
		req.ProgramHostScope, req.RunID, attemptCursor, eventCursor = history.ProgramHostScope, history.RunID, history.AttemptCursor, history.EventCursor
	default:
		return nil, storage.ErrInvalidProgramRun
	}
	if strings.TrimSpace(req.EnvironmentID) == "" || strings.TrimSpace(req.NativeProjectID) == "" || strings.TrimSpace(req.RunID) == "" {
		return nil, storage.ErrInvalidProgramRun
	}
	if strings.TrimSpace(project) == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	var result any
	switch operation {
	case "host-read-program-run":
		result, err = s.ReadProgramRun(ctx, req.ProgramHostScope, req.RunID)
	case "host-authorize-program-run":
		result, err = s.AuthorizeProgramRun(ctx, req.ProgramHostScope, req.RunID)
	case "host-result-program-run":
		result, err = s.RecordProgramResult(ctx, req.ProgramHostScope, req.RunID, &observation)
	case "host-complete-program-run":
		result, err = s.CompleteProgramRun(ctx, req.ProgramHostScope, req.RunID)
	case "host-authorize-next-program-attempt":
		result, err = s.AuthorizeNextProgramAttempt(ctx, req.ProgramHostScope, req.RunID, expectedPreviousAttemptID)
	case "host-result-program-attempt":
		var saved *storage.ProgramRunResult
		saved, err = s.RecordProgramAttemptResult(ctx, req.ProgramHostScope, req.RunID, attemptID, &observation)
		result = struct {
			RunID     string                    `json:"runId"`
			AttemptID string                    `json:"attemptId"`
			Result    *storage.ProgramRunResult `json:"result"`
		}{req.RunID, attemptID, saved}
	case "host-stop-program-loop":
		result, err = s.HostStopProgramLoop(ctx, req.ProgramHostScope, req.RunID, reason)
	case "host-read-program-loop-history":
		result, err = s.HostReadProgramLoopHistory(ctx, req.ProgramHostScope, req.RunID, attemptCursor, eventCursor)
	}
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench program: encode response: %w", encodeErr)
	}
	return encoded, nil
}

// ExecuteHumanProgramLoop records control facts; it never executes or stops a process.
func ExecuteHumanProgramLoop(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (json.RawMessage, error) {
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID == "" || identity.UserKind != storage.KindHuman {
		return nil, storage.ErrProgramRunForbidden
	}
	var runID, attemptID, value, basis, reason string
	var expectedJudgmentID *string
	switch operation {
	case "record-program-loop-decision":
		var req struct {
			RunID              string          `json:"runId"`
			AttemptID          string          `json:"attemptId"`
			Value              string          `json:"value"`
			Basis              string          `json:"basis"`
			ExpectedJudgmentID json.RawMessage `json:"expectedJudgmentId"`
		}
		if decodeMailRequest(body, &req) != nil || len(req.ExpectedJudgmentID) == 0 || json.Unmarshal(req.ExpectedJudgmentID, &expectedJudgmentID) != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.AttemptID) == "" || strings.TrimSpace(req.Basis) == "" || (req.Value != "true" && req.Value != "false" && req.Value != "unknown") {
			return nil, storage.ErrInvalidProgramLoop
		}
		runID, attemptID, value, basis = req.RunID, req.AttemptID, req.Value, req.Basis
	case "stop-program-loop":
		var req struct {
			RunID  string `json:"runId"`
			Reason string `json:"reason"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.Reason) == "" {
			return nil, storage.ErrInvalidProgramLoop
		}
		runID, reason = req.RunID, req.Reason
	default:
		return nil, storage.ErrInvalidProgramLoop
	}
	if strings.TrimSpace(project) == "" || project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	s, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	var result *storage.ProgramLoopEvent
	if operation == "record-program-loop-decision" {
		result, err = s.RecordProgramLoopJudgment(ctx, runID, attemptID, value, basis, expectedJudgmentID)
	} else {
		result, err = s.StopProgramLoop(ctx, runID, reason)
	}
	if err != nil {
		return nil, fmt.Errorf("workbench program: %w", err)
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench program: encode response: %w", encodeErr)
	}
	return encoded, nil
}
