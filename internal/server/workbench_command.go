// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect"
	specv1 "github.com/specgraph/specgraph/gen/specgraph/v1"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"google.golang.org/protobuf/encoding/protojson"
)

// ExecuteWorkbenchCommand reuses the HTTP business rules without HTTP transport.
// The caller must authenticate and authorize the operation, and supply its resolved
// UserID as actor. Project selection never creates a project.
func ExecuteWorkbenchCommand(ctx context.Context, root *postgres.Store, operation, project, slug, actor string, body json.RawMessage) (json.RawMessage, error) {
	if project == "_server" || !validProjectSlug.MatchString(project) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid project slug"))
	}
	store, scopeErr := root.ScopedExisting(ctx, project)
	if scopeErr != nil {
		return nil, fmt.Errorf("workbench command: %w", scopeErr)
	}
	switch operation {
	case "takeover-mail":
		var req storage.TakeoverMailRequest
		if slug != "" || len(body) > 128<<10 || decodeMailRequest(bytes.NewReader(body), &req) != nil {
			return nil, storage.ErrMailInvalid
		}
		result, err := store.TakeoverMail(ctx, req, nil)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(map[string]any{"thread": mailThreadJSON(&result.Thread), "event": result.Event})
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "record-node-event":
		var req storage.RecordNodeEventRequest
		if len(body) > 32<<10 || validateSlug(slug) != nil || decodeMailRequest(bytes.NewReader(body), &req) != nil {
			return nil, storage.ErrInvalidNodeEvent
		}
		event, err := store.RecordNodeEvent(ctx, slug, req)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(event)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "record-test-result":
		var req storage.RecordTestReportRequest
		if len(body) > 32<<10 || decodeMailRequest(bytes.NewReader(body), &req) != nil || (slug != "" && slug != req.DeliveryID) {
			return nil, storage.ErrInvalidTestReport
		}
		report, err := store.RecordTestReport(ctx, &req, nil)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(report)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "assign-review":
		var req storage.AssignReviewRequest
		if len(body) > 32<<10 || validateSlug(slug) != nil || decodeMailRequest(bytes.NewReader(body), &req) != nil {
			return nil, storage.ErrInvalidReview
		}
		if req.TaskSlug != "" && req.TaskSlug != slug {
			return nil, storage.ErrInvalidReview
		}
		req.TaskSlug = slug
		result, err := store.AssignReview(ctx, &req, nil)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "review-source":
		var req struct {
			RequestID string `json:"requestId"`
			Verdict   string `json:"verdict"`
			Basis     string `json:"basis"`
		}
		if len(body) > 32<<10 || validateSlug(slug) != nil || decodeMailRequest(bytes.NewReader(body), &req) != nil {
			return nil, storage.ErrInvalidReview
		}
		result, err := store.ReviewSource(ctx, slug, req.RequestID, req.Verdict, req.Basis)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "abandon-node":
		var req struct {
			Reason string `json:"reason"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 32<<10 || validateSlug(slug) != nil || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF || strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("reason must contain 1-4000 characters"))
		}
		spec, err := store.LifecycleAbandonSpec(ctx, slug, req.Reason)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(map[string]any{"slug": spec.Slug, "stage": spec.Stage, "version": spec.Version})
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "set-node-mark":
		return setWorkbenchNodeMark(ctx, store, slug, actor, body, false)
	case "bind-project":
		var req struct {
			EnvironmentID   string `json:"environmentId"`
			NativeProjectID string `json:"nativeProjectId"`
			WorkspaceRoot   string `json:"workspaceRoot"`
			Reason          string `json:"reason"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 32<<10 || validateSlug(slug) != nil || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF ||
			strings.TrimSpace(req.EnvironmentID) == "" || strings.TrimSpace(req.NativeProjectID) == "" ||
			strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 || len(req.WorkspaceRoot) > 4096 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("valid environmentId, nativeProjectId and reason of 1-4000 characters required"))
		}
		binding, err := store.BindProject(ctx, project, req.EnvironmentID, req.NativeProjectID, req.WorkspaceRoot, req.Reason, actor)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(binding)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "unbind-project":
		var req struct {
			Reason string `json:"reason"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 32<<10 || validateSlug(slug) != nil || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF ||
			strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 || strings.TrimSpace(actor) == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unbind-project requires a reason of 1-4000 characters"))
		}
		binding, err := store.RevokeProjectBinding(ctx, project, req.Reason, actor)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(binding)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "subdivide-node":
		var req storage.SubdivideRequest
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 2<<20 || strings.TrimSpace(slug) == "" || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("valid subdivision fields required; maximum 2 MiB"))
		}
		result, err := store.SubdivideSpec(ctx, slug, actor, req)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "submit-delivery":
		var req struct {
			RunBindingID      string          `json:"run_binding_id"`
			Snapshot          json.RawMessage `json:"snapshot"`
			ExpectedAttemptID string          `json:"expectedAttemptId"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if slug != "" || len(body) > 32*1024 || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF ||
			!manualCompletionKeyPattern.MatchString(req.RunBindingID) || !json.Valid(req.Snapshot) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("valid run_binding_id and JSON snapshot required; maximum 32 KiB"))
		}
		deliveryID, err := store.CreateDelivery(ctx, req.RunBindingID, req.Snapshot, actor, req.ExpectedAttemptID)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(map[string]string{"delivery_id": deliveryID})
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "review-delivery":
		var req struct {
			Verdict string `json:"verdict"`
			Basis   string `json:"basis"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 32*1024 || !manualCompletionKeyPattern.MatchString(slug) || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF ||
			(req.Verdict != "accepted" && req.Verdict != "rejected") || strings.TrimSpace(req.Basis) == "" || utf8.RuneCountInString(req.Basis) > 4000 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("verdict must be accepted or rejected; basis must contain 1-4000 characters"))
		}
		result, err := store.ReviewDelivery(ctx, slug, "", req.Verdict, req.Basis)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "approve-node":
		var req struct {
			ExpectedVersion int32  `json:"expectedVersion"`
			Basis           string `json:"basis"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if len(body) > 32*1024 || validateSlug(slug) != nil || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF ||
			req.ExpectedVersion < 1 || strings.TrimSpace(req.Basis) == "" || utf8.RuneCountInString(req.Basis) > 4000 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("expectedVersion must be positive; basis must contain 1-4000 characters"))
		}
		version, err := store.ApproveWorkbenchNode(ctx, slug, actor, req.ExpectedVersion, req.Basis)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(map[string]any{"approved": slug, "version": version})
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "prepare-run", "bind-run", "authorize-run", "resolve-run", "confirm-run-stopped", "cancel-preparation", "abort-preparation", "complete-run":
		return executeDispatchCommand(ctx, store, operation, slug, actor, body)
	case "create-node":
		var req specv1.CreateSpecRequest
		// Match Connect's existing JSON codec rather than a second request schema.
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, &req); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid create-node body"))
		}
		result, err := createScopedSpec(ctx, store, &req)
		if err != nil {
			return nil, err
		}
		encoded, encodeErr := protojson.Marshal(result.Msg)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "merge-nodes":
		var req storage.MergeRequest
		if slug != "" || len(body) > 2<<20 || decodeMailRequest(bytes.NewReader(body), &req) != nil {
			return nil, storage.ErrInvalidNodeMerge
		}
		result, err := store.MergeNodes(ctx, req, nil)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		return json.Marshal(result)
	case "add-dependency", "remove-dependency":
		if len(body) > 32*1024 || validateSlug(slug) != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid dependency edit body"))
		}
		req, err := decodeDependencyEditBody(bytes.NewReader(body))
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid dependency edit body"))
		}
		var result storage.DependencyEditResult
		if operation == "remove-dependency" {
			result, err = store.RemoveDependency(ctx, slug, actor, req)
		} else {
			result, err = store.AddDependency(ctx, slug, actor, req)
		}
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(result)
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	case "manual-complete":
		if len(body) > 32*1024 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid manual completion body"))
		}
		req, err := decodeManualCompletionBody(bytes.NewReader(body))
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid manual completion body"))
		}
		version, replayed, err := store.ManualComplete(ctx, slug, actor, req.ExpectedVersion, req.IdempotencyKey, req.Note)
		if err != nil {
			return nil, fmt.Errorf("workbench command: %w", err)
		}
		encoded, encodeErr := json.Marshal(map[string]any{"completed": slug, "version": version, "replayed": replayed})
		if encodeErr != nil {
			return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
		}
		return encoded, nil
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported workbench command"))
	}
}

func setWorkbenchNodeMark(ctx context.Context, store *postgres.Store, slug, actor string, body json.RawMessage, manager bool) (json.RawMessage, error) {
	var req struct {
		Kind           string          `json:"kind"`
		Value          string          `json:"value"`
		Reason         string          `json:"reason"`
		ExpectedMarkID json.RawMessage `json:"expectedMarkId"`
	}
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("valid kind/value and reason of 1-4000 characters required"))
	if len(body) > 32<<10 || validateSlug(slug) != nil || decodeMailRequest(bytes.NewReader(body), &req) != nil ||
		strings.TrimSpace(req.Reason) == "" || utf8.RuneCountInString(req.Reason) > 4000 || strings.TrimSpace(actor) == "" ||
		(!(req.Kind == "risk" && (req.Value == "watch" || req.Value == "high" || req.Value == "cleared")) &&
			!(req.Kind == "critical" && (req.Value == "marked" || req.Value == "cleared"))) {
		return nil, invalid
	}
	var mark *postgres.NodeMark
	var err error
	if manager {
		var expected string
		invalidBaseline := connect.NewError(connect.CodeInvalidArgument, errors.New("expectedMarkId must be a string: empty for no prior mark, otherwise a positive decimal ID"))
		if len(req.ExpectedMarkID) == 0 || string(req.ExpectedMarkID) == "null" || json.Unmarshal(req.ExpectedMarkID, &expected) != nil {
			return nil, invalidBaseline
		}
		if expected != "" {
			id, parseErr := strconv.ParseInt(expected, 10, 64)
			if parseErr != nil || id < 1 || strconv.FormatInt(id, 10) != expected {
				return nil, invalidBaseline
			}
		}
		mark, err = store.SetNodeMarkIfCurrent(ctx, slug, req.Kind, req.Value, req.Reason, actor, expected)
	} else {
		if len(req.ExpectedMarkID) != 0 {
			return nil, invalid
		}
		mark, err = store.SetNodeMark(ctx, slug, req.Kind, req.Value, req.Reason, actor)
	}
	if err != nil {
		return nil, fmt.Errorf("workbench command: %w", err)
	}
	encoded, encodeErr := json.Marshal(mark)
	if encodeErr != nil {
		return encoded, fmt.Errorf("workbench command: encode response: %w", encodeErr)
	}
	return encoded, nil
}
