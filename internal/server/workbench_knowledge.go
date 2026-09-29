// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// ExecuteLocalWorkbenchKnowledge requires the operation's read authorization from the caller.
// Project and native identity are supplied by the trusted host, never tool arguments.
// Reading does not establish mail bindings, run responsibility, or role assignments.
func ExecuteLocalWorkbenchKnowledge(ctx context.Context, root *postgres.Store, project, operation string, body io.Reader) (any, error) {
	var environmentID, nativeProjectID string
	var expectedProject string
	var read func(*postgres.Store) (any, error)
	invalid := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid knowledge request"))
	switch operation {
	case "node-events":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.TaskSlug) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadNodeEvents(ctx, req.TaskSlug, req.Cursor) }
	case "report-judgment-history":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			FlowID          string `json:"flowId"`
			ConditionKey    string `json:"conditionKey"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.FlowID) == "" || strings.TrimSpace(req.ConditionKey) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			return s.ReadReportBranchJudgmentHistory(ctx, req.FlowID, req.ConditionKey, req.Cursor)
		}
	case "candidate-loop-read":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			RunID           string `json:"runId"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadCandidateLoop(ctx, req.RunID) }
	case "node-owner-read", "node-owner-history":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.TaskSlug) == "" || (operation == "node-owner-read" && req.Cursor != "") {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			if operation == "node-owner-read" {
				return s.ReadNodeOwnership(ctx, req.TaskSlug)
			}
			return s.ReadNodeOwnershipHistory(ctx, req.TaskSlug, req.Cursor)
		}
	case "candidate-satisfaction-history":
		var req struct {
			EnvironmentID      string `json:"environment_id"`
			NativeProjectID    string `json:"native_project_id"`
			RunID              string `json:"runId"`
			AttemptID          string `json:"attemptId"`
			JudgmentCursor     string `json:"judgmentCursor"`
			InterventionCursor string `json:"interventionCursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.AttemptID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			return s.ReadCandidateSatisfactionHistory(ctx, req.RunID, req.AttemptID, req.JudgmentCursor, req.InterventionCursor)
		}
	case "report-join-read":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			RunID           string `json:"runId"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadReportFlowJoin(ctx, req.RunID) }
	case "report-flow-read":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			FlowID          string `json:"flowId"`
			RunID           string `json:"runId"`
		}
		if decodeMailRequest(body, &req) != nil || (req.FlowID == "") == (req.RunID == "") {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			if req.FlowID != "" {
				return s.ReadReportBranchFlow(ctx, req.FlowID)
			}
			return s.ReadReportBranchFlowByRun(ctx, req.RunID)
		}
	case "conversation-runs":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			ThreadID        string `json:"threadId"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.ThreadID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			return s.ReadConversationRuns(ctx, environmentID, nativeProjectID, req.ThreadID, req.Cursor)
		}
	case "program-loop-read":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			RunID           string `json:"runId"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadProgramLoop(ctx, req.RunID) }
	case "program-loop-history":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			RunID           string `json:"runId"`
			AttemptCursor   string `json:"attemptCursor"`
			EventCursor     string `json:"eventCursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.RunID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			return s.ReadProgramLoopHistory(ctx, req.RunID, req.AttemptCursor, req.EventCursor)
		}
	case "graph-current":
		var req struct {
			EnvironmentID   string          `json:"environment_id"`
			NativeProjectID string          `json:"native_project_id"`
			Offset          json.RawMessage `json:"offset"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, invalid
		}
		offset := 0
		if len(req.Offset) != 0 && (string(req.Offset) == "null" || json.Unmarshal(req.Offset, &offset) != nil || offset < 0 || offset > 2147483647) {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		// ponytail: each page re-reads the full current graph; use source-side paging if measured scale requires it.
		read = func(s *postgres.Store) (any, error) {
			var graph *storage.FullGraph
			if err := s.RunReadSnapshot(ctx, func(snapshotCtx context.Context) error {
				var err error
				graph, err = s.GetFullGraph(snapshotCtx)
				if err != nil {
					err = fmt.Errorf("workbench knowledge: %w", err)
				}
				return err
			}); err != nil {
				return nil, fmt.Errorf("workbench knowledge: %w", err)
			}
			return projectCurrentGraph(graph, offset), nil
		}
	case "dependency-state":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
		}
		if decodeMailRequest(body, &req) != nil || validateSlug(req.TaskSlug) != nil {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadDependencyEditState(ctx, req.TaskSlug) }
	case "node-mark-history":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || validateSlug(req.TaskSlug) != nil {
			return nil, invalid
		}
		var beforeID int64
		if req.Cursor != "" {
			var err error
			beforeID, err = strconv.ParseInt(req.Cursor, 10, 64)
			if err != nil || beforeID < 1 || strconv.FormatInt(beforeID, 10) != req.Cursor {
				return nil, invalid
			}
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadNodeMarkHistory(ctx, req.TaskSlug, beforeID) }
	case "project-binding-history":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, invalid
		}
		var beforeID int64
		if req.Cursor != "" {
			var err error
			beforeID, err = strconv.ParseInt(req.Cursor, 10, 64)
			if err != nil || beforeID < 1 || strconv.FormatInt(beforeID, 10) != req.Cursor {
				return nil, invalid
			}
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		// The closure runs after project resolution below; project is final by then.
		read = func(s *postgres.Store) (any, error) { return s.ProjectBindingHistory(ctx, project, beforeID) }
	case "node-deliveries":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || validateSlug(req.TaskSlug) != nil {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadNodeDeliveries(ctx, req.TaskSlug, req.Cursor) }
	case "test-results":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			DeliveryID      string `json:"deliveryId"`
			Cursor          string `json:"cursor"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.DeliveryID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadTestReports(ctx, req.DeliveryID, req.Cursor) }
	case "review-request-source":
		var req struct {
			EnvironmentID   string          `json:"environment_id"`
			NativeProjectID string          `json:"native_project_id"`
			RequestID       json.RawMessage `json:"requestId"`
			DecisionID      json.RawMessage `json:"decisionId"`
		}
		if decodeMailRequest(body, &req) != nil || (len(req.RequestID) == 0) == (len(req.DecisionID) == 0) {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		var id string
		if len(req.RequestID) > 0 {
			if json.Unmarshal(req.RequestID, &id) != nil || id == "" {
				return nil, invalid
			}
			read = func(s *postgres.Store) (any, error) { return s.ReadReviewRequest(ctx, id) }
		} else {
			if json.Unmarshal(req.DecisionID, &id) != nil || id == "" {
				return nil, invalid
			}
			read = func(s *postgres.Store) (any, error) { return s.ReadReviewDecisionRequest(ctx, id) }
		}
	case "review-status":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			TaskSlug        string `json:"taskSlug"`
		}
		if decodeMailRequest(body, &req) != nil || validateSlug(req.TaskSlug) != nil {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadReviewStatus(ctx, req.TaskSlug) }
	case "review-delivery-source":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			DeliveryID      string `json:"deliveryId"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.DeliveryID) == "" {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.ReadWorkbenchDelivery(ctx, req.DeliveryID) }
	case "knowledge-search":
		var req struct {
			EnvironmentID   string `json:"environment_id"`
			NativeProjectID string `json:"native_project_id"`
			Query           string `json:"query"`
		}
		if decodeMailRequest(body, &req) != nil {
			return nil, invalid
		}
		req.Query = strings.TrimSpace(req.Query)
		if req.Query == "" || utf8.RuneCountInString(req.Query) > 256 {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) { return s.SearchWorkbenchKnowledge(ctx, req.Query) }
	case "knowledge-record":
		var req struct {
			EnvironmentID   string          `json:"environment_id"`
			NativeProjectID string          `json:"native_project_id"`
			Kind            string          `json:"kind"`
			Reference       string          `json:"reference"`
			ExpectedProject json.RawMessage `json:"expectedProject"`
		}
		if decodeMailRequest(body, &req) != nil || strings.TrimSpace(req.Reference) == "" ||
			(req.Kind != "spec" && req.Kind != "decision" && req.Kind != "change") {
			return nil, invalid
		}
		if len(req.ExpectedProject) != 0 && (json.Unmarshal(req.ExpectedProject, &expectedProject) != nil || strings.TrimSpace(expectedProject) == "") {
			return nil, invalid
		}
		environmentID, nativeProjectID = req.EnvironmentID, req.NativeProjectID
		read = func(s *postgres.Store) (any, error) {
			return s.ReadWorkbenchKnowledgeRecord(ctx, req.Kind, req.Reference)
		}
	default:
		return nil, invalid
	}
	if strings.TrimSpace(environmentID) == "" || strings.TrimSpace(nativeProjectID) == "" {
		return nil, invalid
	}
	if operation == "node-owner-read" || operation == "node-owner-history" || operation == "report-flow-read" || operation == "report-join-read" || operation == "candidate-loop-read" || operation == "candidate-satisfaction-history" || operation == "report-judgment-history" || operation == "node-events" {
		resolved, err := root.ProjectForWorkbenchKnowledge(ctx, environmentID, nativeProjectID)
		if err != nil {
			return nil, fmt.Errorf("workbench knowledge: %w", err)
		}
		if project != "" && project != resolved {
			return nil, storage.ErrProjectNotFound
		}
		project = resolved
	}
	if project == "" {
		var err error
		project, err = root.ProjectForWorkbenchKnowledge(ctx, environmentID, nativeProjectID)
		if err != nil {
			return nil, fmt.Errorf("workbench knowledge: %w", err)
		}
	}
	if project == "_server" {
		return nil, storage.ErrProjectNotFound
	}
	scoped, err := root.ScopedExisting(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("workbench knowledge: %w", err)
	}
	if expectedProject != "" && expectedProject != project {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("knowledge record project changed: expected %q, actual %q", expectedProject, project))
	}
	return read(scoped)
}

type currentGraphNode struct {
	Slug     string `json:"slug"`
	Label    string `json:"label"`
	Stage    string `json:"stage"`
	Priority string `json:"priority"`
}

type currentGraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

type currentGraphPage struct {
	Nodes      []currentGraphNode `json:"nodes"`
	Edges      []currentGraphEdge `json:"edges"`
	TotalNodes int                `json:"totalNodes"`
	TotalEdges int                `json:"totalEdges"`
	HasMore    bool               `json:"hasMore"`
	NextOffset *int               `json:"nextOffset"`
}

func projectCurrentGraph(graph *storage.FullGraph, offset int) currentGraphPage {
	sort.Slice(graph.Nodes, func(i, j int) bool {
		if graph.Nodes[i].Slug == graph.Nodes[j].Slug {
			return graph.Nodes[i].Label < graph.Nodes[j].Label
		}
		return graph.Nodes[i].Slug < graph.Nodes[j].Slug
	})
	sort.Slice(graph.Edges, func(i, j int) bool {
		a, b := graph.Edges[i], graph.Edges[j]
		if a.FromID != b.FromID {
			return a.FromID < b.FromID
		}
		if a.ToID != b.ToID {
			return a.ToID < b.ToID
		}
		return a.EdgeType < b.EdgeType
	})
	page := currentGraphPage{
		Nodes:      []currentGraphNode{},
		Edges:      []currentGraphEdge{},
		TotalNodes: len(graph.Nodes),
		TotalEdges: len(graph.Edges),
	}
	if offset < len(graph.Nodes) {
		for _, n := range graph.Nodes[offset:min(offset+100, len(graph.Nodes))] {
			page.Nodes = append(page.Nodes, currentGraphNode{n.Slug, string(n.Label), n.Stage, n.Priority})
		}
	}
	if offset < len(graph.Edges) {
		for _, e := range graph.Edges[offset:min(offset+100, len(graph.Edges))] {
			page.Edges = append(page.Edges, currentGraphEdge{e.FromID, e.ToID, string(e.EdgeType)})
		}
	}
	page.HasMore = offset+100 < max(page.TotalNodes, page.TotalEdges)
	if page.HasMore {
		next := offset + 100
		page.NextOffset = &next
	}
	return page
}
