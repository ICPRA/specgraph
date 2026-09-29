// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

// Loopback-trusted read surface for the project workbench page (plan v2 §4
// ReadCurrentView slice). Mounted unauthenticated alongside /loop/: this
// deployment binds 127.0.0.1, so local callers are the trust boundary, and no
// bearer token ever reaches the browser page.
//
// Extension note (fork): upstream has no equivalent; these aggregate store
// reads server-side so the client stays on plain relative fetches.

func specSummary(s *storage.Spec) map[string]any {
	return map[string]any{
		"slug":      s.Slug,
		"title":     s.Intent,
		"stage":     string(s.Stage),
		"role":      string(s.Role),
		"priority":  string(s.Priority),
		"version":   s.Version,
		"updatedAt": s.UpdatedAt,
	}
}

type workbenchReadBackend interface {
	storage.ScopedBackend
	RunReadSnapshot(context.Context, func(context.Context) error) error
	ReadWorkbenchMetadata(context.Context) (*postgres.WorkbenchMetadata, error)
	ReadSpecSourceRefs(context.Context, string) (map[string]string, error)
	ListManualCompletions(context.Context, string) ([]storage.ManualCompletion, error)
	ListWorkbenchCompletionHooks(context.Context, string) ([]postgres.WorkbenchCompletionHook, error)
}

func workbenchReadError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	if errors.Is(err, storage.ErrProjectNotFound) || errors.Is(err, storage.ErrSpecNotFound) {
		writeLoopJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "error": "project or spec not found"})
		return
	}
	slog.ErrorContext(r.Context(), "workbench read failed", slog.String("operation", operation), slog.Any("error", err))
	writeLoopJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "error": operation + " failed"})
}

func registerWorkbenchReads(mux *http.ServeMux, root *postgres.Store) {
	registerWorkbenchReadHandlers(mux, root, func(ctx context.Context, project string) (workbenchReadBackend, error) {
		return root.ScopedExisting(ctx, project)
	})
}

func registerWorkbenchReadHandlers(mux *http.ServeMux, projects storage.ProjectBackend, scope func(context.Context, string) (workbenchReadBackend, error)) {
	mux.HandleFunc("GET /wb/projects", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		out, err := ReadWorkbenchProjects(ctx, projects)
		if err != nil {
			workbenchReadError(w, r, "list projects", err)
			return
		}
		writeLoopJSON(w, http.StatusOK, out)
	})

	resolve := func(ctx context.Context, r *http.Request) (workbenchReadBackend, string, error) {
		project := r.Header.Get("X-Specgraph-Project")
		if project == "" {
			project = defaultWorkbenchProject
		}
		if project == "_server" {
			return nil, project, storage.ErrProjectNotFound
		}
		store, err := scope(ctx, project)
		return store, project, err
	}
	mux.HandleFunc("GET /wb/current-view", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		store, project, err := resolve(ctx, r)
		if err != nil {
			workbenchReadError(w, r, "scope store", err)
			return
		}
		out, operation, err := ReadWorkbenchCurrentView(ctx, store, project)
		if err != nil {
			workbenchReadError(w, r, operation, err)
			return
		}
		writeLoopJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("GET /wb/specs/{slug}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		store, project, err := resolve(ctx, r)
		if err != nil {
			workbenchReadError(w, r, "scope store", err)
			return
		}
		slug := r.PathValue("slug")
		out, operation, err := ReadWorkbenchSpec(ctx, store, project, slug)
		if err != nil {
			workbenchReadError(w, r, operation, err)
			return
		}
		writeLoopJSON(w, http.StatusOK, out)
	})
}

// ReadWorkbenchProjects projects stored projects for either workbench transport.
func ReadWorkbenchProjects(ctx context.Context, projects storage.ProjectBackend) (map[string]any, error) {
	items, err := projects.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("workbench read: %w", err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, project := range items {
		if project.Slug != "_server" {
			out = append(out, map[string]any{"slug": project.Slug, "managed": project.Managed})
		}
	}
	return map[string]any{"projects": out}, nil
}

// ReadWorkbenchCurrentView reads the existing aggregate in one snapshot.
// The returned operation names the failing read without exposing driver details.
// Transports must advertise only capabilities they actually implement.
func ReadWorkbenchCurrentView(ctx context.Context, store workbenchReadBackend, project string) (view map[string]any, operation string, readErr error) {
	var (
		specs     []*storage.Spec
		decisions []*storage.Decision
		graph     *storage.FullGraph
		metadata  *postgres.WorkbenchMetadata
		ready     []storage.NodeRef
	)
	operation = "read snapshot"
	err := store.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		var err error
		specs, err = store.ListSpecs(txCtx, "", "", 0)
		if err != nil {
			operation = "list specs"
			return fmt.Errorf("%s: %w", operation, err)
		}
		decisions, err = store.ListDecisions(txCtx, storage.DecisionStatus(""), 0)
		if err != nil {
			operation = "list decisions"
			return fmt.Errorf("%s: %w", operation, err)
		}
		graph, err = store.GetFullGraph(txCtx)
		if err != nil {
			operation = "read graph"
			return fmt.Errorf("%s: %w", operation, err)
		}
		metadata, err = store.ReadWorkbenchMetadata(txCtx)
		if err != nil {
			operation = "read run metadata"
			return fmt.Errorf("%s: %w", operation, err)
		}
		ready, err = store.GetReady(txCtx)
		if err != nil {
			operation = "read ready specs"
			return fmt.Errorf("%s: %w", operation, err)
		}
		return nil
	})
	if err != nil {
		return nil, operation, fmt.Errorf("%s: %w", operation, err)
	}
	readySlugs := make([]string, 0, len(ready))
	for _, ref := range ready {
		readySlugs = append(readySlugs, ref.Slug)
	}
	out := make([]map[string]any, 0, len(specs))
	for _, s := range specs {
		out = append(out, specSummary(s))
	}
	nodes := make([]map[string]any, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		nodes = append(nodes, map[string]any{"slug": node.Slug, "label": node.Label, "stage": node.Stage, "title": node.Intent, "priority": node.Priority})
	}
	edges := make([]map[string]any, 0, len(graph.Edges))
	for _, edge := range graph.Edges {
		edges = append(edges, map[string]any{"from": edge.FromID, "to": edge.ToID, "type": edge.EdgeType})
	}
	decisionOut := make([]map[string]any, 0, len(decisions))
	for _, d := range decisions {
		decisionOut = append(decisionOut, map[string]any{
			"slug":   d.Slug,
			"title":  d.Title,
			"status": string(d.Status),
		})
	}
	return map[string]any{
		"project":     project,
		"generatedAt": time.Now().UTC().Format(time.RFC3339),
		"basis": map[string]any{
			"source": "SpecGraph store (loopback-trusted /wb reads)",
		},
		"specs":           out,
		"readySpecSlugs":  readySlugs,
		"decisions":       decisionOut,
		"graph":           map[string]any{"nodes": nodes, "edges": edges},
		"projectBinding":  metadata.ProjectBinding,
		"runs":            metadata.Runs,
		"deliveries":      metadata.Deliveries,
		"evidence":        metadata.Evidence,
		"acceptances":     metadata.Acceptances,
		"nodeMarks":       metadata.NodeMarks,
		"nodeOwners":      metadata.NodeOwners,
		"reviewStates":    metadata.ReviewStates,
		"nodeEventCounts": metadata.NodeEventCounts,
		"deliveryHooks":   metadata.DeliveryHooks,
		"capabilities":    map[string]bool{"independentVerification": false, "discussionAuthorization": false, "manualCompletion": true, "mailInspection": true, "dependencyEditing": true, "dependencyRemoval": true, "changePreview": true, "deliveryHooks": true},
	}, "", nil
}

// ReadWorkbenchSpec reads the complete existing detail projection in one snapshot.
// The returned operation names the failing read without exposing driver details.
func ReadWorkbenchSpec(ctx context.Context, store workbenchReadBackend, project, slug string) (detail map[string]any, operation string, readErr error) {
	var (
		spec              *storage.Spec
		deps              []storage.NodeRef
		impact            []storage.NodeRef
		changes           []*storage.ChangeLogEntry
		events            []*storage.ExecutionEvent
		manualCompletions []storage.ManualCompletion
		completionHooks   []postgres.WorkbenchCompletionHook
		sourceRefs        map[string]string
	)
	operation = "read snapshot"
	err := store.RunReadSnapshot(ctx, func(txCtx context.Context) error {
		var err error
		spec, err = store.GetSpec(txCtx, slug)
		if err != nil {
			operation = "read spec"
			return fmt.Errorf("%s: %w", operation, err)
		}
		sourceRefs, err = store.ReadSpecSourceRefs(txCtx, slug)
		if err != nil {
			operation = "read source refs"
			return fmt.Errorf("%s: %w", operation, err)
		}
		deps, err = store.GetDependencies(txCtx, slug)
		if err != nil {
			operation = "read dependencies"
			return fmt.Errorf("%s: %w", operation, err)
		}
		impact, err = store.GetImpact(txCtx, slug)
		if err != nil {
			operation = "read impact"
			return fmt.Errorf("%s: %w", operation, err)
		}
		changes, err = store.ListChanges(txCtx, slug, storage.ChangeLogFilter{Limit: 20})
		if err != nil {
			operation = "read changes"
			return fmt.Errorf("%s: %w", operation, err)
		}
		events, err = store.GetExecutionEvents(txCtx, slug, 20)
		if err != nil {
			operation = "read execution events"
			return fmt.Errorf("%s: %w", operation, err)
		}
		manualCompletions, err = store.ListManualCompletions(txCtx, slug)
		if err != nil {
			operation = "read manual completions"
			return fmt.Errorf("%s: %w", operation, err)
		}
		completionHooks, err = store.ListWorkbenchCompletionHooks(txCtx, slug)
		if err != nil {
			operation = "read completion hooks"
			return fmt.Errorf("%s: %w", operation, err)
		}
		return nil
	})
	if err != nil {
		return nil, operation, fmt.Errorf("%s: %w", operation, err)
	}
	depOut := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		depOut = append(depOut, map[string]any{"slug": d.Slug, "nodeId": d.ID, "title": string(d.Label)})
	}
	impactOut := make([]map[string]any, 0, len(impact))
	for _, d := range impact {
		impactOut = append(impactOut, map[string]any{"slug": d.Slug, "title": string(d.Label)})
	}
	changeOut := make([]map[string]any, 0, len(changes))
	for _, c := range changes {
		changeOut = append(changeOut, map[string]any{
			"version":   c.Version,
			"summary":   c.Summary,
			"reason":    c.Reason,
			"createdAt": c.Date,
		})
	}
	eventOut := make([]map[string]any, 0, len(events))
	for _, e := range events {
		eventOut = append(eventOut, map[string]any{
			"id":        e.ID,
			"agent":     e.Agent,
			"type":      e.Type.String(),
			"message":   e.Message,
			"createdAt": e.CreatedAt,
		})
	}
	return map[string]any{
		"project":    project,
		"slug":       slug,
		"sourceRefs": sourceRefs,
		"spec": map[string]any{
			"id":             spec.ID,
			"slug":           spec.Slug,
			"intent":         spec.Intent,
			"stage":          string(spec.Stage),
			"role":           string(spec.Role),
			"priority":       string(spec.Priority),
			"complexity":     string(spec.Complexity),
			"version":        spec.Version,
			"createdAt":      spec.CreatedAt,
			"updatedAt":      spec.UpdatedAt,
			"provenanceType": string(spec.Provenance),
			"contentHash":    spec.ContentHash,
			"notes":          spec.Notes,
			"shape":          spec.ShapeOutput,
			"specify":        spec.SpecifyOutput,
		},
		"dependencies":      depOut,
		"impactedBy":        impactOut,
		"changes":           changeOut,
		"events":            eventOut,
		"manualCompletions": manualCompletions,
		"completionHooks":   completionHooks,
	}, "", nil
}
