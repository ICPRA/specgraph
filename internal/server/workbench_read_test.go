// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
)

type workbenchReadStub struct {
	storage.ScopedBackend
	fail            string
	project         string
	limit           int
	spec            *storage.Spec
	snapshotCtx     context.Context
	snapshotCalls   int
	snapshotReads   int
	completionHooks []postgres.WorkbenchCompletionHook
	completionSlug  string
}

var errWorkbenchRead = errors.New("private database error")

func (s *workbenchReadStub) RunReadSnapshot(ctx context.Context, fn func(context.Context) error) error {
	s.snapshotCalls++
	if s.fail == "snapshot begin" {
		return errWorkbenchRead
	}
	txCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.snapshotCtx = txCtx
	defer func() { s.snapshotCtx = nil }()
	if err := fn(txCtx); err != nil {
		return err
	}
	if s.fail == "snapshot commit" {
		return errWorkbenchRead
	}
	return nil
}

func (s *workbenchReadStub) ListManualCompletions(ctx context.Context, _ string) ([]storage.ManualCompletion, error) {
	return []storage.ManualCompletion{}, s.failure(ctx, "manual completions")
}

func (s *workbenchReadStub) ListWorkbenchCompletionHooks(ctx context.Context, slug string) ([]postgres.WorkbenchCompletionHook, error) {
	s.completionSlug = slug
	items := s.completionHooks
	if items == nil {
		items = []postgres.WorkbenchCompletionHook{}
	}
	return items, s.failure(ctx, "completion hooks")
}

func (s *workbenchReadStub) failure(ctx context.Context, operation string) error {
	if operation != "projects" {
		if s.snapshotCtx == nil || ctx != s.snapshotCtx {
			return errors.New("read outside snapshot context")
		}
		s.snapshotReads++
	}
	if s.fail == operation {
		return errWorkbenchRead
	}
	return nil
}

func (s *workbenchReadStub) ListProjects(ctx context.Context) ([]*storage.Project, error) {
	return []*storage.Project{{Slug: "_server"}, {Slug: "alpha", Managed: true}, {Slug: "beta"}}, s.failure(ctx, "projects")
}

func (s *workbenchReadStub) ListSpecs(ctx context.Context, _, _ string, limit int) ([]*storage.Spec, error) {
	s.limit = limit
	return []*storage.Spec{{Slug: s.project + "-task", Intent: "Task", Stage: "approved", UpdatedAt: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)}}, s.failure(ctx, "specs")
}

func (s *workbenchReadStub) ListDecisions(ctx context.Context, _ storage.DecisionStatus, _ int) ([]*storage.Decision, error) {
	return nil, s.failure(ctx, "decisions")
}

func (s *workbenchReadStub) GetReady(ctx context.Context) ([]storage.NodeRef, error) {
	return []storage.NodeRef{{Slug: s.project + "-task", Label: storage.NodeLabelSpec}}, s.failure(ctx, "ready")
}

func (s *workbenchReadStub) GetFullGraph(ctx context.Context) (*storage.FullGraph, error) {
	return &storage.FullGraph{Nodes: []storage.GraphNode{{Slug: s.project + "-task", Label: storage.NodeLabelSpec, Intent: "Task"}}, Edges: []*storage.Edge{{FromID: s.project + "-task", ToID: "upstream", EdgeType: storage.EdgeTypeDependsOn}}}, s.failure(ctx, "graph")
}

func (s *workbenchReadStub) ReadWorkbenchMetadata(ctx context.Context) (*postgres.WorkbenchMetadata, error) {
	return &postgres.WorkbenchMetadata{Runs: []postgres.WorkbenchRun{{ID: "run", TaskSlug: s.project + "-task", State: "bound"}}, Deliveries: []postgres.WorkbenchDelivery{}, Evidence: []postgres.WorkbenchEvidence{{ID: "proof", ExitCode: nil}}, Acceptances: []postgres.WorkbenchAcceptance{}, DeliveryHooks: []postgres.WorkbenchDeliveryHook{}}, s.failure(ctx, "metadata")
}

func (s *workbenchReadStub) ReadSpecSourceRefs(ctx context.Context, _ string) (map[string]string, error) {
	return map[string]string{"intent": "cl-intent-source"}, s.failure(ctx, "source refs")
}

func (s *workbenchReadStub) GetSpec(ctx context.Context, _ string) (*storage.Spec, error) {
	if s.spec != nil {
		return s.spec, s.failure(ctx, "spec")
	}
	return &storage.Spec{Slug: "task"}, s.failure(ctx, "spec")
}
func (s *workbenchReadStub) GetDependencies(ctx context.Context, _ string) ([]storage.NodeRef, error) {
	return nil, s.failure(ctx, "dependencies")
}
func (s *workbenchReadStub) GetImpact(ctx context.Context, _ string) ([]storage.NodeRef, error) {
	return nil, s.failure(ctx, "impact")
}
func (s *workbenchReadStub) ListChanges(ctx context.Context, _ string, _ storage.ChangeLogFilter) ([]*storage.ChangeLogEntry, error) {
	return nil, s.failure(ctx, "changes")
}
func (s *workbenchReadStub) GetExecutionEvents(ctx context.Context, _ string, _ int) ([]*storage.ExecutionEvent, error) {
	return []*storage.ExecutionEvent{{Type: storage.ExecutionEventTypeProgress}}, s.failure(ctx, "events")
}

func workbenchReadTestMux(s *workbenchReadStub) *http.ServeMux {
	mux := http.NewServeMux()
	registerWorkbenchReadHandlers(mux, s, func(_ context.Context, project string) (workbenchReadBackend, error) {
		if project == "missing" {
			return nil, storage.ErrProjectNotFound
		}
		if project == "broken" {
			return nil, errWorkbenchRead
		}
		s.project = project
		return s, nil
	})
	return mux
}

func TestWorkbenchCurrentView(t *testing.T) {
	for _, project := range []string{"alpha", "beta", ""} {
		t.Run(project, func(t *testing.T) {
			s := &workbenchReadStub{}
			r := httptest.NewRequest(http.MethodGet, "/wb/current-view", nil)
			r.Header.Set("X-Specgraph-Project", project)
			w := httptest.NewRecorder()
			workbenchReadTestMux(s).ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if s.snapshotCalls != 1 || s.snapshotReads != 5 {
				t.Fatalf("want five reads in one snapshot, got %d reads in %d snapshots", s.snapshotReads, s.snapshotCalls)
			}
			var got map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			wantProject := project
			if wantProject == "" {
				wantProject = defaultWorkbenchProject
			}
			if got["project"] != wantProject || s.project != wantProject || s.limit != 0 {
				t.Fatalf("scope/limit mismatch: %v, %+v", got["project"], s)
			}
			ready := got["readySpecSlugs"].([]any)
			if len(ready) != 1 || ready[0] != wantProject+"-task" {
				t.Fatalf("ready specs not scoped: %v", ready)
			}
			if _, ok := got["tasks"]; ok {
				t.Fatal("spec stages must not masquerade as execution tasks")
			}
			if _, err := time.Parse(time.RFC3339, got["generatedAt"].(string)); err != nil {
				t.Fatal(err)
			}
			spec := got["specs"].([]any)[0].(map[string]any)
			if spec["slug"] != wantProject+"-task" || spec["updatedAt"] == nil {
				t.Fatalf("spec metadata: %v", spec)
			}
			graph := got["graph"].(map[string]any)
			edge := graph["edges"].([]any)[0].(map[string]any)
			if edge["from"] != wantProject+"-task" || edge["to"] != "upstream" || edge["type"] != "DEPENDS_ON" {
				t.Fatalf("edge: %v", edge)
			}
			if got["runs"].([]any)[0].(map[string]any)["state"] != "bound" {
				t.Fatal("raw run state changed")
			}
			if got["evidence"].([]any)[0].(map[string]any)["exitCode"] != nil {
				t.Fatal("NULL exit code became success")
			}
			for _, key := range []string{"deliveries", "acceptances", "decisions", "deliveryHooks"} {
				if items, ok := got[key].([]any); !ok || len(items) != 0 {
					t.Fatalf("%s must be []", key)
				}
			}
			for key, value := range got["capabilities"].(map[string]any) {
				if value != (key == "manualCompletion" || key == "mailInspection" || key == "dependencyEditing" || key == "dependencyRemoval" || key == "changePreview" || key == "deliveryHooks") {
					t.Fatal("unsupported capability advertised")
				}
			}
		})
	}
}

func TestWorkbenchReadFailures(t *testing.T) {
	for _, test := range []struct{ path, failure string }{
		{"/wb/projects", "projects"}, {"/wb/current-view", "specs"}, {"/wb/current-view", "decisions"},
		{"/wb/current-view", "graph"}, {"/wb/current-view", "metadata"}, {"/wb/current-view", "ready"}, {"/wb/specs/task", "spec"},
		{"/wb/specs/task", "dependencies"}, {"/wb/specs/task", "impact"}, {"/wb/specs/task", "changes"}, {"/wb/specs/task", "events"},
		{"/wb/specs/task", "manual completions"},
		{"/wb/specs/task", "completion hooks"},
	} {
		t.Run(test.failure, func(t *testing.T) {
			stub := &workbenchReadStub{fail: test.failure}
			var readErr error
			switch test.path {
			case "/wb/projects":
				_, readErr = ReadWorkbenchProjects(context.Background(), stub)
			case "/wb/current-view":
				_, _, readErr = ReadWorkbenchCurrentView(context.Background(), stub, "fixture")
			default:
				_, _, readErr = ReadWorkbenchSpec(context.Background(), stub, "fixture", "task")
			}
			if !errors.Is(readErr, errWorkbenchRead) {
				t.Fatalf("wrapped read lost its cause: %v", readErr)
			}
			w := httptest.NewRecorder()
			workbenchReadTestMux(&workbenchReadStub{fail: test.failure}).ServeHTTP(w, httptest.NewRequest(http.MethodGet, test.path, nil))
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("failed read returned %d: %s", w.Code, w.Body.String())
			}
			var got map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got["code"] != "internal" || got["error"] == errWorkbenchRead.Error() {
				t.Fatalf("error contract: %v", got)
			}
		})
	}
	for _, path := range []string{"/wb/current-view", "/wb/specs/task"} {
		for _, project := range []string{"missing", "_server", "broken"} {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, path, nil)
			r.Header.Set("X-Specgraph-Project", project)
			workbenchReadTestMux(&workbenchReadStub{}).ServeHTTP(w, r)
			want := http.StatusNotFound
			if project == "broken" {
				want = http.StatusInternalServerError
			}
			if w.Code != want {
				t.Fatalf("%s %s: status %d", path, project, w.Code)
			}
		}
	}
}

func TestWorkbenchSnapshotFailures(t *testing.T) {
	for _, endpoint := range []struct {
		path  string
		reads int
	}{{"/wb/current-view", 5}, {"/wb/specs/task", 8}} {
		for _, failure := range []string{"snapshot begin", "snapshot commit"} {
			t.Run(endpoint.path+"/"+failure, func(t *testing.T) {
				s := &workbenchReadStub{fail: failure}
				w := httptest.NewRecorder()
				workbenchReadTestMux(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, endpoint.path, nil))
				if w.Code != http.StatusInternalServerError || w.Body.String() != "{\"code\":\"internal\",\"error\":\"read snapshot failed\"}\n" {
					t.Fatalf("snapshot failure leaked a response: %d %s", w.Code, w.Body.String())
				}
				wantReads := endpoint.reads
				if failure == "snapshot begin" {
					wantReads = 0
				}
				if s.snapshotCalls != 1 || s.snapshotReads != wantReads {
					t.Fatalf("want %d reads in one snapshot, got %d reads in %d snapshots", wantReads, s.snapshotReads, s.snapshotCalls)
				}
			})
		}
	}
}

func TestWorkbenchProjects(t *testing.T) {
	w := httptest.NewRecorder()
	s := &workbenchReadStub{}
	workbenchReadTestMux(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/wb/projects", nil))
	if s.snapshotCalls != 0 {
		t.Fatal("single-query project listing must not open a snapshot")
	}
	if w.Code != http.StatusOK || w.Body.String() != "{\"projects\":[{\"managed\":true,\"slug\":\"alpha\"},{\"managed\":false,\"slug\":\"beta\"}]}\n" {
		t.Fatalf("project listing: %d %s", w.Code, w.Body.String())
	}
}

func TestWorkbenchSpecDetail(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/wb/specs/task", nil)
	r.Header.Set("X-Specgraph-Project", "alpha")
	s := &workbenchReadStub{}
	workbenchReadTestMux(s).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("detail: %d %s", w.Code, w.Body.String())
	}
	if s.snapshotCalls != 1 || s.snapshotReads != 8 || s.completionSlug != "task" {
		t.Fatalf("want eight source-scoped reads in one snapshot, got %d reads in %d snapshots for %q", s.snapshotReads, s.snapshotCalls, s.completionSlug)
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["sourceRefs"].(map[string]any)["intent"] != "cl-intent-source" {
		t.Fatal("source identity was not projected")
	}
	if got["project"] != "alpha" || got["events"].([]any)[0].(map[string]any)["type"] != "progress" {
		t.Fatalf("detail metadata: %v", got)
	}
	spec := got["spec"].(map[string]any)
	for _, field := range []string{"shape", "specify"} {
		if value, exists := spec[field]; !exists || value != nil {
			t.Fatalf("absent %s must be explicit null, got %v", field, spec)
		}
	}
	for _, key := range []string{"dependencies", "impactedBy", "changes", "manualCompletions", "completionHooks"} {
		if items, ok := got[key].([]any); !ok || len(items) != 0 {
			t.Fatalf("%s must be []", key)
		}
	}
}

func TestWorkbenchSpecContract(t *testing.T) {
	shape := &storage.ShapeOutput{ScopeIn: []string{"existing queue"}, ScopeOut: []string{"second scheduler"}}
	specify := &storage.SpecifyOutput{
		VerifyCriteria: []storage.VerifyCriterion{{Category: "integration", Description: "A retry creates one message"}},
		Invariants:     []string{"No cross-project delivery"},
		Interfaces:     []storage.InterfaceSection{{Name: "send", Body: "input -> receipt"}},
		Touches:        []storage.FileTouch{{Path: "mail.go", Purpose: "delivery", ChangeType: "create"}},
	}
	w := httptest.NewRecorder()
	workbenchReadTestMux(&workbenchReadStub{spec: &storage.Spec{Slug: "task", ShapeOutput: shape, SpecifyOutput: specify}}).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/wb/specs/task", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("detail: %d %s", w.Code, w.Body.String())
	}
	var got struct {
		Spec struct {
			Shape   *storage.ShapeOutput   `json:"shape"`
			Specify *storage.SpecifyOutput `json:"specify"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal([]any{shape, specify})
	actual, _ := json.Marshal([]any{got.Spec.Shape, got.Spec.Specify})
	if string(actual) != string(want) {
		t.Fatalf("contract changed or omitted: got %s want %s", actual, want)
	}
}
