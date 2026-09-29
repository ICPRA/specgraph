// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchSourceReviewLocalPostgres(t *testing.T) {
	ctx := context.Background()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("review-command"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(ctx)) })
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	credentials := map[string]string{}
	for _, role := range []string{"reader", "admin"} {
		human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Review " + role, Role: role}, nil)
		require.NoError(t, err)
		secret, hash, err := auth.GenerateAPIKeySecret()
		require.NoError(t, err)
		key, err := users.CreateAPIKey(ctx, &storage.APIKey{UserID: human.ID, PHCHash: hash})
		require.NoError(t, err)
		credentials[role] = auth.FormatAPIKeyToken(key.Prefix, secret)
	}
	runs := map[string]string{}
	for _, slug := range []string{"author", "reviewer", "manager"} {
		_, err = s.CreateSpec(ctx, slug, "Source command fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		approved := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
		require.NoError(t, err)
		runs[slug], err = s.PrepareRun(ctx, slug, "workspace-"+slug)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, runs[slug], "local", "thread-"+slug))
	}
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=jsonb_set(body,'{dispatch_target}','{"assignmentRole":"manager","environmentId":"local","projectId":"host-project"}'::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE id=$1)`, runs["manager"])
	require.NoError(t, err)
	project := "review-command"
	call := func(role, operation, slug string, body any) workbenchCommandResponse {
		t.Helper()
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		input, err := json.Marshal(workbenchCommandRequest{ID: "source-review", Operation: operation, Project: project, Slug: slug, Credential: credentials[role], Body: encoded})
		require.NoError(t, err)
		var out bytes.Buffer
		err = runWorkbenchCommandStdio(ctx, bytes.NewReader(append(input, '\n')), &out, func(callCtx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
			identity, err := resolveWorkbenchOperator(callCtx, s, req.Credential, nil, procedure)
			if err != nil {
				return nil, err
			}
			callCtx = auth.WithIdentity(callCtx, identity)
			switch req.Operation {
			case "pm-prepare-run", "pm-bind-run", "pm-authorize-run", "pm-read-preparation":
				action, ok := auth.ActionForProcedure(procedure)
				require.True(t, ok)
				require.Equal(t, "planning-dispatch.write", action)
				return server.ExecuteAgentPlanningDispatch(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
			case "pm-create-node", "pm-subdivide-node", "pm-add-dependency", "pm-remove-dependency", "pm-approve-node", "pm-set-node-mark":
				action, ok := auth.ActionForProcedure(procedure)
				require.True(t, ok)
				require.Equal(t, "planning.write", action)
				return server.ExecuteAgentPlanning(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
			case "review-assign", "review-submit":
				result, err := server.ExecuteAgentReview(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(result)
			case "review-request-source", "review-status", "dependency-state", "node-mark-history":
				result, err := server.ExecuteLocalWorkbenchKnowledge(callCtx, s, req.Project, req.Operation, bytes.NewReader(req.Body))
				if err != nil {
					return nil, err
				}
				return json.Marshal(result)
			default:
				return server.ExecuteWorkbenchCommand(callCtx, s, req.Operation, req.Project, req.Slug, identity.UserID, req.Body)
			}
		})
		require.NoError(t, err)
		var response workbenchCommandResponse
		require.NoError(t, json.Unmarshal(out.Bytes(), &response))
		return response
	}
	scope := func(name string) storage.MailScope {
		return storage.MailScope{EnvironmentID: "local", ThreadID: "thread-" + name, ProviderSessionID: "session", ProviderInstanceID: "codex"}
	}
	t.Run("manager-node-marks", func(t *testing.T) {
		request := map[string]any{"kind": "risk", "value": "watch", "reason": "Synthetic explicit PM evidence", "expectedMarkId": ""}
		body := map[string]any{"scope": scope("manager"), "taskSlug": "author", "request": request}
		project = ""
		recorded := call("reader", "pm-set-node-mark", "", body)
		require.Nil(t, recorded.Error)
		var mark postgres.NodeMark
		require.NoError(t, json.Unmarshal(recorded.Data, &mark))
		require.Equal(t, runs["manager"], mark.Actor)
		project = "review-command"
		for _, name := range []string{"author", "missing"} {
			body["scope"] = scope(name)
			denied := call("reader", "pm-set-node-mark", "", body)
			require.NotNil(t, denied.Error)
			require.Equal(t, "forbidden", denied.Error.Code)
		}
		body["scope"] = scope("manager")
		stale := call("reader", "pm-set-node-mark", "", body)
		require.NotNil(t, stale.Error)
		require.Equal(t, "conflict", stale.Error.Code)
		unrestricted := call("reader", "set-node-mark", "author", map[string]string{"kind": "risk", "value": "high", "reason": "No general management grant"})
		require.NotNil(t, unrestricted.Error)
		require.Equal(t, "forbidden", unrestricted.Error.Code)
		human := call("admin", "set-node-mark", "author", map[string]string{"kind": "risk", "value": "high", "reason": "Synthetic human evidence"})
		require.Nil(t, human.Error)
		read := call("reader", "node-mark-history", "", map[string]string{"environment_id": "local", "native_project_id": "host-project", "taskSlug": "author"})
		require.Nil(t, read.Error)
		var history postgres.NodeMarkHistory
		require.NoError(t, json.Unmarshal(read.Data, &history))
		require.Len(t, history.Items, 2)
		require.Equal(t, "Synthetic human evidence", history.Items[0].Reason)
		require.Equal(t, mark, history.Items[1])
	})
	t.Run("manager-native-dispatch", func(t *testing.T) {
		for _, slug := range []string{"pm-dispatch-child", "pm-dispatch-mutation", "pm-dispatch-foreign", "pm-dispatch-other-actor"} {
			_, err := s.CreateSpec(ctx, slug, "Synthetic dispatch scope", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
			require.NoError(t, err)
			approved := "approved"
			_, err = s.UpdateSpec(ctx, slug, nil, &approved, nil, nil, nil)
			require.NoError(t, err)
		}
		target := map[string]any{"environmentId": "local", "projectId": "host-project", "threadId": "pm-child-thread", "workspace": "pm-child-workspace", "createCommandId": "pm-create", "startCommandId": "pm-start", "messageId": "pm-message", "assignmentRole": "worker", "workPurpose": "requirements", "purposeGuidance": "Synthetic fixture, never start a model"}
		prepare := map[string]any{"task_slug": "pm-dispatch-child", "workspace": "pm-child-workspace", "idempotency_key": "pm-dispatch-key", "dispatch_target": target}
		body := map[string]any{"scope": scope("manager"), "request": prepare}
		project = ""
		absent := call("reader", "pm-read-preparation", "", map[string]any{"scope": scope("manager"), "request": map[string]string{"idempotency_key": "pm-dispatch-key"}})
		require.NotNil(t, absent.Error)
		require.Equal(t, "not_found", absent.Error.Code)
		created := call("reader", "pm-prepare-run", "", body)
		require.Nil(t, created.Error)
		var preparation struct {
			BindingID string `json:"binding_id"`
			Replayed  bool   `json:"replayed"`
		}
		require.NoError(t, json.Unmarshal(created.Data, &preparation))
		require.NotEmpty(t, preparation.BindingID)
		require.False(t, preparation.Replayed)
		runID := preparation.BindingID
		readBody := map[string]any{"scope": scope("manager"), "request": map[string]string{"idempotency_key": "pm-dispatch-key"}}
		read := call("reader", "pm-read-preparation", "", readBody)
		require.Nil(t, read.Error)
		var saved struct {
			Project  string                    `json:"project"`
			Context  storage.RunContext        `json:"context"`
			Dispatch storage.RunDispatchStatus `json:"dispatch"`
		}
		require.NoError(t, json.Unmarshal(read.Data, &saved))
		require.Equal(t, "review-command", saved.Project)
		require.Equal(t, runID, saved.Context.RunID)
		var originalBody struct {
			Target map[string]any `json:"dispatch_target"`
		}
		require.NoError(t, json.Unmarshal(saved.Context.Body, &originalBody))
		require.Equal(t, target, originalBody.Target)
		require.Nil(t, saved.Dispatch.Admission)
		originalContext := saved.Context
		replayed := call("reader", "pm-prepare-run", "", body)
		require.Nil(t, replayed.Error)
		require.NoError(t, json.Unmarshal(replayed.Data, &preparation))
		require.Equal(t, runID, preparation.BindingID)
		require.True(t, preparation.Replayed)
		target["threadId"] = "new-uuid-must-not-replace-original"
		conflict := call("reader", "pm-prepare-run", "", body)
		require.NotNil(t, conflict.Error)
		require.Equal(t, "conflict", conflict.Error.Code)
		target["threadId"] = "pm-child-thread"
		for _, field := range []string{"projectId", "environmentId"} {
			original := target[field]
			target[field] = "foreign"
			denied := call("reader", "pm-prepare-run", "", body)
			require.NotNil(t, denied.Error)
			require.Equal(t, "forbidden", denied.Error.Code)
			target[field] = original
		}
		bind := map[string]any{"scope": scope("manager"), "runId": runID, "request": map[string]string{"environment_id": "local", "thread_ref": "pm-child-thread"}}
		authorize := map[string]any{"scope": scope("manager"), "runId": runID, "request": map[string]any{"packageId": saved.Context.PackageID, "target": target}}
		for operation, request := range map[string]map[string]any{"pm-prepare-run": body, "pm-bind-run": bind, "pm-authorize-run": authorize, "pm-read-preparation": readBody} {
			for _, actor := range []string{"author", "missing"} {
				request["scope"] = scope(actor)
				denied := call("reader", operation, "", request)
				require.NotNil(t, denied.Error)
				require.Equal(t, "forbidden", denied.Error.Code)
			}
			request["scope"] = scope("manager")
			for _, field := range []string{"actor", "role", "project"} {
				request[field] = "forged"
				denied := call("reader", operation, "", request)
				require.NotNil(t, denied.Error)
				require.Equal(t, "invalid_argument", denied.Error.Code)
				delete(request, field)
			}
			for _, state := range []string{"completed", "handed_off"} {
				_, err := s.Pool().Exec(ctx, `UPDATE run_bindings SET state=$1 WHERE id=$2`, state, runs["manager"])
				require.NoError(t, err)
				require.NotNil(t, call("reader", operation, "", request).Error)
			}
			_, err := s.Pool().Exec(ctx, `UPDATE run_bindings SET state='bound' WHERE id=$1`, runs["manager"])
			require.NoError(t, err)
		}
		readBody["runId"] = runID
		require.NotNil(t, call("reader", "pm-read-preparation", "", readBody).Error)
		readBody["request"] = map[string]string{}
		read = call("reader", "pm-read-preparation", "", readBody)
		require.Nil(t, read.Error)
		require.NoError(t, json.Unmarshal(read.Data, &saved))
		require.Equal(t, originalContext, saved.Context)
		require.Nil(t, call("reader", "pm-bind-run", "", bind).Error)
		target["messageId"] = "mutated-message"
		conflict = call("reader", "pm-authorize-run", "", authorize)
		require.NotNil(t, conflict.Error)
		require.Equal(t, "conflict", conflict.Error.Code)
		target["messageId"] = "pm-message"
		admitted := call("reader", "pm-authorize-run", "", authorize)
		require.Nil(t, admitted.Error)
		var admission storage.RunDispatch
		require.NoError(t, json.Unmarshal(admitted.Data, &admission))
		require.Equal(t, runs["manager"], admission.Actor)
		read = call("reader", "pm-read-preparation", "", readBody)
		require.Nil(t, read.Error)
		require.NoError(t, json.Unmarshal(read.Data, &saved))
		require.Equal(t, admission.ID, saved.Dispatch.Admission.ID)
		require.Equal(t, originalContext, saved.Context)
		for _, operation := range []string{"prepare-run", "authorize-run", "resolve-run"} {
			project = "review-command"
			slug := runID
			if operation == "prepare-run" {
				slug = ""
			}
			denied := call("reader", operation, slug, prepare)
			require.NotNil(t, denied.Error)
			require.Equal(t, "forbidden", denied.Error.Code)
		}
		for _, slug := range []string{"pm-dispatch-foreign", "pm-dispatch-other-actor", "pm-dispatch-mutation"} {
			target["threadId"], target["workspace"] = "thread-"+slug, "workspace-"+slug
			if slug == "pm-dispatch-foreign" {
				target["projectId"] = "another-native-project"
			}
			encoded, err := json.Marshal(target)
			require.NoError(t, err)
			otherRun, _, err := s.PrepareRunForOperator(ctx, slug, "workspace-"+slug, "different-original-preparer", slug, encoded)
			require.NoError(t, err)
			pkg, err := s.ReadRunContext(ctx, otherRun)
			require.NoError(t, err)
			bind["runId"], bind["request"] = otherRun, map[string]string{"environment_id": "local", "thread_ref": "thread-" + slug}
			authorize["runId"], authorize["request"] = otherRun, map[string]any{"packageId": pkg.PackageID, "target": target}
			readBody["runId"] = otherRun
			if slug == "pm-dispatch-foreign" {
				for operation, request := range map[string]any{"pm-bind-run": bind, "pm-authorize-run": authorize, "pm-read-preparation": readBody} {
					denied := call("reader", operation, "", request)
					require.NotNil(t, denied.Error)
					require.Equal(t, "forbidden", denied.Error.Code)
				}
				target["projectId"] = "host-project"
				continue
			}
			require.Nil(t, call("reader", "pm-read-preparation", "", readBody).Error)
			require.Nil(t, call("reader", "pm-bind-run", "", bind).Error)
			if slug == "pm-dispatch-mutation" {
				changed := "Contract changed after preparation"
				_, err = s.UpdateSpec(ctx, slug, &changed, nil, nil, nil, nil)
				require.NoError(t, err)
				require.NotNil(t, call("reader", "pm-authorize-run", "", authorize).Error)
			} else {
				require.Nil(t, call("reader", "pm-authorize-run", "", authorize).Error, "current manager is not required to be original preparer")
			}
		}
		prepare["task_slug"], prepare["idempotency_key"] = "pm-dispatch-foreign", "missing-qa"
		target["workPurpose"] = "implementation"
		qa := call("reader", "pm-prepare-run", "", body)
		require.NotNil(t, qa.Error)
		require.Equal(t, "invalid_argument", qa.Error.Code)
		project = "review-command"
	})
	t.Run("manager-planning", func(t *testing.T) {
		create := map[string]any{"slug": "pm-draft", "intent": "Manager-authored draft", "priority": "p2", "complexity": "low"}
		body := map[string]any{"scope": scope("manager"), "request": create}
		for _, name := range []string{"author", "missing"} {
			body["scope"] = scope(name)
			denied := call("reader", "pm-create-node", "", body)
			require.NotNil(t, denied.Error)
			require.Equal(t, "forbidden", denied.Error.Code)
		}
		body["scope"] = scope("manager")
		for _, field := range []string{"actor", "role", "project"} {
			body[field] = "forged"
			denied := call("reader", "pm-create-node", "", body)
			require.NotNil(t, denied.Error)
			require.Equal(t, "invalid_argument", denied.Error.Code)
			delete(body, field)
		}
		for _, field := range []string{"provenance", "stage", "specifyOutput"} {
			create[field] = "imported-done"
			denied := call("reader", "pm-create-node", "", body)
			require.NotNil(t, denied.Error)
			require.Equal(t, "invalid_argument", denied.Error.Code)
			delete(create, field)
		}
		_, err := s.EnsureProject(ctx, "planning-other")
		require.NoError(t, err)
		project = "planning-other"
		require.NotNil(t, call("reader", "pm-create-node", "", body).Error)
		project = "review-command"
		for _, state := range []string{"completed", "handed_off"} {
			_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state=$1 WHERE id=$2`, state, runs["manager"])
			require.NoError(t, err)
			require.NotNil(t, call("reader", "pm-create-node", "", body).Error)
		}
		_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='bound' WHERE id=$1`, runs["manager"])
		require.NoError(t, err)
		project = "" // Existing host binding resolves the project without model selection.
		created := call("reader", "pm-create-node", "", body)
		require.Nil(t, created.Error)
		project = "review-command"
		draft, err := s.GetSpec(ctx, "pm-draft")
		require.NoError(t, err)
		require.Equal(t, storage.SpecStage("spark"), draft.Stage)
		require.Equal(t, storage.SpecProvenanceAuthored, draft.Provenance)
		unrestricted := call("reader", "create-node", "", create)
		require.NotNil(t, unrestricted.Error)
		require.Equal(t, "forbidden", unrestricted.Error.Code)
		approved := call("reader", "pm-approve-node", "", map[string]any{"scope": scope("manager"), "taskSlug": draft.Slug, "request": map[string]any{"expectedVersion": draft.Version, "basis": "Execution eligibility only"}})
		require.Nil(t, approved.Error)
		require.Contains(t, string(approved.Data), `"approved":"pm-draft"`)
		draft, err = s.GetSpec(ctx, draft.Slug)
		require.NoError(t, err)
		read := call("reader", "dependency-state", "", map[string]string{"environment_id": "local", "native_project_id": "host-project", "taskSlug": draft.Slug})
		require.Nil(t, read.Error)
		var baseline storage.DependencyEditState
		require.NoError(t, json.Unmarshal(read.Data, &baseline))
		prerequisite, err := s.GetSpec(ctx, "author")
		require.NoError(t, err)
		edit := map[string]any{"prerequisite": prerequisite.Slug, "expected_version": draft.Version, "expected_prerequisite_version": prerequisite.Version, "expected_revision": strconv.FormatInt(baseline.Revision, 10), "reason": "Synthetic prerequisite", "idempotency_key": "pm-add"}
		body = map[string]any{"scope": scope("manager"), "taskSlug": draft.Slug, "request": edit}
		added := call("reader", "pm-add-dependency", "", body)
		require.Nil(t, added.Error)
		state, err := s.ReadDependencyEditState(ctx, draft.Slug)
		require.NoError(t, err)
		require.Equal(t, runs["manager"], state.Operations[0].Actor)
		edit["expected_revision"], edit["idempotency_key"] = strconv.FormatInt(state.Revision, 10), "pm-remove"
		removed := call("reader", "pm-remove-dependency", "", body)
		require.Nil(t, removed.Error)
		edit["expected_revision"], edit["idempotency_key"] = "0", "stale-pm"
		stale := call("reader", "pm-add-dependency", "", body)
		require.NotNil(t, stale.Error)
		require.Equal(t, "conflict", stale.Error.Code)
		state, err = s.ReadDependencyEditState(ctx, draft.Slug)
		require.NoError(t, err)
		edit["prerequisite"], edit["expected_prerequisite_version"] = draft.Slug, draft.Version
		edit["expected_revision"], edit["idempotency_key"] = strconv.FormatInt(state.Revision, 10), "pm-self-cycle"
		cyclic := call("reader", "pm-add-dependency", "", body)
		require.NotNil(t, cyclic.Error)
		require.Equal(t, "failed_precondition", cyclic.Error.Code)
		subdivided := call("reader", "pm-subdivide-node", "", map[string]any{"scope": scope("manager"), "taskSlug": draft.Slug, "request": map[string]any{"expectedVersion": draft.Version, "reason": "Split work without starting models", "children": []map[string]string{{"slug": "pm-child", "intent": "Child scope", "priority": "p2", "complexity": "low"}}}})
		require.Nil(t, subdivided.Error)
		var subdivision storage.SubdivisionResult
		require.NoError(t, json.Unmarshal(subdivided.Data, &subdivision))
		require.Equal(t, []string{"pm-child"}, subdivision.ChildSlugs)
		child, err := s.GetSpec(ctx, "pm-child")
		require.NoError(t, err)
		require.Equal(t, storage.SpecStage("spark"), child.Stage)
		parent, err := s.GetSpec(ctx, draft.Slug)
		require.NoError(t, err)
		require.Equal(t, storage.SpecRoleSummary, parent.Role)
		summaryApproval := call("reader", "pm-approve-node", "", map[string]any{"scope": scope("manager"), "taskSlug": parent.Slug, "request": map[string]any{"expectedVersion": parent.Version, "basis": "Must retain original summary restriction"}})
		require.NotNil(t, summaryApproval.Error)
		require.Equal(t, "failed_precondition", summaryApproval.Error.Code)
	})
	refs, err := s.ReadSpecSourceRefs(ctx, "author")
	require.NoError(t, err)
	request := struct {
		Scope storage.MailScope `json:"scope"`
		storage.AssignReviewRequest
	}{Scope: scope("manager"), AssignReviewRequest: storage.AssignReviewRequest{TaskSlug: "author", Kind: "requirements", Sources: []storage.ReviewSource{{Kind: "specgraph", SpecSlug: "author", Field: "intent", ChangeID: refs["intent"]}}, AuthorResponsibility: storage.ReviewAuthor{Kind: "agent", RunID: runs["author"]}, ReviewerRunID: runs["reviewer"]}}
	assigned := call("reader", "review-assign", "", request)
	require.Nil(t, assigned.Error)
	var state storage.ReviewStatus
	require.NoError(t, json.Unmarshal(assigned.Data, &state))
	require.Len(t, state.Reviews, 2)
	require.Contains(t, string(assigned.Data), `"requirementDecisionIds":[]`)
	require.Contains(t, string(assigned.Data), `"decisions":[]`)
	id := state.Reviews[0].Request.ID
	submitted := call("reader", "review-submit", "", map[string]any{"scope": scope("reviewer"), "requestId": id, "verdict": "accepted", "basis": "Requirement accepted, not a test result"})
	require.Nil(t, submitted.Error)
	var result storage.SourceReviewResult
	require.NoError(t, json.Unmarshal(submitted.Data, &result))
	require.True(t, result.Recorded)
	require.Equal(t, "agent", result.Decision.ActorKind)
	denied := call("reader", "review-source", "author", map[string]string{"requestId": id, "verdict": "rejected", "basis": "No manage permission"})
	require.NotNil(t, denied.Error)
	require.Equal(t, "forbidden", denied.Error.Code)
	intervention := call("admin", "review-source", "author", map[string]string{"requestId": id, "verdict": "rejected", "basis": "Human identifies missing requirement"})
	require.Nil(t, intervention.Error)
	source := call("reader", "review-request-source", "", map[string]string{"environment_id": "local", "native_project_id": "host-project", "requestId": id})
	require.Nil(t, source.Error)
	var original storage.ReviewRequest
	require.NoError(t, json.Unmarshal(source.Data, &original))
	require.Equal(t, request.Sources, original.Sources)
	require.Len(t, original.Decisions, 2)
	require.Equal(t, "agent", original.Decisions[0].ActorKind)
	require.Equal(t, "human", original.Decisions[1].ActorKind)
	status := call("reader", "review-status", "", map[string]string{"environment_id": "local", "native_project_id": "host-project", "taskSlug": "author"})
	require.Nil(t, status.Error)
	require.NoError(t, json.Unmarshal(status.Data, &state))
	require.Len(t, state.Reviews[0].Request.Decisions, 2)
	newAssignment := call("reader", "review-assign", "", request)
	require.Nil(t, newAssignment.Error)
	require.NoError(t, json.Unmarshal(newAssignment.Data, &state))
	require.NotEqual(t, id, state.Reviews[0].Request.ID)
	byDecision := map[string]string{"environment_id": "local", "native_project_id": "host-project", "decisionId": result.Decision.ID}
	resolved := call("reader", "review-request-source", "", byDecision)
	require.Nil(t, resolved.Error)
	var historical storage.ReviewRequest
	require.NoError(t, json.Unmarshal(resolved.Data, &historical))
	require.Equal(t, original, historical, "shared decision references must resolve the original request, not the latest assignment")
	for _, body := range []map[string]string{
		{"environment_id": "local", "native_project_id": "host-project"},
		{"environment_id": "local", "native_project_id": "host-project", "requestId": id, "decisionId": result.Decision.ID},
	} {
		invalid := call("reader", "review-request-source", "", body)
		require.NotNil(t, invalid.Error)
		require.Equal(t, "invalid_argument", invalid.Error.Code)
	}
	_, err = s.EnsureProject(ctx, "review-command-other")
	require.NoError(t, err)
	project = "review-command-other"
	foreign := call("reader", "review-request-source", "", byDecision)
	require.NotNil(t, foreign.Error)
	require.Equal(t, "not_found", foreign.Error.Code)
	var mailRows int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT (SELECT count(*) FROM mail_bindings WHERE project_slug='review-command')+(SELECT count(*) FROM mail_grants WHERE project_slug='review-command')`).Scan(&mailRows))
	require.Zero(t, mailRows)
}
