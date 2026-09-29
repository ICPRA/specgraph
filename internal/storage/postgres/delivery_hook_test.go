// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/stretchr/testify/require"
)

func TestDeliveryTestHook(t *testing.T) {
	s := newStore(t, postgres.WithProject("delivery-hook"))
	ctx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: "actual-configurer", UserKind: storage.KindServiceAccount, Subject: "apikey:fixture", EffectiveRole: auth.RoleReader})
	create := func(slug string) storage.ReviewSource {
		t.Helper()
		_, err := s.CreateSpec(ctx, slug, "Fixed contract "+slug, "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
		refs, err := s.ReadSpecSourceRefs(ctx, slug)
		require.NoError(t, err)
		return storage.ReviewSource{Kind: "specgraph", SpecSlug: slug, Field: "intent", ChangeID: refs["intent"]}
	}
	plan := create("plan")
	otherPlan := create("other-plan")
	create("manager")
	manager, err := s.PrepareRun(ctx, "manager", "manager-workspace")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, manager, "env", "manager-thread"))
	managerTarget, err := json.Marshal(map[string]any{"assignmentRole": "manager", "environmentId": "env", "projectId": "native-project", "workPurpose": "implementation", "qaBasis": storage.DispatchQABasis{TestPlanSources: []storage.ReviewSource{plan}}})
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=body || jsonb_build_object('dispatch_target',$2::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE project_slug='delivery-hook' AND id=$1)`, manager, managerTarget)
	require.NoError(t, err)
	scope := storage.MailScope{EnvironmentID: "env", ThreadID: "manager-thread", ProviderSessionID: "session", ProviderInstanceID: "codex"}
	create("implementation")
	source, err := s.PrepareRun(ctx, "implementation", "source-workspace")
	require.NoError(t, err)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, source, "env", "source-thread"))
	// Synthetic original assignment: shared plan references are real store refs.
	sourceTarget, err := json.Marshal(map[string]any{"workPurpose": "implementation", "assignmentRole": "developer", "qaBasis": storage.DispatchQABasis{TestPlanSources: []storage.ReviewSource{plan}}})
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `UPDATE context_packages SET body=body || jsonb_build_object('dispatch_target',$2::jsonb) WHERE id=(SELECT package_id FROM run_bindings WHERE project_slug='delivery-hook' AND id=$1)`, source, sourceTarget)
	require.NoError(t, err)
	commit := strings.Repeat("a", 40)
	preparer := manager
	prepare := func(slug string, planSource storage.ReviewSource) string {
		t.Helper()
		create(slug)
		body, err := json.Marshal(map[string]any{"environmentId": "env", "projectId": "native-project", "runtimeMode": "full-access", "threadId": "thread-" + slug, "workspace": "workspace-" + slug,
			"createCommandId": "create-" + slug, "startCommandId": "start-" + slug, "messageId": "message-" + slug, "workPurpose": "test_execution", "purposeGuidance": "Execute the fixed test plan",
			"gitBaseline": map[string]any{"isRepo": true, "commitSha": commit}, "qaBasis": storage.DispatchQABasis{RequirementDecisionIDs: []string{}, DesignDecisionIDs: []string{}, DesignSources: []storage.ReviewSource{}, TestPlanSources: []storage.ReviewSource{planSource}, Applicability: "Applies to the fixed implementation commit"}})
		require.NoError(t, err)
		run, _, err := s.PrepareRunForOperator(ctx, slug, "workspace-"+slug, preparer, "prepare-"+slug, body)
		require.NoError(t, err)
		return run
	}
	target := prepare("test-early", plan)
	req := storage.ArmDeliveryHookRequest{SourceRunID: source, TargetRunID: target, CommitSHA: commit, IdempotencyKey: "arm-early"}
	authorScope := scope
	authorScope.ThreadID = "source-thread"
	_, err = s.ArmDeliveryTestHook(ctx, authorScope, req)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	_, err = s.ArmDeliveryTestHook(context.Background(), scope, req)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	hook, err := s.ArmDeliveryTestHook(ctx, scope, req)
	require.NoError(t, err)
	require.Equal(t, "armed", hook.State)
	require.Equal(t, manager, *hook.ConfiguredByRunID)
	require.Equal(t, "actual-configurer", hook.HostConsumerUserID)
	require.Equal(t, "actual-configurer", hook.ConfiguredByUserID)
	require.Nil(t, hook.DeliveryID)
	again, err := s.ArmDeliveryTestHook(ctx, scope, req)
	require.NoError(t, err)
	require.Equal(t, hook, again)
	changed := req
	changed.CommitSHA = strings.Repeat("b", 40)
	_, err = s.ArmDeliveryTestHook(ctx, scope, changed)
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	for _, body := range []string{`{}`, `{"commitSha":"` + commit + `"}`, `{"git":{"head":{"isRepo":true,"commitSha":"` + strings.Repeat("b", 40) + `"}}}`} {
		_, err = s.CreateDelivery(ctx, source, json.RawMessage(body), source)
		require.NoError(t, err, "nonmatching and non-Git delivery remains accepted")
	}
	stillArmed, err := s.ReadDeliveryTestHook(ctx, scope, hook.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", stillArmed.State)
	snapshot := json.RawMessage(`{"git":{"head":{"isRepo":true,"commitSha":"` + commit + `"}}}`)
	rollback := errors.New("rollback delivery fixture")
	err = s.RunInTransaction(ctx, func(txCtx context.Context) error {
		_, err := s.CreateDelivery(txCtx, source, snapshot, source)
		require.NoError(t, err)
		pending, err := s.ReadDeliveryTestHook(txCtx, scope, hook.ID)
		require.NoError(t, err)
		require.Equal(t, "pending", pending.State)
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	stillArmed, err = s.ReadDeliveryTestHook(ctx, scope, hook.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", stillArmed.State, "delivery and trigger roll back together")
	delivery, err := s.CreateDelivery(ctx, source, snapshot, source)
	require.NoError(t, err)
	newer, err := s.CreateDelivery(ctx, source, snapshot, source)
	require.NoError(t, err)
	pending, err := s.ArmDeliveryTestHook(ctx, scope, req)
	require.NoError(t, err)
	require.Equal(t, "pending", pending.State)
	require.Equal(t, delivery, *pending.DeliveryID, "later duplicate delivery does not rebind the one-shot hook")
	require.NotEqual(t, newer, *pending.DeliveryID)
	lateReq := req
	lateReq.TargetRunID, lateReq.IdempotencyKey, lateReq.DeliveryID = prepare("test-late", plan), "arm-late", &delivery
	late, err := s.ArmDeliveryTestHook(ctx, scope, lateReq)
	require.NoError(t, err)
	require.Equal(t, delivery, *late.DeliveryID, "late arm uses explicit selected delivery, never latest")
	badPlans := req
	badPlans.TargetRunID, badPlans.IdempotencyKey = prepare("test-disjoint", otherPlan), "arm-disjoint"
	_, err = s.ArmDeliveryTestHook(ctx, scope, badPlans)
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	cancelReq := req
	cancelReq.TargetRunID, cancelReq.IdempotencyKey = prepare("test-cancel", plan), "arm-cancel"
	cancelHook, err := s.ArmDeliveryTestHook(ctx, scope, cancelReq)
	require.NoError(t, err)
	cancelled, err := s.CancelDeliveryTestHook(ctx, scope, cancelHook.ID, "Explicitly disabled before admission")
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled.State)
	repeatCancel, err := s.CancelDeliveryTestHook(ctx, scope, cancelHook.ID, "Explicitly disabled before admission")
	require.NoError(t, err)
	require.Equal(t, cancelled, repeatCancel)
	_, err = s.CreateDelivery(ctx, source, snapshot, source)
	require.NoError(t, err)
	cancelled, err = s.ReadDeliveryTestHook(ctx, scope, cancelHook.ID)
	require.NoError(t, err)
	require.Nil(t, cancelled.DeliveryID)
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='completed' WHERE project_slug='delivery-hook' AND id=$1`, lateReq.TargetRunID)
	require.NoError(t, err)
	_, err = s.CancelDeliveryTestHook(ctx, scope, late.ID, "Not a stop request")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	page, err := s.ListDeliveryTestHooks(ctx, scope, 1, "")
	require.NoError(t, err)
	require.Len(t, page.Hooks, 1)
	require.NotEmpty(t, page.NextCursor)
	next, err := s.ListDeliveryTestHooks(ctx, scope, 50, page.NextCursor)
	require.NoError(t, err)
	require.Len(t, next.Hooks, 2)
	_, err = s.ReadDeliveryTestHook(ctx, authorScope, hook.ID)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	// Both orderings are legitimate: arming without deliveryId never adopts old delivery.
	raceReq := req
	raceReq.TargetRunID, raceReq.IdempotencyKey = prepare("test-race", plan), "arm-race"
	var wg sync.WaitGroup
	wg.Add(2)
	var raceHook *storage.DeliveryTestHook
	var armErr, deliveryErr error
	go func() { defer wg.Done(); raceHook, armErr = s.ArmDeliveryTestHook(ctx, scope, raceReq) }()
	go func() { defer wg.Done(); _, deliveryErr = s.CreateDelivery(ctx, source, snapshot, source) }()
	wg.Wait()
	require.NoError(t, armErr)
	require.NoError(t, deliveryErr)
	_, err = s.CreateDelivery(ctx, source, snapshot, source)
	require.NoError(t, err)
	raceHook, err = s.ReadDeliveryTestHook(ctx, scope, raceHook.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", raceHook.State)
	body, err := json.Marshal(map[string]any{"scope": scope, "request": req})
	require.NoError(t, err)
	wire, err := server.ExecuteAgentDeliveryHook(ctx, s, "", "pm-arm-delivery-hook", strings.NewReader(string(body)))
	require.NoError(t, err)
	var fromWire storage.DeliveryTestHook
	require.NoError(t, json.Unmarshal(wire, &fromWire))
	require.Equal(t, hook.ID, fromWire.ID)
	require.Equal(t, "pending", fromWire.State)
	_, err = server.ExecuteAgentDeliveryHook(ctx, s, "delivery-hook", "pm-arm-delivery-hook", strings.NewReader(`{"scope":{},"request":{"configuredByRunId":"forged"}}`))
	require.ErrorIs(t, err, storage.ErrInvalidDeliveryHook)
	_, err = server.ExecuteAgentDeliveryHook(ctx, s, "delivery-hook", "pm-arm-delivery-hook", strings.NewReader(`{"scope":{},"request":{"deliveryId":null}}`))
	require.ErrorIs(t, err, storage.ErrInvalidDeliveryHook)
	_, err = s.EnsureProject(ctx, "delivery-hook-other")
	require.NoError(t, err)
	other, err := s.ScopedExisting(ctx, "delivery-hook-other")
	require.NoError(t, err)
	_, err = other.ReadDeliveryTestHook(ctx, scope, hook.ID)
	require.Error(t, err, "same native scope cannot select another project's hook")
	prepared, err := s.ReadRunContext(ctx, target)
	require.NoError(t, err)
	var preparedBody struct {
		Target json.RawMessage `json:"dispatch_target"`
	}
	require.NoError(t, json.Unmarshal(prepared.Body, &preparedBody))
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, target, "env", "thread-test-early"))
	admission, err := s.AuthorizeRunDispatch(ctx, target, manager, prepared.PackageID, preparedBody.Target)
	require.NoError(t, err)
	_, err = s.CancelDeliveryTestHook(ctx, scope, hook.ID, "Cannot stop an admitted run")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	require.NoError(t, s.ResolveRunDispatch(ctx, target, admission.ID, manager, "stopped_writing", "Synthetic test run was never physically started"))
	_, err = s.CancelDeliveryTestHook(ctx, scope, hook.ID, "Cannot cancel a resolved admission")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	pmSourceReq := req
	pmSourceReq.SourceRunID, pmSourceReq.TargetRunID, pmSourceReq.IdempotencyKey = manager, prepare("test-pm-source", plan), "arm-pm-source"
	pmSourceHook, err := s.ArmDeliveryTestHook(ctx, scope, pmSourceReq)
	require.NoError(t, err, "actual bound PM may configure its own implementation source")
	require.Equal(t, manager, pmSourceHook.SourceRunID)
	require.Equal(t, manager, *pmSourceHook.ConfiguredByRunID)
	_, err = s.ArmDeliveryTestHook(ctx, authorScope, pmSourceReq)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden, "ordinary implementation scope still has no PM authority")

	hostScope := storage.DeliveryHookHostScope{EnvironmentID: "env", NativeProjectID: "native-project"}
	hostBody, err := json.Marshal(map[string]any{"environmentId": "env", "nativeProjectId": "native-project", "hookId": raceHook.ID})
	require.NoError(t, err)
	hostWire, err := server.ExecuteHostDeliveryHook(ctx, s, "", "host-read-delivery-hook", strings.NewReader(string(hostBody)))
	require.NoError(t, err)
	var hostRead storage.DeliveryHookPreparation
	require.NoError(t, json.Unmarshal(hostWire, &hostRead))
	require.Nil(t, hostRead.ReadinessError)
	require.Equal(t, manager, hostRead.Parent.RunID)
	require.Equal(t, "manager-thread", hostRead.Parent.ThreadID)
	require.Equal(t, raceReq.TargetRunID, hostRead.Context.RunID)
	_, err = server.ExecuteHostDeliveryHook(ctx, s, "", "host-bind-delivery-hook", strings.NewReader(`{"environmentId":"env","nativeProjectId":"native-project","hookId":"`+raceHook.ID+`","targetRunId":"forged"}`))
	require.ErrorIs(t, err, storage.ErrInvalidDeliveryHook)
	wrongUser := auth.WithIdentity(context.Background(), &auth.Identity{UserID: "other-authenticated-user"})
	_, err = s.PrepareDeliveryHookDispatch(wrongUser, hostScope, raceHook.ID, "read")
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	startPhase, createPhase := "start", "create"
	_, err = s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "commandAccepted", Phase: &startPhase})
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict, "start acceptance requires original admission")
	detail := "Synthetic ambiguous native create result"
	unknown, err := s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "unconfirmed", Phase: &createPhase, Detail: &detail})
	require.NoError(t, err)
	require.Equal(t, "unconfirmed", unknown.State)
	_, err = s.PrepareDeliveryHookDispatch(ctx, hostScope, raceHook.ID, "bind")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict, "unknown result cannot automatically redispatch")
	pendingHost, err := s.ListHostDeliveryTestHooks(ctx, hostScope, 100, "")
	require.NoError(t, err)
	foundUnknown := false
	for _, h := range pendingHost.Hooks {
		if h.ID == raceHook.ID {
			foundUnknown = true
		}
	}
	require.True(t, foundUnknown, "unknown results stay discoverable for receipt reconciliation")
	retried, err := s.RetryDeliveryTestHook(ctx, scope, raceHook.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", retried.State)
	require.Equal(t, raceHook.TargetRunID, retried.TargetRunID)
	blockedDetail := "Synthetic provider guard failed before native dispatch"
	_, err = s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "blocked", Detail: &blockedDetail})
	require.NoError(t, err)
	pendingHost, err = s.ListHostDeliveryTestHooks(ctx, hostScope, 100, "")
	require.NoError(t, err)
	for _, h := range pendingHost.Hooks {
		require.NotEqual(t, raceHook.ID, h.ID, "blocked work requires explicit PM retry")
	}
	_, err = s.RetryDeliveryTestHook(ctx, scope, raceHook.ID)
	require.NoError(t, err)
	_, err = s.PrepareDeliveryHookDispatch(ctx, hostScope, raceHook.ID, "bind")
	require.NoError(t, err)
	authorized, err := s.PrepareDeliveryHookDispatch(ctx, hostScope, raceHook.ID, "authorize")
	require.NoError(t, err)
	require.Equal(t, manager, authorized.Dispatch.Admission.Actor)
	require.Equal(t, raceHook.TargetRunID, authorized.Dispatch.Admission.RunID)
	_, err = s.CancelDeliveryTestHook(ctx, scope, raceHook.ID, "Unknown after admission is not cancellation")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	_, err = s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "unconfirmed", Phase: &startPhase, Detail: &detail})
	require.NoError(t, err)
	testScope := scope
	testScope.ThreadID = "thread-test-race"
	trigger, err := s.ReadOwnDeliveryHookContext(ctx, testScope)
	require.NoError(t, err)
	require.Equal(t, raceHook.ID, trigger.HookID)
	require.Equal(t, *raceHook.DeliveryID, trigger.DeliveryID)
	_, err = s.ReadOwnDeliveryHookContext(ctx, authorScope)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	require.NoError(t, s.BindRunThreadInEnvironment(ctx, badPlans.TargetRunID, "env", "thread-test-disjoint"))
	ordinaryTestScope := scope
	ordinaryTestScope.ThreadID = "thread-test-disjoint"
	_, err = s.ReadOwnDeliveryHookContext(ctx, ordinaryTestScope)
	require.ErrorIs(t, err, storage.ErrDeliveryHookNotFound, "ordinary testing assignment must not receive a fabricated trigger")
	rejectReq := req
	rejectReq.TargetRunID, rejectReq.IdempotencyKey, rejectReq.DeliveryID = prepare("test-native-rejected", plan), "arm-native-rejected", &delivery
	rejectHook, err := s.ArmDeliveryTestHook(ctx, scope, rejectReq)
	require.NoError(t, err)
	rejectDetail := "Synthetic explicit rejection of the original create command"
	rejected, err := s.RecordDeliveryHookHostResult(ctx, hostScope, rejectHook.ID, storage.DeliveryHookHostResult{Status: "rejected", Phase: &createPhase, Detail: &rejectDetail})
	require.NoError(t, err)
	require.Equal(t, "rejected", rejected.State)
	_, err = s.RetryDeliveryTestHook(ctx, scope, rejectHook.ID)
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict, "native terminal rejection cannot be reset")
	_, err = s.Pool().Exec(ctx, `UPDATE run_bindings SET state='completed' WHERE project_slug='delivery-hook' AND id=$1`, manager)
	require.NoError(t, err)
	historical, err := s.PrepareDeliveryHookDispatch(ctx, hostScope, raceHook.ID, "read")
	require.NoError(t, err)
	require.NotNil(t, historical.ReadinessError)
	require.Equal(t, "forbidden", historical.ReadinessError.Code)
	require.Equal(t, authorized.Context, historical.Context, "retired PM does not erase original command identities")
	_, err = s.PrepareDeliveryHookDispatch(ctx, hostScope, raceHook.ID, "authorize")
	require.ErrorIs(t, err, storage.ErrPlanningForbidden)
	accepted, err := s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "commandAccepted", Phase: &startPhase})
	require.NoError(t, err, "actual original receipt can reconcile after PM retirement")
	againAccepted, err := s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "commandAccepted", Phase: &startPhase})
	require.NoError(t, err)
	require.Equal(t, accepted, againAccepted)
	_, err = s.RecordDeliveryHookHostResult(ctx, hostScope, raceHook.ID, storage.DeliveryHookHostResult{Status: "rejected", Phase: &startPhase, Detail: &detail})
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict, "terminal acceptance cannot be replaced")
	view, _, err := server.ReadWorkbenchCurrentView(ctx, s, "delivery-hook")
	require.NoError(t, err)
	require.True(t, view["capabilities"].(map[string]bool)["deliveryHooks"])
	projected := view["deliveryHooks"].([]postgres.WorkbenchDeliveryHook)
	byID := map[string]postgres.WorkbenchDeliveryHook{}
	for _, item := range projected {
		byID[item.ID] = item
		require.Equal(t, "delivery-hook", item.Project)
	}
	require.Equal(t, *accepted, byID[raceHook.ID].DeliveryTestHook)
	require.Equal(t, "implementation", byID[raceHook.ID].SourceTaskSlug)
	require.Equal(t, "test-race", byID[raceHook.ID].TargetTaskSlug)
	require.Equal(t, "cancelled", byID[cancelHook.ID].State)
	require.Equal(t, "armed", byID[pmSourceHook.ID].State)
	require.Equal(t, "rejected", byID[rejectHook.ID].State)
	encodedProjection, err := json.Marshal(projected)
	require.NoError(t, err)
	require.NotContains(t, string(encodedProjection), "dispatch_target")
	require.NotContains(t, string(encodedProjection), "purposeGuidance")
	foreignView, _, err := server.ReadWorkbenchCurrentView(ctx, other, "delivery-hook-other")
	require.NoError(t, err)
	foreignHooks := foreignView["deliveryHooks"].([]postgres.WorkbenchDeliveryHook)
	require.NotNil(t, foreignHooks)
	require.Empty(t, foreignHooks, "project filter excludes actual hooks in another project")

	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Synthetic human hook configurer", Role: "admin"}, nil)
	require.NoError(t, err)
	otherHuman, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Synthetic retry actor and host owner", Role: "admin"}, nil)
	require.NoError(t, err)
	consumer, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Synthetic existing host service", Role: "reader", OwnerUserID: otherHuman.ID})
	require.NoError(t, err)
	humanCtx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman, EffectiveRole: auth.RoleAdmin, Source: "oidc"})
	retryCtx := auth.WithIdentity(context.Background(), &auth.Identity{UserID: otherHuman.ID, UserKind: storage.KindHuman, EffectiveRole: auth.RoleAdmin, Source: "oidc"})
	consumerIdentity := &auth.Identity{UserID: consumer.ID, UserKind: storage.KindServiceAccount, Source: "apikey", EffectiveRole: auth.RoleReader}
	consumerCtx := auth.WithIdentity(context.Background(), consumerIdentity)
	preparer = human.ID
	humanReq := req
	humanReq.TargetRunID, humanReq.IdempotencyKey, humanReq.DeliveryID = prepare("human-test", plan), "human-arm", &delivery
	_, err = s.ArmHumanDeliveryTestHook(consumerCtx, humanReq, consumerIdentity)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden, "service account cannot enter human management path")
	_, err = s.ArmHumanDeliveryTestHook(humanCtx, humanReq, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman, Source: "apikey"})
	require.ErrorIs(t, err, storage.ErrPlanningForbidden, "host consumer must be a verified service identity")
	humanHook, err := s.ArmHumanDeliveryTestHook(humanCtx, humanReq, consumerIdentity)
	require.NoError(t, err, "human configuration requires no live PM; fixture PM is already retired")
	require.Nil(t, humanHook.ConfiguredByRunID)
	require.Equal(t, human.ID, humanHook.ConfiguredByUserID)
	require.Equal(t, consumer.ID, humanHook.HostConsumerUserID)
	humanHost, err := s.PrepareDeliveryHookDispatch(consumerCtx, hostScope, humanHook.ID, "read")
	require.NoError(t, err)
	require.Nil(t, humanHost.Parent)
	require.Nil(t, humanHost.ReadinessError)
	_, err = s.PrepareDeliveryHookDispatch(humanCtx, hostScope, humanHook.ID, "read")
	require.ErrorIs(t, err, storage.ErrPlanningForbidden, "human configurer is not silently the background consumer")
	_, err = s.RecordDeliveryHookHostResult(consumerCtx, hostScope, humanHook.ID, storage.DeliveryHookHostResult{Status: "blocked", Detail: &blockedDetail})
	require.NoError(t, err)
	humanRetry, err := s.RetryHumanDeliveryTestHook(retryCtx, humanHook.ID)
	require.NoError(t, err)
	require.Equal(t, human.ID, humanRetry.ConfiguredByUserID)
	require.Equal(t, consumer.ID, humanRetry.HostConsumerUserID)
	require.Nil(t, humanRetry.RetriedByRunID)
	require.Equal(t, otherHuman.ID, *humanRetry.RetriedByUserID)
	require.NotNil(t, humanRetry.RetriedAt)
	_, err = s.PrepareDeliveryHookDispatch(consumerCtx, hostScope, humanHook.ID, "bind")
	require.NoError(t, err)
	humanAdmission, err := s.PrepareDeliveryHookDispatch(consumerCtx, hostScope, humanHook.ID, "authorize")
	require.NoError(t, err)
	require.Equal(t, human.ID, humanAdmission.Dispatch.Admission.Actor)
	_, err = s.RecordDeliveryHookHostResult(consumerCtx, hostScope, humanHook.ID, storage.DeliveryHookHostResult{Status: "commandAccepted", Phase: &startPhase})
	require.NoError(t, err)
	_, err = s.CancelHumanDeliveryTestHook(humanCtx, humanHook.ID, "Cannot stop an admitted target")
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict)
	_, err = s.RetryHumanDeliveryTestHook(retryCtx, humanHook.ID)
	require.ErrorIs(t, err, storage.ErrDeliveryHookConflict, "terminal receipt cannot be reset")
	humanReq.TargetRunID, humanReq.IdempotencyKey = prepare("human-cancel", plan), "human-cancel"
	humanCancelHook, err := s.ArmHumanDeliveryTestHook(humanCtx, humanReq, consumerIdentity)
	require.NoError(t, err)
	humanCancelled, err := s.CancelHumanDeliveryTestHook(retryCtx, humanCancelHook.ID, "Explicit human cancellation")
	require.NoError(t, err)
	require.Nil(t, humanCancelled.CancelledByRunID)
	require.Equal(t, otherHuman.ID, *humanCancelled.CancelledByUserID)
	require.Equal(t, human.ID, humanCancelled.ConfiguredByUserID)
	_, err = s.RecordDeliveryHookHostResult(ctx, hostScope, hook.ID, storage.DeliveryHookHostResult{Status: "blocked", Detail: &blockedDetail})
	require.NoError(t, err)
	_, err = s.RetryHumanDeliveryTestHook(humanCtx, hook.ID)
	require.ErrorIs(t, err, storage.ErrPlanningForbidden, "human retry cannot replace the retired PM grant")
	humanBody, err := json.Marshal(map[string]any{"sourceRunId": humanReq.SourceRunID, "targetRunId": humanReq.TargetRunID, "commitSha": humanReq.CommitSHA, "idempotencyKey": humanReq.IdempotencyKey, "deliveryId": humanReq.DeliveryID, "hostCredential": "private-fixture-not-persisted"})
	require.NoError(t, err)
	humanWire, err := server.ExecuteHumanDeliveryHook(humanCtx, s, "delivery-hook", "arm-delivery-hook", strings.NewReader(string(humanBody)), consumerIdentity)
	require.NoError(t, err)
	require.NotContains(t, string(humanWire), "private-fixture-not-persisted")
	_, err = server.ExecuteHumanDeliveryHook(humanCtx, s, "delivery-hook", "retry-delivery-hook", strings.NewReader(`{"hookId":"`+humanHook.ID+`","hostConsumerUserId":"forged"}`), nil)
	require.ErrorIs(t, err, storage.ErrInvalidDeliveryHook)
	terminalReq := req
	terminalReq.TargetRunID, terminalReq.IdempotencyKey = prepare("terminal-target", plan), "arm-terminal"
	terminalHook, err := s.ArmHumanDeliveryTestHook(humanCtx, terminalReq, consumerIdentity)
	require.NoError(t, err)
	require.Equal(t, "armed", terminalHook.State)
	create("replacement-implementation")
	doneStage := "done"
	_, err = s.UpdateSpec(ctx, "implementation", nil, &doneStage, nil, nil, nil)
	require.NoError(t, err)
	_, _, err = s.LifecycleSupersedeSpec(ctx, "implementation", "replacement-implementation", "Replace original implementation")
	require.NoError(t, err)
	_, err = s.CreateDelivery(ctx, source, snapshot, source)
	require.ErrorIs(t, err, storage.ErrSpecTerminal)
	var terminalDelivery *string
	var terminalTriggered *time.Time
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT delivery_id,triggered_at FROM delivery_test_hooks WHERE project_slug='delivery-hook' AND id=$1`, terminalHook.ID).Scan(&terminalDelivery, &terminalTriggered))
	require.Nil(t, terminalDelivery, "rejected terminal delivery cannot trigger the pending hook")
	require.Nil(t, terminalTriggered)
}
