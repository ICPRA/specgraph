// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package auth

import (
	"sort"

	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
)

// Workbench procedure identities share the existing Cedar action registry even
// though their transport is HTTP/JSON rather than generated ConnectRPC.
const (
	WorkbenchReadOwnDeliveryContextProcedure      = "/workbench.v1.WorkbenchService/ReadOwnDeliveryContext"
	WorkbenchSubmitOwnDeliveryProcedure           = "/workbench.v1.WorkbenchService/SubmitOwnDelivery"
	WorkbenchInspectMailProcedure                 = "/workbench.v1.WorkbenchService/InspectMail"
	WorkbenchMailReadProcedure                    = "/workbench.v1.WorkbenchService/MailRead"
	WorkbenchMailWriteProcedure                   = "/workbench.v1.WorkbenchService/MailWrite"
	WorkbenchMailManageProcedure                  = "/workbench.v1.WorkbenchService/MailManage"
	WorkbenchPrepareRunProcedure                  = "/workbench.v1.WorkbenchService/PrepareRun"
	WorkbenchReadRunContextProcedure              = "/workbench.v1.WorkbenchService/ReadRunContext"
	WorkbenchDispatchProcedure                    = "/workbench.v1.WorkbenchService/Dispatch"
	WorkbenchBindRunThreadProcedure               = "/workbench.v1.WorkbenchService/BindRunThread"
	WorkbenchSubmitDeliveryProcedure              = "/workbench.v1.WorkbenchService/SubmitDelivery"
	WorkbenchAcceptDeliveryProcedure              = "/workbench.v1.WorkbenchService/AcceptDelivery"
	WorkbenchCompleteRunProcedure                 = "/workbench.v1.WorkbenchService/CompleteRun"
	WorkbenchManualCompleteProcedure              = "/workbench.v1.WorkbenchService/ManualComplete"
	WorkbenchEditDependencyProcedure              = "/workbench.v1.WorkbenchService/EditDependency"
	WorkbenchSubdivisionProcedure                 = "/workbench.v1.WorkbenchService/Subdivision"
	WorkbenchChangePreviewProcedure               = "/workbench.v1.WorkbenchService/ChangePreview"
	WorkbenchSetNodeMarkProcedure                 = "/workbench.v1.WorkbenchService/SetNodeMark"
	WorkbenchRecordNodeEventProcedure             = "/workbench.v1.WorkbenchService/RecordNodeEvent"
	WorkbenchRecordOwnNodeEventProcedure          = "/workbench.v1.WorkbenchService/RecordOwnNodeEvent"
	WorkbenchRecordOwnCandidateJudgmentProcedure  = "/workbench.v1.WorkbenchService/RecordOwnCandidateJudgment"
	WorkbenchRecordOwnNodeHandoffSummaryProcedure = "/workbench.v1.WorkbenchService/RecordOwnNodeHandoffSummary"
	WorkbenchAssignAgentReviewProcedure           = "/workbench.v1.WorkbenchService/AssignAgentReview"
	WorkbenchSubmitAgentReviewProcedure           = "/workbench.v1.WorkbenchService/SubmitAgentReview"
	WorkbenchRecordTestResultProcedure            = "/workbench.v1.WorkbenchService/RecordTestResult"
	WorkbenchSubmitAgentTestResultProcedure       = "/workbench.v1.WorkbenchService/SubmitAgentTestResult"
	WorkbenchCompleteAgentRunProcedure            = "/workbench.v1.WorkbenchService/CompleteAgentRun"
	WorkbenchAgentPlanningProcedure               = "/workbench.v1.WorkbenchService/AgentPlanning"
	WorkbenchAgentPlanningDispatchProcedure       = "/workbench.v1.WorkbenchService/AgentPlanningDispatch"
	WorkbenchConfigureDeliveryHookProcedure       = "/workbench.v1.WorkbenchService/ConfigureDeliveryHook"
	WorkbenchReadDeliveryHookProcedure            = "/workbench.v1.WorkbenchService/ReadDeliveryHook"
	WorkbenchDispatchDeliveryHookProcedure        = "/workbench.v1.WorkbenchService/DispatchDeliveryHook"
	WorkbenchManageDeliveryHookProcedure          = "/workbench.v1.WorkbenchService/ManageDeliveryHook"
	WorkbenchPrepareProgramRunProcedure           = "/workbench.v1.WorkbenchService/PrepareProgramRun"
	WorkbenchReadProgramRunProcedure              = "/workbench.v1.WorkbenchService/ReadProgramRun"
	WorkbenchReadConversationRunsProcedure        = "/workbench.v1.WorkbenchService/ReadConversationRuns"
	WorkbenchExecuteProgramRunProcedure           = "/workbench.v1.WorkbenchService/ExecuteProgramRun"
	WorkbenchManageProgramLoopProcedure           = "/workbench.v1.WorkbenchService/ManageProgramLoop"
	WorkbenchManageSummaryProcedure               = "/workbench.v1.WorkbenchService/ManageSummary"
	WorkbenchMergeNodesProcedure                  = "/workbench.v1.WorkbenchService/MergeNodes"
	WorkbenchAgentSummaryProcedure                = "/workbench.v1.WorkbenchService/AgentSummary"
	WorkbenchManageCompletionHookProcedure        = "/workbench.v1.WorkbenchService/ManageCompletionHook"
	WorkbenchReadCompletionHookProcedure          = "/workbench.v1.WorkbenchService/ReadCompletionHook"
	WorkbenchDispatchCompletionHookProcedure      = "/workbench.v1.WorkbenchService/DispatchCompletionHook"
)

// procedureActions maps each RPC procedure to a stable, RPC-method-decoupled
// action name (domain.verb). It replaces the rpcPermissions table: where
// rpcPermissions held "spec:read", this holds "spec.read", which the Cedar
// policies gate via the verb action-group. Renaming an RPC method changes
// only this map, not any policy.
var procedureActions = map[string]string{
	WorkbenchReadOwnDeliveryContextProcedure:      "execution.read",
	WorkbenchSubmitOwnDeliveryProcedure:           "delivery-submission.write",
	WorkbenchInspectMailProcedure:                 "workbench.manage",
	WorkbenchMailReadProcedure:                    "mail.read",
	WorkbenchMailWriteProcedure:                   "mail.write",
	WorkbenchMailManageProcedure:                  "mail.manage",
	WorkbenchPrepareRunProcedure:                  "workbench.write",
	WorkbenchReadRunContextProcedure:              "workbench.write",
	WorkbenchDispatchProcedure:                    "workbench.manage",
	WorkbenchBindRunThreadProcedure:               "workbench.write",
	WorkbenchSubmitDeliveryProcedure:              "workbench.write",
	WorkbenchAcceptDeliveryProcedure:              "workbench.manage",
	WorkbenchCompleteRunProcedure:                 "workbench.write",
	WorkbenchManualCompleteProcedure:              "workbench.manage",
	WorkbenchEditDependencyProcedure:              "workbench.manage",
	WorkbenchSubdivisionProcedure:                 "workbench.manage",
	WorkbenchChangePreviewProcedure:               "workbench.manage",
	WorkbenchSetNodeMarkProcedure:                 "workbench.manage",
	WorkbenchRecordNodeEventProcedure:             "workbench.manage",
	WorkbenchRecordOwnNodeEventProcedure:          "node-event.write",
	WorkbenchRecordOwnCandidateJudgmentProcedure:  "candidate-judgment.write",
	WorkbenchRecordOwnNodeHandoffSummaryProcedure: "node-handoff.write",
	WorkbenchAssignAgentReviewProcedure:           "review-assignment.write",
	WorkbenchSubmitAgentReviewProcedure:           "review-decision.write",
	WorkbenchRecordTestResultProcedure:            "workbench.write",
	WorkbenchSubmitAgentTestResultProcedure:       "test-result.write",
	WorkbenchCompleteAgentRunProcedure:            "run-completion.write",
	WorkbenchAgentPlanningProcedure:               "planning.write",
	WorkbenchAgentPlanningDispatchProcedure:       "planning-dispatch.write",
	WorkbenchConfigureDeliveryHookProcedure:       "hook-config.write",
	WorkbenchReadDeliveryHookProcedure:            "execution.read",
	WorkbenchDispatchDeliveryHookProcedure:        "hook-dispatch.write",
	WorkbenchManageDeliveryHookProcedure:          "workbench.manage",
	WorkbenchPrepareProgramRunProcedure:           "workbench.manage",
	WorkbenchReadProgramRunProcedure:              "execution.read",
	WorkbenchReadConversationRunsProcedure:        "execution.read",
	WorkbenchExecuteProgramRunProcedure:           "program-execution.write",
	WorkbenchManageProgramLoopProcedure:           "workbench.manage",
	WorkbenchManageSummaryProcedure:               "workbench.manage",
	WorkbenchMergeNodesProcedure:                  "workbench.manage",
	WorkbenchAgentSummaryProcedure:                "summary.write",
	WorkbenchManageCompletionHookProcedure:        "workbench.manage",
	WorkbenchReadCompletionHookProcedure:          "execution.read",
	WorkbenchDispatchCompletionHookProcedure:      "program-execution.write",
	// SpecService
	specgraphv1connect.SpecServiceGetSpecProcedure:         "spec.read",
	specgraphv1connect.SpecServiceListSpecsProcedure:       "spec.read",
	specgraphv1connect.SpecServiceCreateSpecProcedure:      "spec.write",
	specgraphv1connect.SpecServiceUpdateSpecProcedure:      "spec.write",
	specgraphv1connect.SpecServiceListChangesProcedure:     "spec.read",
	specgraphv1connect.SpecServiceCompareVersionsProcedure: "spec.read",
	// DecisionService
	specgraphv1connect.DecisionServiceGetDecisionProcedure:    "decision.read",
	specgraphv1connect.DecisionServiceListDecisionsProcedure:  "decision.read",
	specgraphv1connect.DecisionServiceCreateDecisionProcedure: "decision.write",
	specgraphv1connect.DecisionServiceUpdateDecisionProcedure: "decision.write",
	// GraphService
	specgraphv1connect.GraphServiceGetFullGraphProcedure:      "graph.read",
	specgraphv1connect.GraphServiceGetDependenciesProcedure:   "graph.read",
	specgraphv1connect.GraphServiceGetTransitiveDepsProcedure: "graph.read",
	specgraphv1connect.GraphServiceGetImpactProcedure:         "graph.read",
	specgraphv1connect.GraphServiceGetReadyProcedure:          "graph.read",
	specgraphv1connect.GraphServiceGetCriticalPathProcedure:   "graph.read",
	specgraphv1connect.GraphServiceListEdgesProcedure:         "graph.read",
	specgraphv1connect.GraphServiceAddEdgeProcedure:           "graph.write",
	specgraphv1connect.GraphServiceRemoveEdgeProcedure:        "graph.delete",
	// ClaimService
	specgraphv1connect.ClaimServiceClaimSpecProcedure:   "claim.write",
	specgraphv1connect.ClaimServiceHeartbeatProcedure:   "claim.write",
	specgraphv1connect.ClaimServiceUnclaimSpecProcedure: "claim.write",
	// ConstitutionService
	specgraphv1connect.ConstitutionServiceGetConstitutionProcedure:          "constitution.read",
	specgraphv1connect.ConstitutionServiceUpdateConstitutionProcedure:       "constitution.write",
	specgraphv1connect.ConstitutionServiceEmitToolFilesProcedure:            "constitution.read",
	specgraphv1connect.ConstitutionServiceRefreshConstitutionLayerProcedure: "constitution.write",
	// AuthoringService
	specgraphv1connect.AuthoringServiceGetPromptsProcedure:         "authoring.read",
	specgraphv1connect.AuthoringServiceSparkProcedure:              "authoring.write",
	specgraphv1connect.AuthoringServiceShapeProcedure:              "authoring.write",
	specgraphv1connect.AuthoringServiceSpecifyProcedure:            "authoring.write",
	specgraphv1connect.AuthoringServiceDecomposeProcedure:          "authoring.write",
	specgraphv1connect.AuthoringServiceApproveProcedure:            "authoring.write",
	specgraphv1connect.AuthoringServiceRecordConversationProcedure: "authoring.write",
	specgraphv1connect.AuthoringServiceListConversationsProcedure:  "authoring.read",
	// ExecutionService
	specgraphv1connect.ExecutionServiceGenerateBundleProcedure:     "execution.read",
	specgraphv1connect.ExecutionServiceGetPrimeProcedure:           "execution.read",
	specgraphv1connect.ExecutionServiceGetExecutionEventsProcedure: "execution.read",
	specgraphv1connect.ExecutionServiceReportProgressProcedure:     "execution.write",
	specgraphv1connect.ExecutionServiceReportBlockerProcedure:      "execution.write",
	specgraphv1connect.ExecutionServiceReportCompletionProcedure:   "execution.write",
	// LifecycleService
	specgraphv1connect.LifecycleServiceCheckDriftProcedure:          "lifecycle.read",
	specgraphv1connect.LifecycleServiceLintProcedure:                "lifecycle.read",
	specgraphv1connect.LifecycleServiceAcknowledgeDriftProcedure:    "lifecycle.write",
	specgraphv1connect.LifecycleServiceTransitionAmendProcedure:     "lifecycle.write",
	specgraphv1connect.LifecycleServiceTransitionSupersedeProcedure: "lifecycle.write",
	specgraphv1connect.LifecycleServiceTransitionAbandonProcedure:   "lifecycle.write",
	// SyncService
	specgraphv1connect.SyncServiceGetSyncStatusProcedure: "sync.read",
	specgraphv1connect.SyncServiceSyncBeadsProcedure:     "sync.write",
	specgraphv1connect.SyncServiceSyncGitHubProcedure:    "sync.write",
	// AnalyticalPassService
	specgraphv1connect.AnalyticalPassServiceRunAnalyticalPassProcedure:   "analytical_pass.write",
	specgraphv1connect.AnalyticalPassServiceStoreFindingsProcedure:       "analytical_pass.write",
	specgraphv1connect.AnalyticalPassServiceListFindingsProcedure:        "analytical_pass.read",
	specgraphv1connect.AnalyticalPassServiceListProjectFindingsProcedure: "analytical_pass.read",
	// ExportService
	specgraphv1connect.ExportServiceExportProjectProcedure: "export.read",
	specgraphv1connect.ExportServiceImportProjectProcedure: "export.write",
	specgraphv1connect.ExportServiceVerifyExportProcedure:  "export.read",
	// SliceService
	specgraphv1connect.SliceServiceListSlicesProcedure:    "slice.read",
	specgraphv1connect.SliceServiceGetSliceProcedure:      "slice.read",
	specgraphv1connect.SliceServiceClaimSliceProcedure:    "slice.write",
	specgraphv1connect.SliceServiceCompleteSliceProcedure: "slice.write",
	// IdentityService — whoami is a self-read; all admin-management
	// operations use the "manage" verb (admin-only via base.cedar).
	specgraphv1connect.IdentityServiceWhoamiProcedure:               "identity.read",
	specgraphv1connect.IdentityServiceListUsersProcedure:            "user.manage",
	specgraphv1connect.IdentityServiceGetUserProcedure:              "user.manage",
	specgraphv1connect.IdentityServiceUpdateUserRoleProcedure:       "user.manage",
	specgraphv1connect.IdentityServiceSoftDeleteUserProcedure:       "user.manage",
	specgraphv1connect.IdentityServicePurgeUserProcedure:            "user.manage",
	specgraphv1connect.IdentityServiceCreateServiceAccountProcedure: "serviceaccount.manage",
	specgraphv1connect.IdentityServiceCreateAPIKeyProcedure:         "apikey.manage",
	specgraphv1connect.IdentityServiceRevokeAPIKeyProcedure:         "apikey.manage",
	specgraphv1connect.IdentityServiceRotateAPIKeyProcedure:         "apikey.manage",
	specgraphv1connect.IdentityServiceListAPIKeysProcedure:          "apikey.manage",
	specgraphv1connect.IdentityServiceListOIDCBindingsProcedure:     "oidc.manage",
	specgraphv1connect.IdentityServiceUnbindOIDCProcedure:           "oidc.manage",
	// Self-service API-key operations (AUTH-03, D-06): the caller acts on
	// their OWN keys. Gated by the "self" verb (apikey.self) — base.cedar
	// permits any authenticated role; the handler enforces the source/floor
	// Cedar cannot see. ResyncUserRole (AUTH-02, D-04) is an admin-only
	// reconciliation and uses the "manage" verb like the other user.* ops.
	specgraphv1connect.IdentityServiceCreateMyAPIKeyProcedure: "apikey.self",
	specgraphv1connect.IdentityServiceListMyAPIKeysProcedure:  "apikey.self",
	specgraphv1connect.IdentityServiceRotateMyAPIKeyProcedure: "apikey.self",
	specgraphv1connect.IdentityServiceRevokeMyAPIKeyProcedure: "apikey.self",
	specgraphv1connect.IdentityServiceResyncUserRoleProcedure: "user.manage",
}

// ActionForProcedure returns the stable action name for an RPC procedure.
func ActionForProcedure(procedure string) (string, bool) {
	a, ok := procedureActions[procedure]
	return a, ok
}

// ActionNames returns the distinct action names, sorted. Passed to
// NewCedarEngine to build the action-group entity graph.
func ActionNames() []string {
	seen := make(map[string]bool, len(procedureActions))
	for _, a := range procedureActions {
		seen[a] = true
	}
	names := make([]string, 0, len(seen))
	for a := range seen {
		names = append(names, a)
	}
	sort.Strings(names)
	return names
}
