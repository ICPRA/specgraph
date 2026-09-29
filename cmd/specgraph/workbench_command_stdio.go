// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specgraph/specgraph/gen/specgraph/v1/specgraphv1connect"
	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/config"
	"github.com/specgraph/specgraph/internal/server"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/spf13/cobra"
)

type workbenchCommandRequest struct {
	ID         string          `json:"id"`
	Operation  string          `json:"operation"`
	Credential string          `json:"credential"`
	Project    string          `json:"project"`
	Slug       string          `json:"slug,omitempty"`
	Body       json.RawMessage `json:"body"`
}

type workbenchCommandResponse struct {
	ID    string              `json:"id"`
	Data  json.RawMessage     `json:"data,omitempty"`
	Error *workbenchReadError `json:"error,omitempty"`
}

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:               "workbench-command-stdio",
		Short:             "Execute explicit authorized workbench commands over local stdio",
		Args:              cobra.NoArgs,
		PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.LoadGlobalExplicit(globalConfigPath())
			if err != nil {
				return errors.New("workbench-command-stdio: cannot load existing config; check --config")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), workbenchReadTimeout)
			store, err := postgres.OpenExisting(ctx, cfg.Server.Postgres.URL)
			cancel()
			if errors.Is(err, postgres.ErrSchemaVersionMismatch) {
				return runWorkbenchCommandStdio(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(),
					func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error) { return nil, err })
			}
			if err != nil {
				return errors.New("workbench-command-stdio: cannot open existing PostgreSQL database; check configuration and availability")
			}
			defer store.Close(cmd.Context()) //nolint:errcheck // Concrete postgres.Store.Close closes its pool and always returns nil.
			return runWorkbenchCommandStdio(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(),
				func(ctx context.Context, req workbenchCommandRequest, procedure string) (json.RawMessage, error) {
					procedures := []string{procedure}
					if req.Operation == "conversation-runs" || req.Operation == "graph-current" || req.Operation == "knowledge-search" || req.Operation == "knowledge-record" || req.Operation == "review-status" || req.Operation == "review-delivery-source" || req.Operation == "review-request-source" || req.Operation == "test-results" || req.Operation == "node-deliveries" || req.Operation == "dependency-state" || req.Operation == "node-mark-history" || req.Operation == "project-binding-history" || req.Operation == "program-loop-read" || req.Operation == "program-loop-history" {
						procedures = workbenchKnowledgeProcedures(req.Operation, req.Body)
					}
					identity, commandErr := resolveWorkbenchOperator(ctx, store, req.Credential, cfg.Auth.Policies.ExtraDirs, procedures[0], procedures[1:]...)
					if commandErr != nil {
						return nil, commandErr
					}
					if req.Operation == "test-result-submit" {
						if len(req.Body) > 32<<10 {
							return nil, storage.ErrInvalidTestReport
						}
						report, err := server.ExecuteAgentTestReport(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(report)
					}
					if req.Operation == "summary-status" || req.Operation == "summary-history" {
						return server.ReadLocalWorkbenchSummary(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "preview-node-merge" || req.Operation == "node-merge-history" || req.Operation == "node-merge-receipt" {
						return server.ReadLocalWorkbenchNodeMerge(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "candidate-loop-read" && identity.UserKind == storage.KindHuman && req.Project != "" {
						var selector struct {
							EnvironmentID   string `json:"environment_id"`
							NativeProjectID string `json:"native_project_id"`
						}
						if json.Unmarshal(req.Body, &selector) == nil && selector.EnvironmentID == "" && selector.NativeProjectID == "" {
							return server.ExecuteHumanCandidateLoopRead(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						}
					}
					if (req.Operation == "node-owner-read" || req.Operation == "node-owner-history") && identity.UserKind == storage.KindHuman && req.Project != "" {
						var selector struct {
							EnvironmentID   string `json:"environment_id"`
							NativeProjectID string `json:"native_project_id"`
						}
						if json.Unmarshal(req.Body, &selector) == nil && selector.EnvironmentID == "" && selector.NativeProjectID == "" {
							return server.ExecuteHumanNodeOwnershipRead(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
						}
					}
					if req.Operation == "candidate-satisfaction-history" && identity.UserKind == storage.KindHuman && req.Project != "" {
						var selector struct {
							EnvironmentID   string `json:"environment_id"`
							NativeProjectID string `json:"native_project_id"`
						}
						if json.Unmarshal(req.Body, &selector) == nil && selector.EnvironmentID == "" && selector.NativeProjectID == "" {
							return server.ExecuteHumanCandidateSatisfactionHistory(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						}
					}
					if req.Operation == "report-judgment-history" && identity.UserKind == storage.KindHuman && req.Project != "" {
						var selector struct {
							EnvironmentID   string `json:"environment_id"`
							NativeProjectID string `json:"native_project_id"`
						}
						if json.Unmarshal(req.Body, &selector) == nil && selector.EnvironmentID == "" && selector.NativeProjectID == "" {
							return server.ExecuteHumanReportJudgmentHistory(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						}
					}
					if req.Operation == "node-owner-read" || req.Operation == "node-owner-history" || req.Operation == "candidate-loop-read" || req.Operation == "candidate-satisfaction-history" || req.Operation == "report-flow-read" || req.Operation == "report-join-read" || req.Operation == "report-judgment-history" || req.Operation == "node-events" {
						result, err := server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					if req.Operation == "node-owner-take-begin" || req.Operation == "node-owner-take-commit" || req.Operation == "node-owner-take-cancel" || req.Operation == "node-owner-return" {
						return server.ExecuteNodeOwnershipCommand(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "confirm-program-run-stopped" {
						return server.ExecuteHumanProgramStop(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
					}
					if req.Operation == "node-handoff-summary-own" {
						return server.ExecuteOwnNodeHandoffSummary(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
					}
					if req.Operation == "report-flow-arm" || req.Operation == "report-flow-cancel" || req.Operation == "pm-report-flow-arm" || req.Operation == "pm-report-flow-cancel" || req.Operation == "report-join-arm" || req.Operation == "pm-report-join-arm" {
						return server.ExecuteReportBranchFlow(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "report-judgment-record" || req.Operation == "pm-report-judgment-record" {
						return server.ExecuteReportBranchJudgmentRecord(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "candidate-loop-arm" || req.Operation == "candidate-loop-stop" || req.Operation == "candidate-loop-abandon" || req.Operation == "pm-candidate-loop-arm" || req.Operation == "pm-candidate-loop-stop" {
						return server.ExecuteCandidateLoop(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "candidate-satisfaction-record" || req.Operation == "pm-candidate-satisfaction-record" || req.Operation == "candidate-satisfaction-own" {
						return server.ExecuteCandidateSatisfactionRecord(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "candidate-intervention-resolve" {
						return server.ExecuteCandidateInterventionResolve(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
					}
					if req.Operation == "candidate-next-own" {
						return server.ExecuteOwnCandidateNext(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
					}
					if req.Operation == "record-own-node-event" {
						event, err := server.ExecuteOwnNodeEvent(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(event)
					}
					if procedure == auth.WorkbenchManageSummaryProcedure || procedure == auth.WorkbenchAgentSummaryProcedure {
						return server.ExecuteWorkbenchSummary(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "host-list-hooks" || req.Operation == "host-read-completion-hook" || req.Operation == "host-authorize-completion-hook" || req.Operation == "host-result-completion-hook" {
						return server.ExecuteHostCompletionHook(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if procedure == auth.WorkbenchManageCompletionHookProcedure {
						if identity.UserKind != storage.KindHuman {
							return nil, storage.ErrProgramRunForbidden
						}
						var consumer *auth.Identity
						if req.Operation == "arm-completion-hook" {
							var private struct {
								HostCredential string `json:"hostCredential"`
							}
							if json.Unmarshal(req.Body, &private) != nil || private.HostCredential == "" {
								return nil, storage.ErrInvalidCompletionHook
							}
							consumer, commandErr = resolveWorkbenchOperator(ctx, store, private.HostCredential, cfg.Auth.Policies.ExtraDirs, auth.WorkbenchDispatchCompletionHookProcedure)
							if commandErr != nil {
								return nil, commandErr
							}
						}
						return server.ExecuteHumanCompletionHook(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body), consumer)
					}
					if procedure == auth.WorkbenchPrepareProgramRunProcedure {
						if identity.UserKind != storage.KindHuman {
							return nil, storage.ErrProgramRunForbidden
						}
						var private struct {
							HostCredential string `json:"hostCredential"`
						}
						if json.Unmarshal(req.Body, &private) != nil || private.HostCredential == "" {
							return nil, storage.ErrInvalidProgramRun
						}
						consumer, err := resolveWorkbenchOperator(ctx, store, private.HostCredential, cfg.Auth.Policies.ExtraDirs, auth.WorkbenchExecuteProgramRunProcedure)
						if err != nil {
							return nil, err
						}
						return server.ExecuteHumanProgramRun(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body), consumer)
					}
					if procedure == auth.WorkbenchManageProgramLoopProcedure {
						return server.ExecuteHumanProgramLoop(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if strings.HasPrefix(req.Operation, "host-") && (procedure == auth.WorkbenchReadProgramRunProcedure || procedure == auth.WorkbenchExecuteProgramRunProcedure) {
						return server.ExecuteHostProgramRun(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if procedure == auth.WorkbenchAgentPlanningDispatchProcedure {
						return server.ExecuteAgentPlanningDispatch(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if strings.HasPrefix(req.Operation, "host-") && (procedure == auth.WorkbenchDispatchDeliveryHookProcedure || procedure == auth.WorkbenchReadDeliveryHookProcedure) {
						return server.ExecuteHostDeliveryHook(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if procedure == auth.WorkbenchManageDeliveryHookProcedure {
						if identity.UserKind != storage.KindHuman {
							return nil, storage.ErrPlanningForbidden
						}
						var consumer *auth.Identity
						if req.Operation == "arm-delivery-hook" {
							var private struct {
								HostCredential string `json:"hostCredential"`
							}
							if json.Unmarshal(req.Body, &private) != nil || private.HostCredential == "" {
								return nil, storage.ErrInvalidDeliveryHook
							}
							consumer, commandErr = resolveWorkbenchOperator(ctx, store, private.HostCredential, cfg.Auth.Policies.ExtraDirs, auth.WorkbenchDispatchDeliveryHookProcedure)
							if commandErr != nil {
								return nil, commandErr
							}
							if consumer.UserKind != storage.KindServiceAccount || consumer.Source != "apikey" {
								return nil, storage.ErrPlanningForbidden
							}
						}
						return server.ExecuteHumanDeliveryHook(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body), consumer)
					}
					if procedure == auth.WorkbenchConfigureDeliveryHookProcedure || procedure == auth.WorkbenchReadDeliveryHookProcedure {
						return server.ExecuteAgentDeliveryHook(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if strings.HasPrefix(req.Operation, "pm-") {
						return server.ExecuteAgentPlanning(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
					}
					if req.Operation == "run-complete-self" {
						if len(req.Body) > 32<<10 {
							return nil, storage.ErrInvalidRunBinding
						}
						result, err := server.ExecuteAgentRunCompletion(auth.WithIdentity(ctx, identity), store, req.Project, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					if req.Operation == "delivery-self-context" || req.Operation == "delivery-submit-self" {
						if len(req.Body) > 32<<10 {
							return nil, storage.ErrInvalidDeliverySubmission
						}
						result, err := server.ExecuteAgentDelivery(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					if req.Operation == "review-assign" || req.Operation == "review-submit" {
						if len(req.Body) > 32<<10 {
							return nil, storage.ErrInvalidReview
						}
						result, err := server.ExecuteAgentReview(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					if req.Operation == "conversation-runs" || req.Operation == "graph-current" || req.Operation == "knowledge-search" || req.Operation == "knowledge-record" || req.Operation == "review-status" || req.Operation == "review-delivery-source" || req.Operation == "review-request-source" || req.Operation == "test-results" || req.Operation == "node-deliveries" || req.Operation == "dependency-state" || req.Operation == "node-mark-history" || req.Operation == "project-binding-history" || req.Operation == "program-loop-read" || req.Operation == "program-loop-history" {
						result, err := server.ExecuteLocalWorkbenchKnowledge(auth.WithIdentity(ctx, identity), store, req.Project, req.Operation, bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					if strings.HasPrefix(req.Operation, "mail-") {
						if req.Operation == "mail-initialize" {
							if _, err := resolveWorkbenchOperator(ctx, store, req.Credential, cfg.Auth.Policies.ExtraDirs, specgraphv1connect.IdentityServiceCreateAPIKeyProcedure); err != nil {
								return nil, err
							}
							result, err := server.CreateLocalMailCredential(auth.WithIdentity(ctx, identity), store.ExistingAuth(), identity.UserID)
							if err != nil {
								return nil, err
							}
							return json.Marshal(result)
						}
						if req.Operation == "mail-grant" || req.Operation == "mail-bind" {
							result, err := server.ExecuteWorkbenchMailEnrollment(auth.WithIdentity(ctx, identity), store, req.Project, strings.TrimPrefix(req.Operation, "mail-"), bytes.NewReader(req.Body))
							if err != nil {
								return nil, err
							}
							return json.Marshal(result)
						}
						if req.Project == "" {
							var body struct {
								Scope storage.MailScope `json:"scope"`
							}
							if json.Unmarshal(req.Body, &body) != nil {
								return nil, storage.ErrMailInvalid
							}
							req.Project, commandErr = store.ProjectForLocalMailScope(ctx, body.Scope)
							if commandErr != nil {
								return nil, commandErr
							}
						}
						result, err := server.ExecuteLocalWorkbenchMail(auth.WithIdentity(ctx, identity), store, req.Project, strings.TrimPrefix(req.Operation, "mail-"), bytes.NewReader(req.Body))
						if err != nil {
							return nil, err
						}
						return json.Marshal(result)
					}
					return server.ExecuteWorkbenchCommand(auth.WithIdentity(ctx, identity), store, req.Operation, req.Project, req.Slug, identity.UserID, req.Body)
				})
		},
	})
}

func workbenchKnowledgeProcedures(operation string, body json.RawMessage) []string {
	if operation == "node-owner-read" || operation == "node-owner-history" || operation == "candidate-loop-read" || operation == "candidate-satisfaction-history" || operation == "report-flow-read" || operation == "report-join-read" || operation == "report-judgment-history" || operation == "node-events" {
		return []string{auth.WorkbenchReadProgramRunProcedure}
	}
	if operation == "conversation-runs" {
		return []string{auth.WorkbenchReadConversationRunsProcedure}
	}
	if operation == "program-loop-read" || operation == "program-loop-history" {
		return []string{auth.WorkbenchReadProgramRunProcedure}
	}
	if operation == "graph-current" {
		return []string{specgraphv1connect.GraphServiceGetFullGraphProcedure}
	}
	if operation == "review-status" || operation == "review-delivery-source" || operation == "review-request-source" || operation == "test-results" || operation == "node-deliveries" || operation == "dependency-state" || operation == "node-mark-history" || operation == "project-binding-history" {
		return []string{specgraphv1connect.SpecServiceGetSpecProcedure}
	}
	if operation == "knowledge-search" {
		return []string{specgraphv1connect.SpecServiceGetSpecProcedure, specgraphv1connect.DecisionServiceGetDecisionProcedure, specgraphv1connect.SpecServiceListChangesProcedure}
	}
	var req struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(body, &req) != nil {
		return nil
	}
	switch req.Kind {
	case "spec":
		return []string{specgraphv1connect.SpecServiceGetSpecProcedure}
	case "decision":
		return []string{specgraphv1connect.DecisionServiceGetDecisionProcedure}
	case "change":
		return []string{specgraphv1connect.SpecServiceListChangesProcedure}
	default:
		return nil
	}
}

func runWorkbenchCommandStdio(ctx context.Context, input io.Reader, output io.Writer,
	execute func(context.Context, workbenchCommandRequest, string) (json.RawMessage, error),
) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), (2<<20)+2)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var req workbenchCommandRequest
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		err := decoder.Decode(&req)
		procedure := ""
		switch req.Operation {
		case "node-handoff-summary-own":
			procedure = auth.WorkbenchRecordOwnNodeHandoffSummaryProcedure
		case "node-owner-read", "node-owner-history":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "node-owner-take-begin", "node-owner-take-commit", "node-owner-take-cancel", "node-owner-return", "confirm-program-run-stopped":
			procedure = auth.WorkbenchDispatchProcedure
		case "candidate-satisfaction-own":
			procedure = auth.WorkbenchRecordOwnCandidateJudgmentProcedure
		case "candidate-satisfaction-record", "candidate-intervention-resolve":
			procedure = auth.WorkbenchPrepareRunProcedure
		case "pm-candidate-satisfaction-record":
			procedure = auth.WorkbenchAgentPlanningProcedure
		case "candidate-satisfaction-history":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "record-own-node-event":
			procedure = auth.WorkbenchRecordOwnNodeEventProcedure
		case "node-events":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "report-judgment-record":
			procedure = auth.WorkbenchPrepareRunProcedure
		case "pm-report-judgment-record":
			procedure = auth.WorkbenchAgentPlanningProcedure
		case "report-judgment-history":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "candidate-loop-arm", "candidate-loop-stop", "candidate-loop-abandon":
			procedure = auth.WorkbenchPrepareRunProcedure
		case "pm-candidate-loop-arm", "pm-candidate-loop-stop":
			procedure = auth.WorkbenchAgentPlanningProcedure
		case "candidate-loop-read":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "candidate-next-own":
			procedure = auth.WorkbenchSubmitOwnDeliveryProcedure
		case "report-flow-arm", "report-flow-cancel", "report-join-arm":
			procedure = auth.WorkbenchPrepareRunProcedure
		case "pm-report-flow-arm", "pm-report-flow-cancel", "pm-report-join-arm":
			procedure = auth.WorkbenchAgentPlanningProcedure
		case "report-flow-read", "report-join-read":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "arm-completion-hook", "cancel-completion-hook", "retry-completion-hook":
			procedure = auth.WorkbenchManageCompletionHookProcedure
		case "host-list-hooks", "host-read-completion-hook":
			procedure = auth.WorkbenchReadCompletionHookProcedure
		case "host-authorize-completion-hook", "host-result-completion-hook":
			procedure = auth.WorkbenchDispatchCompletionHookProcedure
		case "summary-status", "summary-history":
			procedure = specgraphv1connect.SpecServiceGetSpecProcedure
		case "preview-node-merge", "node-merge-history", "node-merge-receipt":
			procedure = specgraphv1connect.SpecServiceGetSpecProcedure
		case "merge-nodes":
			procedure = auth.WorkbenchMergeNodesProcedure
		case "record-summary-disposition", "accept-summary", "revoke-summary-acceptance":
			procedure = auth.WorkbenchManageSummaryProcedure
		case "pm-summary-disposition", "pm-accept-summary", "pm-revoke-summary-acceptance":
			procedure = auth.WorkbenchAgentSummaryProcedure
		case "prepare-program-run":
			procedure = auth.WorkbenchPrepareProgramRunProcedure
		case "host-read-program-run", "host-read-program-loop-history":
			procedure = auth.WorkbenchReadProgramRunProcedure
		case "host-authorize-program-run", "host-result-program-run", "host-complete-program-run", "host-authorize-next-program-attempt", "host-result-program-attempt", "host-stop-program-loop":
			procedure = auth.WorkbenchExecuteProgramRunProcedure
		case "record-program-loop-decision", "stop-program-loop":
			procedure = auth.WorkbenchManageProgramLoopProcedure
		case "arm-delivery-hook", "cancel-delivery-hook", "retry-delivery-hook":
			procedure = auth.WorkbenchManageDeliveryHookProcedure
		case "pm-arm-delivery-hook", "pm-cancel-delivery-hook", "pm-retry-delivery-hook":
			procedure = auth.WorkbenchConfigureDeliveryHookProcedure
		case "pm-read-delivery-hook", "pm-list-delivery-hooks", "delivery-hook-context", "host-list-delivery-hooks", "host-read-delivery-hook":
			procedure = auth.WorkbenchReadDeliveryHookProcedure
		case "host-bind-delivery-hook", "host-authorize-delivery-hook", "host-result-delivery-hook":
			procedure = auth.WorkbenchDispatchDeliveryHookProcedure
		case "pm-prepare-run", "pm-bind-run", "pm-authorize-run", "pm-read-preparation":
			procedure = auth.WorkbenchAgentPlanningDispatchProcedure
		case "pm-create-node", "pm-merge-nodes", "pm-subdivide-node", "pm-add-dependency", "pm-remove-dependency", "pm-approve-node", "pm-set-node-mark", "pm-record-program-loop-decision", "pm-stop-program-loop":
			procedure = auth.WorkbenchAgentPlanningProcedure
		case "delivery-self-context":
			procedure = auth.WorkbenchReadOwnDeliveryContextProcedure
		case "delivery-submit-self":
			procedure = auth.WorkbenchSubmitOwnDeliveryProcedure
		case "record-node-event":
			procedure = auth.WorkbenchRecordNodeEventProcedure
		case "run-complete-self":
			procedure = auth.WorkbenchCompleteAgentRunProcedure
		case "record-test-result":
			procedure = auth.WorkbenchRecordTestResultProcedure
		case "test-result-submit":
			procedure = auth.WorkbenchSubmitAgentTestResultProcedure
		case "review-assign":
			procedure = auth.WorkbenchAssignAgentReviewProcedure
		case "review-submit":
			procedure = auth.WorkbenchSubmitAgentReviewProcedure
		case "abandon-node":
			procedure = specgraphv1connect.LifecycleServiceTransitionAbandonProcedure
		case "set-node-mark":
			procedure = auth.WorkbenchSetNodeMarkProcedure
		case "bind-project", "unbind-project":
			procedure = auth.WorkbenchBindProjectProcedure
		case "conversation-runs", "graph-current", "knowledge-search", "knowledge-record", "review-status", "review-delivery-source", "review-request-source", "test-results", "node-deliveries", "dependency-state", "node-mark-history", "project-binding-history", "program-loop-read", "program-loop-history":
			if procedures := workbenchKnowledgeProcedures(req.Operation, req.Body); len(procedures) != 0 {
				procedure = procedures[0]
			}
		case "subdivide-node":
			procedure = auth.WorkbenchSubdivisionProcedure
		case "create-node":
			procedure = specgraphv1connect.SpecServiceCreateSpecProcedure
		case "add-dependency", "remove-dependency":
			procedure = auth.WorkbenchEditDependencyProcedure
		case "manual-complete":
			procedure = auth.WorkbenchManualCompleteProcedure
		case "review-delivery":
			procedure = auth.WorkbenchAcceptDeliveryProcedure
		case "assign-review", "review-source":
			procedure = auth.WorkbenchAcceptDeliveryProcedure
		case "submit-delivery":
			procedure = auth.WorkbenchSubmitDeliveryProcedure
		case "complete-run":
			procedure = auth.WorkbenchCompleteRunProcedure
		case "prepare-run":
			procedure = auth.WorkbenchPrepareRunProcedure
		case "bind-run":
			procedure = auth.WorkbenchBindRunThreadProcedure
		case "approve-node", "authorize-run", "resolve-run", "confirm-run-stopped", "cancel-preparation", "abort-preparation":
			procedure = auth.WorkbenchDispatchProcedure
		case "mail-inbox", "mail-thread", "mail-context", "mail-directory", "mail-owned", "mail-retired", "mail-owner-history":
			procedure = auth.WorkbenchMailReadProcedure
		case "mail-grant", "mail-bind":
			procedure = auth.WorkbenchMailManageProcedure
		case "mail-initialize":
			procedure = specgraphv1connect.IdentityServiceCreateServiceAccountProcedure
		case "mail-send", "mail-read", "mail-ack", "mail-close", "mail-handoff", "mail-takeover":
			procedure = auth.WorkbenchMailWriteProcedure
		case "takeover-mail":
			// actions.go maps InspectMail to workbench.manage, not mail.read.
			// TakeoverMail additionally rejects non-human identities on this route.
			procedure = auth.WorkbenchInspectMailProcedure
		}
		mail := strings.HasPrefix(req.Operation, "mail-")
		agentReview := req.Operation == "node-handoff-summary-own" || req.Operation == "candidate-satisfaction-own" || req.Operation == "record-own-node-event" || req.Operation == "candidate-next-own" || req.Operation == "review-assign" || req.Operation == "review-submit" || req.Operation == "test-result-submit" || req.Operation == "run-complete-self" || req.Operation == "delivery-self-context" || req.Operation == "delivery-submit-self" || strings.HasPrefix(req.Operation, "pm-")
		knowledge := req.Operation == "node-owner-read" || req.Operation == "node-owner-history" || req.Operation == "candidate-satisfaction-history" || req.Operation == "node-events" || req.Operation == "report-judgment-history" || req.Operation == "candidate-loop-read" || req.Operation == "report-flow-read" || req.Operation == "report-join-read" || req.Operation == "conversation-runs" || req.Operation == "graph-current" || req.Operation == "knowledge-search" || req.Operation == "knowledge-record" || req.Operation == "review-status" || req.Operation == "review-delivery-source" || req.Operation == "review-request-source" || req.Operation == "test-results" || req.Operation == "node-deliveries" || req.Operation == "dependency-state" || req.Operation == "node-mark-history" || req.Operation == "project-binding-history" || req.Operation == "program-loop-read" || req.Operation == "program-loop-history" || req.Operation == "preview-node-merge" || req.Operation == "node-merge-history" || req.Operation == "node-merge-receipt"
		hookHost := strings.HasPrefix(req.Operation, "host-") || req.Operation == "delivery-hook-context"
		automaticMailProject := (mail && req.Operation != "mail-grant" && req.Operation != "mail-bind") || agentReview || hookHost || req.Operation == "summary-status" || req.Operation == "summary-history"
		if procedure == auth.WorkbenchReadProgramRunProcedure || procedure == auth.WorkbenchExecuteProgramRunProcedure {
			automaticMailProject = false
		}
		limit := workbenchRequestLimit
		if mail || req.Operation == "takeover-mail" {
			limit = 512 << 10
		}
		if req.Operation == "subdivide-node" || req.Operation == "pm-subdivide-node" || req.Operation == "accept-summary" || req.Operation == "pm-accept-summary" || req.Operation == "merge-nodes" || req.Operation == "pm-merge-nodes" {
			limit = 2 << 20
		}
		withoutSlug := req.Operation == "node-owner-take-begin" || req.Operation == "node-owner-take-commit" || req.Operation == "node-owner-take-cancel" || req.Operation == "node-owner-return" || req.Operation == "confirm-program-run-stopped" || req.Operation == "candidate-satisfaction-record" || req.Operation == "candidate-intervention-resolve" || req.Operation == "report-judgment-record" || req.Operation == "candidate-loop-arm" || req.Operation == "candidate-loop-stop" || req.Operation == "candidate-loop-abandon" || req.Operation == "report-flow-arm" || req.Operation == "report-flow-cancel" || req.Operation == "report-join-arm" || req.Operation == "takeover-mail" || req.Operation == "create-node" || req.Operation == "merge-nodes" || req.Operation == "prepare-run" || req.Operation == "prepare-program-run" || req.Operation == "abort-preparation" || req.Operation == "submit-delivery" || mail || knowledge || agentReview || hookHost || procedure == auth.WorkbenchManageDeliveryHookProcedure || procedure == auth.WorkbenchManageCompletionHookProcedure || procedure == auth.WorkbenchManageSummaryProcedure || req.Operation == "summary-status" || req.Operation == "summary-history"
		withoutSlug = withoutSlug || procedure == auth.WorkbenchManageProgramLoopProcedure
		validSlug := (withoutSlug && req.Slug == "") || (!withoutSlug && strings.TrimSpace(req.Slug) != "")
		if req.Operation == "record-test-result" {
			validSlug = true
		} // Optional URL selector is checked against body.deliveryId by the handler.
		response := workbenchCommandResponse{ID: req.ID}
		if err != nil || decoder.Decode(new(any)) != io.EOF || len(scanner.Bytes()) > limit ||
			strings.TrimSpace(req.ID) == "" || (strings.TrimSpace(req.Project) == "" && !automaticMailProject && !knowledge) || procedure == "" || !validSlug || len(req.Body) == 0 {
			response.Error = &workbenchReadError{Code: "invalid_request", Message: fmt.Sprintf("invalid workbench command fields; maximum %d bytes", limit)}
		} else {
			commandCtx, cancel := context.WithTimeout(ctx, workbenchReadTimeout)
			data, err := execute(commandCtx, req, procedure)
			cancel()
			response.Data = data
			if err != nil {
				response.Error = workbenchCommandError(err)
			}
		}
		if err := encoder.Encode(response); err != nil {
			return fmt.Errorf("workbench-command-stdio: write response: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		if writeErr := encoder.Encode(workbenchCommandResponse{Error: &workbenchReadError{Code: "invalid_request", Message: "cannot read request line; maximum 2 MiB (smaller limits apply by operation)"}}); writeErr != nil {
			return fmt.Errorf("workbench-command-stdio: write response: %w", writeErr)
		}
		return fmt.Errorf("workbench-command-stdio: read input: %w", err)
	}
	return nil
}

func workbenchCommandError(err error) *workbenchReadError {
	var databaseError *pgconn.PgError
	switch {
	case errors.Is(err, storage.ErrInvalidNodeOwnership):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidProjectBinding):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrProjectBindingNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrProjectBindingMismatch):
		return &workbenchReadError{Code: "project_binding_mismatch", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeOwnershipConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeOwnershipNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeOwnershipPending), errors.Is(err, storage.ErrNodeOwnershipHandoffRequired):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidCandidateSatisfaction):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateSatisfactionConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateInterventionNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateInterventionHeld):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidReportBranchJudgment):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrReportBranchJudgmentConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrReportBranchJudgmentNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidCandidateLoop):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateLoopConflict), errors.Is(err, storage.ErrCandidateBudgetHeld):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateLoopNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrCandidateLoopWaiting), errors.Is(err, storage.ErrCandidateLoopStopped), errors.Is(err, storage.ErrCandidateConditionNotMet), errors.Is(err, storage.ErrCandidateAttemptsExhausted), errors.Is(err, storage.ErrCandidateLoopCompleted):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidReportBranchFlow), errors.Is(err, storage.ErrInvalidReportFlowJoin):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrReportBranchFlowConflict), errors.Is(err, storage.ErrReportFlowJoinConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrReportBranchFlowNotFound), errors.Is(err, storage.ErrReportFlowJoinNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrReportBranchFlowNotSelected), errors.Is(err, storage.ErrReportFlowJoinUnsatisfied):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidProgramLoop):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramLoopConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramLoopWaiting), errors.Is(err, storage.ErrProgramLoopStopped), errors.Is(err, storage.ErrProgramLoopNeedsHuman):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidCompletionHook):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrCompletionHookConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrCompletionHookNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramHookRequired):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidSummary):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidNodeMerge):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeMergeConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeMergeForbidden):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeMergeNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrSummaryForbidden):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrSummaryConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrSummaryNotAcceptable):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrSummaryAcceptanceNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidProgramRun):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramRunForbidden):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramResultConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrProgramCompletionUnavailable):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidDeliveryHook):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrDeliveryHookConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrDeliveryHookNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrDependencyCycle):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrPlanningForbidden):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidDeliverySubmission):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidDeliveryCursor):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidConversationRunCursor):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidNodeEvent):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrNodeEventConflict):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrImplementationTestsRequired):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidTestReport):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrTestReportForbidden):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrReviewForbidden), errors.Is(err, storage.ErrReviewSelfReview):
		return &workbenchReadError{Code: "forbidden", Message: err.Error()}
	case errors.Is(err, storage.ErrReviewHumanHold), errors.Is(err, storage.ErrReviewAlreadyDecided):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrInvalidReview):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrReviewRequestNotFound):
		return &workbenchReadError{Code: "not_found", Message: err.Error()}
	case errors.Is(err, storage.ErrAbandonExecutionPending):
		return &workbenchReadError{Code: "failed_precondition", Message: storage.ErrAbandonExecutionPending.Error()}
	case errors.Is(err, postgres.ErrWorkbenchProjectAssociationMissing):
		return &workbenchReadError{Code: "project_association_missing", Message: "no recorded SpecGraph project association; configure the host project mapping"}
	case errors.Is(err, postgres.ErrWorkbenchProjectAssociationAmbiguous):
		return &workbenchReadError{Code: "project_association_ambiguous", Message: "multiple recorded SpecGraph project associations; configure the host project mapping"}
	case errors.Is(err, postgres.ErrSchemaVersionMismatch):
		return &workbenchReadError{Code: "schema_mismatch", Message: "database requires the current initialization schema; no data was changed"}
	case errors.Is(err, auth.ErrUnauthenticated):
		return &workbenchReadError{Code: "unauthorized", Message: "valid existing SpecGraph API key (spgr_sk_) or session (spgr_ws_) required"}
	case errors.Is(err, storage.ErrMailForbidden):
		return &workbenchReadError{Code: "forbidden", Message: "permission for this workbench operation is required"}
	case errors.Is(err, storage.ErrMailInvalid):
		return &workbenchReadError{Code: "invalid_argument", Message: "invalid mail request"}
	case errors.Is(err, storage.ErrInvalidRunPreparation), errors.Is(err, storage.ErrInvalidRunBinding):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case errors.Is(err, storage.ErrRunBindingConflict), errors.Is(err, storage.ErrNotClaimOwner), errors.Is(err, storage.ErrAgentNotClaimOwner):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrSpecNotApproved), errors.Is(err, storage.ErrDependenciesNotReady), errors.Is(err, storage.ErrExecutionDependenciesChanged), errors.Is(err, storage.ErrDispatchResolved), errors.Is(err, storage.ErrPreparationCancelled), errors.Is(err, storage.ErrCompletionRequiresRequirementReview), errors.Is(err, storage.ErrManagedCompletionRequiresAcceptance):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case errors.Is(err, storage.ErrMailNotFound), errors.Is(err, storage.ErrRunBindingNotFound), errors.Is(err, storage.ErrRunContextNotFound):
		return &workbenchReadError{Code: "not_found", Message: "mail resource or run not found"}
	case errors.Is(err, storage.ErrMailConflict), errors.Is(err, storage.ErrMailClosed):
		return &workbenchReadError{Code: "conflict", Message: "mail request conflicts with stored state"}
	case errors.Is(err, context.DeadlineExceeded), connect.CodeOf(err) == connect.CodeDeadlineExceeded:
		return &workbenchReadError{Code: "timeout", Message: "workbench command timed out; verify stored state before another submission"}
	case errors.As(err, &databaseError) && (databaseError.Code == "42703" || databaseError.Code == "42P01"):
		return &workbenchReadError{Code: "schema_mismatch", Message: "database schema does not match this build; no migration was run"}
	case errors.Is(err, storage.ErrSpecNotFound), errors.Is(err, storage.ErrProjectNotFound), errors.Is(err, storage.ErrDecisionNotFound), errors.Is(err, storage.ErrDeliveryNotFound), errors.Is(err, postgres.ErrWorkbenchChangeNotFound):
		return &workbenchReadError{Code: "not_found", Message: "project, node or delivery not found"}
	case errors.Is(err, storage.ErrConcurrentModification), errors.Is(err, storage.ErrManualCompletionConflict), errors.Is(err, storage.ErrSpecAlreadyClaimed):
		return &workbenchReadError{Code: "conflict", Message: err.Error()}
	case errors.Is(err, storage.ErrSpecTerminal), errors.Is(err, storage.ErrSpecIneligibleStage), errors.Is(err, storage.ErrSummaryNotExecutable), errors.Is(err, storage.ErrDispatchResponsibilityHeld):
		return &workbenchReadError{Code: "failed_precondition", Message: err.Error()}
	case connect.CodeOf(err) == connect.CodeInvalidArgument, errors.Is(err, storage.ErrInvalidSubdivisionRequest):
		return &workbenchReadError{Code: "invalid_argument", Message: err.Error()}
	case connect.CodeOf(err) == connect.CodeAlreadyExists, errors.Is(err, storage.ErrSpecAlreadyExists):
		return &workbenchReadError{Code: "already_exists", Message: "node already exists"}
	default:
		return &workbenchReadError{Code: "internal", Message: "workbench command failed; verify stored state before another submission"}
	}
}
