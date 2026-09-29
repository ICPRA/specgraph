// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

package storage

import "errors"

// --- Spec errors ---

// ErrSpecNotFound is returned when a spec does not exist.
var ErrSpecNotFound = errors.New("spec not found")

// ErrSpecAlreadyExists is returned when creating a spec with a slug that already exists.
var ErrSpecAlreadyExists = errors.New("spec already exists")

// ErrSpecAlreadyApproved is returned when attempting to modify an already-approved spec.
var ErrSpecAlreadyApproved = errors.New("spec is already approved")

// ErrSpecSuperseded is returned when attempting to amend a spec that has been superseded.
var ErrSpecSuperseded = errors.New("spec has been superseded and cannot be amended")

// ErrInvalidStageTransition is returned when a stage transition violates funnel rules.
var ErrInvalidStageTransition = errors.New("invalid stage transition")

// --- Claim errors ---

// ErrSpecAlreadyClaimed is returned when a spec has an active claim by another agent.
var ErrSpecAlreadyClaimed = errors.New("spec already claimed")

// ErrNotClaimOwner is returned when the agent does not own the claim.
var ErrNotClaimOwner = errors.New("agent does not own the claim")

// ErrSpecNotClaimed is returned when the spec is not claimed.
var ErrSpecNotClaimed = errors.New("spec is not claimed")

// --- Execution errors ---

// ErrSpecNotApproved is returned when a bundle is requested for a spec not in an executable stage.
var ErrSpecNotApproved = errors.New("spec is not in an approved or in_progress stage")

// ErrAgentNotClaimOwner is returned when an agent reports an event but does not hold the claim.
var ErrAgentNotClaimOwner = errors.New("agent does not hold the claim for this spec")

// ErrManagedCompletionRequiresAcceptance is returned when RecordCompletion is
// called in a managed project without an accepted delivery: completion claims
// must be confirmed by real evidence and an acceptance verdict (plan v1 §7.2).
var ErrManagedCompletionRequiresAcceptance = errors.New("managed project: completion requires an accepted delivery with evidence")

// ErrCompletionRequiresRequirementReview preserves the old acceptance when the
// primary requirement baseline is absent or differs from the current contract.
var ErrCompletionRequiresRequirementReview = errors.New("completion requires requirement review: primary requirement baseline is missing or changed")

// ErrDependenciesNotReady prevents preparation before direct prerequisites finish.
var ErrDependenciesNotReady = errors.New("execution prerequisites are unfinished or unsupported")

// ErrExecutionDependenciesChanged requires review of a changed relationship baseline.
var ErrExecutionDependenciesChanged = errors.New("execution dependency baseline is missing or changed; review is required")

// ErrDependencyInUse prevents deletion from silently satisfying a dependency.
var ErrDependencyInUse = errors.New("node remains a prerequisite; explicitly resolve its dependency relationships before removal")

// ErrDependencyCycle rejects a prerequisite edge that would introduce a graph cycle.
var ErrDependencyCycle = errors.New("adding this prerequisite would create a dependency cycle")

// ErrPlanningForbidden rejects planning outside the current project's verified host-bound manager run.
var ErrPlanningForbidden = errors.New("planning requires the current host-bound manager run in this project")

// ErrSummaryNotExecutable separates a summary container from implementation work.
var ErrSummaryNotExecutable = errors.New("summary node is not an executable work item; use its child tasks and current-structure review")

// ErrSubdivisionNotFound denotes an absent operation in the selected project.
var ErrSubdivisionNotFound = errors.New("subdivision not found")

// ErrInvalidSubdivisionRequest rejects malformed proposed children or a missing subdivision reason.
var ErrInvalidSubdivisionRequest = errors.New("invalid subdivision request")

// ErrRunContextNotFound denotes an absent prepared context in the selected project.
var ErrRunContextNotFound = errors.New("recorded run context not found")

// ErrInvalidRunPreparation rejects preparation input that does not satisfy its fixed task and target contract.
var ErrInvalidRunPreparation = errors.New("invalid run preparation request")

// ErrPreparationCancelled prevents reuse or admission of a cancelled preparation request.
var ErrPreparationCancelled = errors.New("preparation request was cancelled")

// ErrDispatchResponsibilityHeld prevents conflicting work while an admitted run's business responsibility remains held.
var ErrDispatchResponsibilityHeld = errors.New("dispatch responsibility is unresolved")

// ErrDispatchResolved rejects further dispatch operations after responsibility was released.
var ErrDispatchResolved = errors.New("dispatch responsibility has already been released")

// ErrAbandonExecutionPending requires explicit execution cleanup before abandoning a node.
var ErrAbandonExecutionPending = errors.New("confirm dispatched runs stopped and release active claims before abandoning")

// ErrReviewForbidden rejects an unverified review principal or missing explicit reviewer assignment.
var ErrReviewForbidden = errors.New("verified review principal or explicit reviewer assignment required")

// ErrReviewSelfReview rejects a reviewer sharing the writer's run or native environment/thread identity.
var ErrReviewSelfReview = errors.New("reviewer must be a different run and native environment/thread from the writer")

// ErrReviewHumanHold prevents agent review or completion while explicit human intervention is required.
var ErrReviewHumanHold = errors.New("review requires human intervention; use source review to resolve the hold")

// ErrReviewAlreadyDecided prevents another decision on an already decided assigned request.
var ErrReviewAlreadyDecided = errors.New("this assigned review request already has a decision")

// ErrReviewRequestNotFound denotes an absent assigned request in the selected project.
var ErrReviewRequestNotFound = errors.New("review request not found")

// ErrInvalidReview rejects malformed review input or sources that do not meet the assigned contract.
var ErrInvalidReview = errors.New("invalid review request")

// ErrInvalidNodeEvent rejects malformed operator events or references outside the specified node context.
var ErrInvalidNodeEvent = errors.New("node event requires a valid event ID, reason and matching node/run/delivery references")

// ErrInvalidDeliveryCursor rejects a browsing cursor outside the node's project-scoped delivery history.
var ErrInvalidDeliveryCursor = errors.New("delivery cursor must reference a delivery of this node in this project")

// ErrInvalidConversationRunCursor rejects a run outside the scoped conversation history.
var ErrInvalidConversationRunCursor = errors.New("run cursor must reference a run of this conversation in this project")

// ErrInvalidDeliverySubmission rejects a self-submission that mismatches its own run or native HEAD shape.
var ErrInvalidDeliverySubmission = errors.New("delivery submission requires the expected own run, summary and native HEAD shape")

// ErrNodeEventConflict rejects reuse of a recorded operator event ID rather than inferring another occurrence.
var ErrNodeEventConflict = errors.New("node event ID already recorded; inspect event history before another submission")

// ErrInvalidTestReport rejects malformed assertions or mismatched fixed delivery and test-plan references.
var ErrInvalidTestReport = errors.New("invalid test report or fixed delivery/plan reference")

// Report branch errors distinguish malformed input, fixed-definition conflicts and unmet admission facts.
var (
	ErrInvalidReportBranchFlow      = errors.New("invalid report branch flow")
	ErrReportBranchFlowConflict     = errors.New("report branch flow conflicts with an existing occurrence or run")
	ErrReportBranchFlowNotFound     = errors.New("report branch flow not found")
	ErrReportBranchFlowNotSelected  = errors.New("report branch is cancelled, unknown or not selected")
	ErrInvalidReportBranchJudgment  = errors.New("invalid report branch judgment request")
	ErrReportBranchJudgmentConflict = errors.New("report branch judgment predecessor, source or actor conflict")
	ErrReportBranchJudgmentNotFound = errors.New("report branch judgment condition not found")
	ErrInvalidReportFlowJoin        = errors.New("invalid report flow join")
	ErrReportFlowJoinConflict       = errors.New("report flow join conflicts with the original run or configuration")
	ErrReportFlowJoinNotFound       = errors.New("report flow join not found")
	ErrReportFlowJoinUnsatisfied    = errors.New("report flow join is cancelled or not satisfied")
)

// ErrTestReportForbidden rejects a reporter outside the authorized operator or bound implementation/test run.
var ErrTestReportForbidden = errors.New("test reporting requires the authorized operator or bound implementation/test_execution run")

// ErrImplementationTestsRequired prevents completion until each assigned plan has a latest passed report for the current commit.
var ErrImplementationTestsRequired = errors.New("implementation completion requires passed latest reports for every assigned test plan on the current delivery commit")

// Candidate-loop errors keep budget, stale-attempt and unavailable-result failures explicit.
var (
	ErrInvalidCandidateLoop          = errors.New("invalid candidate loop request")
	ErrCandidateLoopConflict         = errors.New("candidate loop conflicts with a recorded run or attempt")
	ErrCandidateLoopNotFound         = errors.New("candidate loop not found")
	ErrCandidateLoopWaiting          = errors.New("candidate attempt is waiting for a formal delivery or reports")
	ErrCandidateLoopStopped          = errors.New("candidate loop stopped or abandoned")
	ErrCandidateBudgetHeld           = errors.New("unresolved candidate budget belongs to another run")
	ErrCandidateConditionNotMet      = errors.New("candidate loop has not met its completion condition")
	ErrCandidateAttemptsExhausted    = errors.New("candidate attempt budget is exhausted; human intervention required")
	ErrCandidateLoopCompleted        = errors.New("candidate loop already completed its original task")
	ErrInvalidCandidateSatisfaction  = errors.New("invalid candidate satisfaction request")
	ErrCandidateSatisfactionConflict = errors.New("candidate satisfaction predecessor, attempt or actor conflict")
	ErrCandidateInterventionHeld     = errors.New("candidate contradiction requires named human intervention")
	ErrCandidateInterventionNotFound = errors.New("candidate intervention not found")
	ErrInvalidNodeOwnership          = errors.New("invalid node ownership request")
	ErrNodeOwnershipConflict         = errors.New("node ownership version, owner or receipt conflict")
	ErrNodeOwnershipPending          = errors.New("node has a pending human takeover")
	ErrNodeOwnershipHandoffRequired  = errors.New("node ownership handoff or stop evidence is incomplete")
	ErrNodeOwnershipNotFound         = errors.New("node ownership operation not found")
)

// ErrRunBindingNotFound is returned when a run binding does not exist.
var ErrRunBindingNotFound = errors.New("run binding not found")

// ErrDeliveryNotFound is returned when a delivery is absent from the scoped project.
var ErrDeliveryNotFound = errors.New("delivery not found")

// ErrRunBindingConflict is returned when a run cannot accept the requested thread identity.
var ErrRunBindingConflict = errors.New("run binding identity conflict")

// ErrInvalidRunBinding is returned when an explicit run identity is invalid.
var ErrInvalidRunBinding = errors.New("invalid run binding identity")

// --- Lifecycle errors ---

var (
	// ErrSpecNotDone is returned when a lifecycle operation requires done stage.
	ErrSpecNotDone = errors.New("spec must be in done stage")
	// ErrSpecIneligibleStage is returned when a spec's stage does not support the requested operation.
	ErrSpecIneligibleStage = errors.New("spec is not in an eligible stage for this operation")
	// ErrSpecIneligibleForDrift is returned when a spec cannot be drift-checked.
	ErrSpecIneligibleForDrift = errors.New("spec is not eligible for drift checking (must be done)")
	// ErrSpecTerminal is returned when a spec is superseded or abandoned.
	ErrSpecTerminal = errors.New("spec is in a terminal state (superseded or abandoned)")
	// ErrNewSpecNotFound is returned when a replacement spec does not exist.
	ErrNewSpecNotFound = errors.New("replacement spec not found")
	// ErrNewSpecTerminal is returned when a replacement spec is in a terminal state.
	ErrNewSpecTerminal = errors.New("replacement spec is in a terminal state")
	// ErrConcurrentModification is returned when an optimistic-lock version guard fails.
	ErrConcurrentModification = errors.New("concurrent modification detected — retry the operation")
	// ErrInternalGuardFailure signals an unexpected precondition violation.
	ErrInternalGuardFailure = errors.New("internal guard failure — unexpected precondition violation")
	// ErrInvalidReEntryStage is returned when the requested re-entry stage is disallowed.
	ErrInvalidReEntryStage = errors.New("re-entry stage is not allowed for this operation")
	// ErrSameSlugs is returned when old and new slugs are identical in a supersede operation.
	ErrSameSlugs = errors.New("old and new slugs must differ")
	// ErrEdgeNotFound is returned when no matching dependency edge exists.
	ErrEdgeNotFound = errors.New("no matching dependency edge found")
	// ErrSpecNotAmendable is returned when amend is attempted on a spec not in an eligible stage.
	ErrSpecNotAmendable = errors.New("spec must be in approved, in_progress, or review stage to amend")
	// ErrReEntryStageRequired is returned when amend is called without a re-entry stage.
	ErrReEntryStageRequired = errors.New("re_entry_stage is required for amend")
)

// --- Version errors ---

// ErrVersionNotFound is returned when a requested version does not exist.
var ErrVersionNotFound = errors.New("version not found")

// --- Decision errors ---

// ErrDecisionNotFound is returned when a decision does not exist.
var ErrDecisionNotFound = errors.New("decision not found")

// ErrDecisionAlreadyExists is returned when creating a decision with a slug that already exists.
var ErrDecisionAlreadyExists = errors.New("decision already exists")

// ErrSupersededByRequired is returned when status is superseded but superseded_by is not provided.
var ErrSupersededByRequired = errors.New("superseded_by is required when status is superseded")

// --- Constitution errors ---

// ErrConstitutionNotFound is returned when no constitution exists.
var ErrConstitutionNotFound = errors.New("constitution not found")

// --- Project errors ---

// ErrProjectNotFound is returned when no project exists with the given slug.
var ErrProjectNotFound = errors.New("project not found")

// --- Slice errors ---

var (
	// ErrSliceNotFound is returned when a slice lookup finds no matching node.
	ErrSliceNotFound = errors.New("slice not found")
	// ErrSliceWrongStatus is returned when a status transition is invalid.
	ErrSliceWrongStatus = errors.New("slice status precondition not met")
)

// --- Sync errors ---

var (
	// ErrSyncMappingNotFound is returned when a sync mapping does not exist.
	ErrSyncMappingNotFound = errors.New("sync mapping not found")
	// ErrSyncMappingExists is returned when a sync mapping already exists for the spec+adapter pair.
	ErrSyncMappingExists = errors.New("sync mapping already exists for this spec and adapter")
)

// --- Provenance ---

// ErrProvenanceMismatch is returned when provenance_type and provenance_detail are inconsistent.
var ErrProvenanceMismatch = errors.New("provenance_type does not match populated provenance_detail variant")

// ErrAuthoredRequiresSparkOnly is returned when an AUTHORED spec create includes stage outputs beyond spark.
var ErrAuthoredRequiresSparkOnly = errors.New("AUTHORED provenance: only spark_output may be set at create")

// ErrRetroactiveRequiresAllOutputs is returned when a RETROACTIVE_FROM_PR create is missing one or more funnel outputs.
var ErrRetroactiveRequiresAllOutputs = errors.New("RETROACTIVE_FROM_PR provenance: spark/shape/specify/decompose outputs all required")

// ErrRetroactiveRequiresPRRef is returned when a RETROACTIVE_FROM_PR create is missing url or sha.
var ErrRetroactiveRequiresPRRef = errors.New("RETROACTIVE_FROM_PR provenance: url and sha are required")

// ErrDeclaredRequiresAllOutputs is returned when a DECLARED create is missing one or more funnel outputs.
var ErrDeclaredRequiresAllOutputs = errors.New("DECLARED provenance: spark/shape/specify/decompose outputs all required")

// ErrDeclaredRequiresDeclaredBy is returned when a DECLARED create is missing declared_by.
var ErrDeclaredRequiresDeclaredBy = errors.New("DECLARED provenance: declared_by is required")

// ErrClaimRequiresAuthored is returned when claim is invoked on a non-AUTHORED spec.
var ErrClaimRequiresAuthored = errors.New("claim requires provenance_type = AUTHORED")

// ErrCompletionRequiresAuthored is returned when report-completion is invoked on a non-AUTHORED spec.
var ErrCompletionRequiresAuthored = errors.New("report-completion requires provenance_type = AUTHORED")

// --- Identity errors ---

// ErrUserNotFound is returned when a user does not exist.
var ErrUserNotFound = errors.New("user not found")

// ErrAPIKeyNotFound is returned when an API key does not exist.
var ErrAPIKeyNotFound = errors.New("api key not found")

// ErrOIDCBindingNotFound is returned when an OIDC binding does not exist.
var ErrOIDCBindingNotFound = errors.New("oidc binding not found")

// ErrBootstrapExists is returned when a bootstrap user already exists.
var ErrBootstrapExists = errors.New("bootstrap user already exists")

// ErrAPIKeyPrefixExists is returned when an API key prefix collides with an existing key.
var ErrAPIKeyPrefixExists = errors.New("api key prefix collision")

// ErrQuotaExceeded is returned when a self-service key mint would exceed the
// caller's active-key quota. Enforced under a parent-row lock so the count is
// race-free (T-02-05). Handlers map this to CodeResourceExhausted.
var ErrQuotaExceeded = errors.New("api key quota exceeded")

// ErrSessionNotFound is returned when a web session does not exist (or is expired/revoked at lookup).
var ErrSessionNotFound = errors.New("web session not found")

// ErrLoginFlowNotFound is returned when an OIDC login-flow row does not exist or has expired.
var ErrLoginFlowNotFound = errors.New("oidc login flow not found")

// ErrCLICodeNotFound is returned when a CLI one-time login code does not exist
// or has expired.
var ErrCLICodeNotFound = errors.New("cli login code not found")

// ErrCLIChallengeMismatch is returned when the PKCE verifier presented at the
// CLI exchange does not match the challenge stored with the code.
var ErrCLIChallengeMismatch = errors.New("cli login challenge mismatch")
