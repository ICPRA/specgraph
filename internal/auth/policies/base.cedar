// SpecGraph base authorization policies.
// Explicitly authorized narrow Agent writes. The shared transaction still
// requires a host-bound manager or an assigned independent reviewer run.
permit (principal, action == SpecGraph::Action::"review-assignment.write", resource)
when { principal has role && principal.role == "reader" };
permit (principal, action == SpecGraph::Action::"review-decision.write", resource)
when { principal has role && principal.role == "reader" };

// User-authorized report-only write: storage requires the host-bound eligible run,
// its frozen plans and the matching delivery commit. No general writer grant.
permit (principal, action == SpecGraph::Action::"test-result.write", resource)
when { principal has role && principal.role == "reader" };

// User-authorized self completion retains the original guards and derives its run from host scope.
permit (principal, action == SpecGraph::Action::"run-completion.write", resource)
when { principal has role && principal.role == "reader" };

// User-authorized self submission is confined to the host-bound run and its saved workspace.
permit (principal, action == SpecGraph::Action::"delivery-submission.write", resource)
when { principal has role && principal.role == "reader" };

// Self retry/rework declarations are limited to the original bound run in storage.
// This narrow action does not grant reader keys the other execution.write RPCs.
permit (principal, action == SpecGraph::Action::"node-event.write", resource)
when { principal has role && principal.role == "reader" };

// A bound work Agent may judge only its own current candidate attempt.
permit (principal, action == SpecGraph::Action::"candidate-judgment.write", resource)
when { principal has role && principal.role == "reader" };

// A bound work Agent may record only its own exact progress handoff fact.
permit (principal, action == SpecGraph::Action::"node-handoff.write", resource)
when { principal has role && principal.role == "reader" };

// Planning checks the current host-bound manager in the business transaction.
permit (principal, action == SpecGraph::Action::"planning.write", resource)
when { principal has role && principal.role == "reader" };

// Native dispatch is separately scoped; it does not grant general workbench management.
// Explicitly user-authorized PM configuration, not dispatch or model execution.
// Like planning.write, this is only the transport gate: delivery_hook.go's
// arm/read/list/cancel all resolve PlanningManagerRun inside their transaction.
// A reader without the matching project's live manager run cannot configure hooks.
permit (principal, action == SpecGraph::Action::"hook-config.write", resource)
when { principal has role && principal.role == "reader" };

// Local host consumes only its authenticated user's fixed durable PM grant.
// Backend derives all target/actor/command identities and rechecks dispatch guards.
permit (principal, action == SpecGraph::Action::"hook-dispatch.write", resource)
when { principal has role && principal.role == "reader" };

// The local host may only consume its recorded human grant; storage checks the
// service identity, environment and native project. No PM command permission.
permit (principal, action == SpecGraph::Action::"program-execution.write", resource)
when { principal has role && principal.role == "reader" };

// Explicit summary decisions require the current project's manager in storage;
// this narrow transport action does not expand planning or workbench management.
permit (principal, action == SpecGraph::Action::"summary.write", resource)
when { principal has role && principal.role == "reader" };

permit (principal, action == SpecGraph::Action::"planning-dispatch.write", resource)
when { principal has role && principal.role == "reader" };

// Only the delegated mailbox transport admits reader API keys to this action;
// its active grant and exact provider binding remain mandatory. Forbid wins.
permit (
	principal,
	action == SpecGraph::Action::"mail.write",
	resource
) when {
	principal has role && principal.role == "reader"
};

//
// Migrated from the static rpcPermissions table. Roles gate VERBS via action
// groups: every concrete action (e.g. SpecGraph::Action::"spec.read") is a
// member of its verb group (SpecGraph::Action::"read") via the entity graph
// the engine builds at construction. New RPCs join a group by being added to
// internal/auth/actions.go — these policies do not change.
//
// Cedar semantics: default-deny; any matching permit allows; an explicit
// forbid (none here) would beat every permit. Discrete per-action and
// ownership policies for later stories layer on top via additional sources.

permit (
	principal,
	action in SpecGraph::Action::"read",
	resource
) when {
	principal has role &&
	(principal.role == "reader" || principal.role == "writer" || principal.role == "admin")
};

permit (
	principal,
	action in SpecGraph::Action::"write",
	resource
) when {
	principal has role &&
	(principal.role == "writer" || principal.role == "admin")
};

permit (
	principal,
	action in SpecGraph::Action::"delete",
	resource
) when {
	principal has role && principal.role == "admin"
};

// Identity-management operations (IdentityService): admin-only. The "manage"
// verb is reserved for sensitive operations that must NOT inherit the
// read/write/delete role gates (e.g. ListUsers is a read but must be admin).
permit (
	principal,
	action in SpecGraph::Action::"manage",
	resource
) when {
	principal has role && principal.role == "admin"
};

// Self-service operations (apikey.self): any AUTHENTICATED principal may act on
// their OWN API keys. Cedar only sees principal.role/id/email — it cannot see
// Source or the minted role floor — so this permit is deliberately broad and
// does NOT gate on principal.role == "admin". The handler further restricts it:
// it rejects Source == "apikey" (no key-mints-key laundering) and floors the
// minted role via auth.RoleMin. Landed in the same commit as the "self" verb in
// knownVerbs (engine.go) and the apikey.self action map (actions.go) so the
// engine boots consistently.
permit (
	principal,
	action in SpecGraph::Action::"self",
	resource
) when {
	principal has role
};
