// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Sean Brandt

//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/specgraph/specgraph/internal/auth"
	"github.com/specgraph/specgraph/internal/storage"
	"github.com/specgraph/specgraph/internal/storage/postgres"
	"github.com/specgraph/specgraph/internal/storage/postgres/postgrestest"
	"github.com/stretchr/testify/require"
)

func TestWorkbenchCompletionHookDesktopDiscoveryPostgres(t *testing.T) {
	binary, bridge := os.Getenv("VACPMS_STDIO_BINARY"), os.Getenv("VACPMS_STDIO_BRIDGE")
	if binary == "" || bridge == "" {
		t.Skip("set candidate binary and external desktop bridge paths")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("completion-desktop-discovery"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	users, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	human, err := users.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: "Discovery fixture", Role: "admin"}, nil)
	require.NoError(t, err)
	consumer, err := users.CreateServiceAccount(ctx, &storage.User{Kind: storage.KindServiceAccount, DisplayName: "Inert discovery host", Role: "reader", OwnerUserID: human.ID})
	require.NoError(t, err)
	ctx = auth.WithIdentity(ctx, &auth.Identity{UserID: human.ID, UserKind: storage.KindHuman})
	for _, slug := range []string{"source", "target", "empty"} {
		_, err := s.CreateSpec(ctx, slug, "Synthetic node for original hook discovery", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
		require.NoError(t, err)
		stage := "approved"
		_, err = s.UpdateSpec(ctx, slug, nil, &stage, nil, nil, nil)
		require.NoError(t, err)
	}
	host := &auth.Identity{UserID: consumer.ID, UserKind: storage.KindServiceAccount, Source: "apikey"}
	// Inert fixture metadata only; this test never authorizes or executes the program.
	run, err := s.PrepareProgramRun(ctx, &storage.PrepareProgramRunRequest{TaskSlug: "target", IdempotencyKey: "discovery-run", Command: storage.ProgramCommand{Executable: `C:\fixture\never-executed.exe`, Args: []string{"private literal"}, Cwd: `C:\fixture`, EnvironmentID: "fixture-env", NativeProjectID: "fixture-native", TimeoutMS: 10000, WorkPurpose: "coordination"}}, host)
	require.NoError(t, err)
	source, err := s.GetSpec(ctx, "source")
	require.NoError(t, err)
	hook, err := s.ArmHumanCompletionProgramHook(ctx, storage.ArmCompletionHookRequest{SourceTaskSlug: source.Slug, SourceSpecID: source.ID, TargetRunID: run.Context.RunID, TargetPackageID: run.Context.PackageID, IdempotencyKey: "discovery-hook", ConfirmTrigger: true}, host)
	require.NoError(t, err)
	config := filepath.Join(t.TempDir(), "reader.yaml")
	require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("server:\n  postgres:\n    url: %q\n", url)), 0600))
	command := exec.CommandContext(ctx, "node", "--input-type=module", "-e", `
import {pathToFileURL} from 'node:url';
import assert from 'node:assert/strict';
const {readWorkbench}=await import(pathToFileURL(process.argv[1]).href);
const input={executable:process.argv[2],config:process.argv[3],project:'completion-desktop-discovery',resource:'spec'};
const detail=await readWorkbench({...input,slug:'source'});
assert.equal(detail.completionHooks.length,1);
assert.deepEqual((await readWorkbench({...input,slug:'empty'})).completionHooks,[]);
console.log(JSON.stringify(detail.completionHooks));
`, bridge, binary, config)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	var hooks []postgres.WorkbenchCompletionHook
	require.NoError(t, json.Unmarshal(output, &hooks))
	require.Len(t, hooks, 1)
	require.Equal(t, hook.ID, hooks[0].ID)
	require.Equal(t, "armed", hooks[0].State)
	require.Equal(t, "source", hooks[0].SourceTaskSlug)
	require.Equal(t, "target", hooks[0].TargetTaskSlug)
	require.Equal(t, run.Context.RunID, hooks[0].TargetRunID)
	require.Equal(t, run.Context.PackageID, hooks[0].TargetPackageID)
	require.NotContains(t, string(output), "private literal")
	require.NotContains(t, string(output), "never-executed")
	unchanged, err := s.ReadHumanCompletionProgramHook(ctx, hook.ID)
	require.NoError(t, err)
	require.Nil(t, unchanged.Admission)
}

func TestWorkbenchStdioDesktopReadPostgres(t *testing.T) {
	binary, bridge := os.Getenv("VACPMS_STDIO_BINARY"), os.Getenv("VACPMS_STDIO_BRIDGE")
	if binary == "" || bridge == "" {
		t.Skip("set candidate binary and external desktop bridge paths")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	url, err := postgrestest.ConnString(ctx)
	require.NoError(t, err)
	s, err := postgres.New(ctx, url, postgres.WithProject("stdio-desktop-check"))
	require.NoError(t, err)
	defer s.Close(context.Background())
	_, err = s.CreateSpec(ctx, "node", "Read through the real desktop child bridge", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	config := filepath.Join(t.TempDir(), "reader.yaml")
	_, err = s.CreateSpec(ctx, "dispatch-node", "Synthetic IPC dispatch fixture", "p2", "low", storage.SpecProvenanceAuthored, storage.SpecProvenanceDetail{}, nil, nil, nil, nil)
	require.NoError(t, err)
	authStore, err := postgres.NewAuth(ctx, s.Pool())
	require.NoError(t, err)
	credentials := map[string]string{}
	for _, role := range []string{"admin", "reader"} {
		user, err := authStore.CreateHuman(ctx, &storage.User{Kind: storage.KindHuman, DisplayName: role, Role: role}, nil)
		require.NoError(t, err)
		secret, hash, err := auth.GenerateAPIKeySecret()
		require.NoError(t, err)
		key, err := authStore.CreateAPIKey(ctx, &storage.APIKey{UserID: user.ID, PHCHash: hash})
		require.NoError(t, err)
		credentials[role] = auth.FormatAPIKeyToken(key.Prefix, secret)
		if role == "reader" {
			credentials["readerSubject"] = "apikey:" + key.ID
		}
	}
	_, err = s.Pool().Exec(ctx, `INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES('stdio-desktop-check','stdio-mail-a','node'),('stdio-desktop-check','stdio-mail-b','node')`)
	require.NoError(t, err)
	mail, err := s.SendMail(ctx, "stdio-mail-a", &storage.SendMailRequest{TaskSlug: "node", Subject: "IPC collaboration", Body: "Actual stored message", RecipientRunIDs: []string{"stdio-mail-b"}, IdempotencyKey: "mail"})
	require.NoError(t, err)
	_, err = s.SendMail(ctx, "stdio-mail-a", &storage.SendMailRequest{TaskSlug: "node", Subject: "IPC collaboration", ThreadID: mail.Thread.ID, Body: "Reply with context", RecipientRunIDs: []string{"stdio-mail-b"}, IdempotencyKey: "reply", References: []storage.MailReferenceRequest{{MessageID: mail.Message.ID, Kind: "reply"}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("server:\n  postgres:\n    url: %q\n", url)), 0600))
	command := exec.CommandContext(ctx, "node", "--input-type=module", "-e", `
import {pathToFileURL} from 'node:url';
import fs from 'node:fs';
const credentials=JSON.parse(fs.readFileSync(0,'utf8'));
const {readWorkbench,commandWorkbench}=await import(pathToFileURL(process.argv[1]).href);
const input={executable:process.argv[2],config:process.argv[3],project:'stdio-desktop-check'};
const projects=await readWorkbench({...input,project:undefined,resource:'projects'});
const view=await readWorkbench({...input,resource:'current-view'});
const detail=await readWorkbench({...input,resource:'spec',slug:'node'});
const identity=await readWorkbench({...input,project:undefined,resource:'mail-identity',credential:credentials.admin});
let denied=false;
try { await readWorkbench({...input,resource:'mail-threads',credential:credentials.reader}); }
catch(error) { denied=error.message.startsWith('forbidden:'); }
if(!denied) throw new Error('Reader role must not inspect mail');
const mailList=await readWorkbench({...input,resource:'mail-threads',credential:credentials.admin});
const mail=await readWorkbench({...input,resource:'mail-thread',threadId:mailList.threads[0].id,credential:credentials.admin});
const createBody={slug:'created',intent:'Created through IPC',priority:'p2',complexity:'low'};
const deniedWrite=await commandWorkbench({...input,operation:'create-node',credential:credentials.reader,body:createBody});
if(deniedWrite.error?.code!=='forbidden') throw new Error('Reader must not create nodes');
const created=await commandWorkbench({...input,operation:'create-node',credential:credentials.admin,body:createBody});
if(created.error) throw new Error(created.error.code);
const baseline=await readWorkbench({...input,resource:'dependencies',slug:'created',credential:credentials.admin});
const dependency=await commandWorkbench({...input,operation:'add-dependency',credential:credentials.admin,slug:'created',body:{prerequisite:'node',expected_version:created.data.spec.version,expected_prerequisite_version:detail.spec.version,expected_revision:baseline.revision,reason:'Existing prerequisite',idempotency_key:'dependency'}});
if(dependency.error) throw new Error(dependency.error.code);
const removeBase=await readWorkbench({...input,resource:'dependencies',slug:'created',credential:credentials.admin});
const removed=await commandWorkbench({...input,operation:'remove-dependency',credential:credentials.admin,slug:'created',body:{prerequisite:'node',expected_version:removeBase.specVersion,expected_prerequisite_version:detail.spec.version,expected_revision:removeBase.revision,reason:'Fixture unlink',idempotency_key:'remove-dependency'}});
if(removed.error||removed.data?.operation!=='remove'||removed.data?.changed!==true)throw Error('Dependency was not removed');
const completion=await commandWorkbench({...input,operation:'manual-complete',credential:credentials.admin,slug:'node',body:{expected_version:detail.spec.version,idempotency_key:'completion',note:'Human completed the prerequisite'}});
if(completion.error) throw new Error(completion.error.code);
const completed=await readWorkbench({...input,resource:'spec',slug:'node'});
const approval=await commandWorkbench({...input,operation:'approve-node',slug:'dispatch-node',credential:credentials.admin,body:{expectedVersion:1,basis:'Reviewed synthetic fixture scope for dispatch'}});
if(approval.error||approval.data?.approved!=='dispatch-node'||approval.data?.version!==2)throw Error('Approval failed: '+JSON.stringify(approval.error));
const reviewed=await readWorkbench({...input,resource:'spec',slug:'dispatch-node'});
if(reviewed.spec.stage!=='approved'||!reviewed.changes.some(c=>c.reason==='Reviewed synthetic fixture scope for dispatch'))throw Error('Approval audit missing');
const target={version:1,promptFormatVersion:'vacpms-run-v2',environmentId:'fixture-env',projectId:'fixture-project',workspace:'C:/fixture',threadId:'fixture-thread',createCommandId:'fixture-create',startCommandId:'fixture-start',messageId:'fixture-message',createdAt:'2026-09-23T00:00:00Z',title:'Fixture',modelSelection:{instanceId:'fixture',model:'fixture'},runtimeMode:'approval-required',interactionMode:'default',branch:null,worktreePath:null,promptInjectedEffort:null,promptPrefix:''};
const preparation={task_slug:'dispatch-node',workspace:target.workspace,idempotency_key:'dispatch-fixture',dispatch_target:target};
const dispatch=await commandWorkbench({...input,operation:'prepare-run',credential:credentials.admin,body:preparation});
if(dispatch.error) throw Error('Prepare failed: '+dispatch.error.code);
const slug=dispatch.data.binding_id;
const runContext=await readWorkbench({...input,resource:'run-context',slug,credential:credentials.admin});
if(runContext.runId!==slug||runContext.taskSlug!=='dispatch-node')throw Error('Context identity mismatch');
const bound=await commandWorkbench({...input,operation:'bind-run',slug,credential:credentials.admin,body:{thread_ref:target.threadId,environment_id:target.environmentId}});
if(bound.error)throw Error('Bind failed');
const admitted=await commandWorkbench({...input,operation:'authorize-run',slug,credential:credentials.admin,body:{packageId:runContext.packageId,target}});
if(admitted.error)throw Error('Admission failed: '+admitted.error.code);
const state=await readWorkbench({...input,resource:'run-dispatch',slug,credential:credentials.admin});
if(!state.admission?.id)throw Error('Missing admission');
const abort=await commandWorkbench({...input,operation:'abort-preparation',credential:credentials.admin,body:{taskSlug:'dispatch-node',workspace:target.workspace,idempotencyKey:'dispatch-fixture',target,note:'Fixture abort'}});
if(abort.error?.code!=='failed_precondition'||abort.data?.runId!==slug)throw Error('Lost admitted run identity');
const resolved=await commandWorkbench({...input,operation:'resolve-run',slug,credential:credentials.admin,body:{admissionId:state.admission.id,kind:'stopped_writing',note:'Fixture never started a provider'}});
if(resolved.error)throw Error('Resolution failed');
console.log(JSON.stringify({projects,view,detail,identity,mail,created,dependency,completed}));
`, bridge, binary, config)
	input, err := json.Marshal(credentials)
	require.NoError(t, err)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	var result struct {
		Projects struct{ Projects []struct{ Slug string } }
		View     struct {
			Project           string
			ReadOnlyTransport bool
			Capabilities      map[string]bool
		}
		Detail struct {
			Spec struct {
				Slug, Intent, Stage, Role string
				Version                   int
			}
		}
		Identity struct{ Identity struct{ Role string } }
		Created  struct {
			Data struct{ Spec struct{ Slug, Stage string } }
		}
		Dependency struct{ Data map[string]any }
		Completed  struct{ Spec struct{ Stage string } }
		Mail       struct {
			Items []struct {
				Message struct {
					Body       string
					References []struct{ Body string }
				}
				Receipts []struct {
					ReadAt         *string `json:"read_at"`
					AcknowledgedAt *string `json:"acknowledged_at"`
				}
			}
		}
	}
	require.NoError(t, json.Unmarshal(output, &result))
	require.NotEmpty(t, result.Projects.Projects)
	require.Equal(t, "stdio-desktop-check", result.View.Project)
	require.True(t, result.View.ReadOnlyTransport)
	for name, available := range result.View.Capabilities {
		require.Equal(t, name == "mailInspection" || name == "manualCompletion" || name == "dependencyEditing" || name == "dependencyRemoval" || name == "createNode" || name == "nodeApproval" || name == "runDispatch" || name == "deliveryReview" || name == "deliveryHooks", available)
	}
	require.Equal(t, "node", result.Detail.Spec.Slug)
	require.Equal(t, "Read through the real desktop child bridge", result.Detail.Spec.Intent)
	require.Equal(t, "spark", result.Detail.Spec.Stage)
	require.Equal(t, "work", result.Detail.Spec.Role)
	require.Equal(t, 1, result.Detail.Spec.Version)
	require.Equal(t, "admin", result.Identity.Identity.Role)
	require.Equal(t, "created", result.Created.Data.Spec.Slug)
	require.Equal(t, "spark", result.Created.Data.Spec.Stage)
	require.NotEmpty(t, result.Dependency.Data)
	require.Equal(t, "done", result.Completed.Spec.Stage)
	require.Len(t, result.Mail.Items, 2)
	require.Equal(t, "Actual stored message", result.Mail.Items[1].Message.References[0].Body)
	for _, item := range result.Mail.Items {
		for _, receipt := range item.Receipts {
			require.Nil(t, receipt.ReadAt)
			require.Nil(t, receipt.AcknowledgedAt)
		}
	}
	var changed int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM mail_recipients WHERE project_slug='stdio-desktop-check' AND (read_at IS NOT NULL OR acknowledged_at IS NOT NULL)`).Scan(&changed))
	require.Zero(t, changed)
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM api_keys WHERE last_used_at IS NOT NULL`).Scan(&changed))
	require.Zero(t, changed)
	for _, name := range []string{"c", "d", "e"} {
		runID := "stdio-mail-" + name
		_, err = s.Pool().Exec(ctx, `INSERT INTO run_bindings(project_slug,id,task_spec_slug) VALUES('stdio-desktop-check',$1,'node')`, runID)
		require.NoError(t, err)
		require.NoError(t, s.BindRunThreadInEnvironment(ctx, runID, "env", name))
	}
	mailCommand := exec.CommandContext(ctx, "node", "--input-type=module", "-e", `
import {pathToFileURL} from 'node:url';
import fs from 'node:fs';
import assert from 'node:assert/strict';
const credentials=JSON.parse(fs.readFileSync(0,'utf8'));
const {requestMail}=await import(new URL('../workbench-mail/client.mjs',pathToFileURL(process.argv[1])).href);
const connection={executable:process.argv[2],config:process.argv[3],token:credentials.reader};
const invoke=(who,operation,payload)=>requestMail(connection,{environmentId:'env',threadId:who,providerSessionId:who,providerInstanceId:'provider'},operation,payload);
assert.deepEqual(await invoke('c','context',{}),{project:'stdio-desktop-check',task_slug:'node',run_id:'stdio-mail-c'});
const sent=await invoke('c','send',{subject:'Local mail',body:'Original context',recipient_run_ids:['stdio-mail-d','stdio-mail-e'],idempotency_key:'local-send'});
const forwarded=await invoke('d','send',{subject:'Forward',body:'For third party',recipient_run_ids:['stdio-mail-e'],idempotency_key:'local-forward',references:[{message_id:sent.message.id,kind:'forward'}]});
assert.equal(forwarded.message.references[0].body,'Original context');
const reply=await invoke('e','send',{subject:'Reply to third party',body:'Reply',recipient_run_ids:['stdio-mail-c'],idempotency_key:'local-reply',references:[{message_id:forwarded.message.id,kind:'reply'}]});
assert.equal(reply.message.references[0].body,'For third party');
assert((await invoke('e','inbox',{limit:10})).items.length>=2);
assert.equal((await invoke('d','thread',{mail_thread_id:sent.thread.id,limit:10})).items.length,1);
assert((await invoke('d','read',{message_id:sent.message.id})).read_at);
assert((await invoke('d','ack',{message_id:sent.message.id})).acknowledged_at);
assert((await invoke('c','close',{mail_thread_id:sent.thread.id,resolution:'Resolved'})).closed_at);
assert(Array.isArray((await requestMail(connection,{environmentId:'env',threadId:'c',providerSessionId:'c-resumed',providerInstanceId:'provider'},'inbox',{limit:10})).items));
await assert.rejects(invoke('not-enrolled','inbox',{limit:10}),/forbidden/);
const {setupMail}=await import(new URL('./setup-mail.mjs',pathToFileURL(process.argv[1])).href);
const path=await import('node:path');
const home=path.join(path.dirname(process.argv[3]),'mail-setup');
const setupInput={home,runtime:{executable:process.argv[2],config:process.argv[3]},credential:credentials.admin};
assert.deepEqual(await setupMail(setupInput),{configured:true,reused:false});
const originalToken=fs.readFileSync(path.join(home,'mail-secrets','mail.token'),'utf8');
assert.deepEqual(await setupMail({...setupInput,credential:''}),{configured:true,reused:true});
assert(originalToken===fs.readFileSync(path.join(home,'mail-secrets','mail.token'),'utf8'),'Setup must reuse its token');
const {execFileSync}=await import('node:child_process');
execFileSync(path.join(process.env.SystemRoot,'System32/WindowsPowerShell/v1.0/powershell.exe'),['-NoProfile','-NonInteractive','-Command',"$ErrorActionPreference='Stop'; $allowed=@([System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value,'S-1-5-18','S-1-5-32-544'); foreach($rule in ([System.IO.File]::GetAccessControl($env:CHECK_TOKEN_PATH)).GetAccessRules($true,$true,[System.Security.Principal.SecurityIdentifier])) { if($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Value -notin $allowed) { throw 'Unexpected credential access' } }"],{windowsHide:true,timeout:15000,env:{...process.env,CHECK_TOKEN_PATH:path.join(home,'mail-secrets','mail.token')}});
console.log('Local mail round trip passed');
`, bridge, binary, config)
	mailCommand.Stdin = strings.NewReader(string(input))
	mailOutput, err := mailCommand.CombinedOutput()
	require.NoError(t, err, string(mailOutput))
	require.Contains(t, string(mailOutput), "Local mail round trip passed")
	var accounts int
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT count(*) FROM users WHERE kind='service_account' AND display_name='VACPMS local mail'`).Scan(&accounts))
	require.Equal(t, 1, accounts)
	var grantID string
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT id FROM mail_grants WHERE project_slug='stdio-desktop-check' AND environment_id='env'`).Scan(&grantID))
	_, err = s.RevokeMailGrant(ctx, grantID, "fixture")
	require.NoError(t, err)
	require.ErrorIs(t, s.EnsureLocalMailBinding(ctx, credentials["readerSubject"], "fixture", storage.MailScope{EnvironmentID: "env", ThreadID: "c", ProviderSessionID: "c", ProviderInstanceID: "provider"}), storage.ErrMailForbidden)
}
