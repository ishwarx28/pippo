// Exercises race-safe subagent control over the runtime boundary.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"pippo/go/model"
)

type runAttempt struct {
	request model.Request
	release chan string
}

type controlledProvider struct {
	started chan runAttempt
	partial string
}

func (p *controlledProvider) Stream(
	ctx context.Context,
	_ string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	attempt := runAttempt{request: request, release: make(chan string, 1)}
	select {
	case p.started <- attempt:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case report := <-attempt.release:
		return yield(model.Chunk{Text: report})
	case <-ctx.Done():
		if p.partial != "" {
			if err := yield(model.Chunk{Text: p.partial}); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
}

type runtimeRuns struct {
	mu      sync.Mutex
	next    int
	entries map[string]runMeta
	creates []runCreate
	reports map[string][]string
	refs    []runReport
}

func (r *runtimeRuns) create(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
	var input runCreate
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	id := fmt.Sprintf("r_%08x", r.next)
	meta := runMeta{
		ID: id, ParentID: input.ParentID, TaskID: input.TaskID, ProjectID: "project_123abc",
		Role: input.Role, Title: input.Title, Request: input.Request, Status: runRunning,
		Attempt: 1, Constraints: input.Constraints, Media: input.Media,
		Related: input.Related, Highlight: input.Highlight,
		Reports: append([]runReport(nil), r.refs...),
	}
	r.entries[id] = meta
	r.creates = append(r.creates, input)
	return meta, nil
}

func (r *runtimeRuns) update(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
	var input runUpdate
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.entries[input.ID]
	if !ok {
		return nil, fmt.Errorf("unknown run %s", input.ID)
	}
	meta.Status, meta.Attempt = input.Status, input.Attempt
	if input.Report != "" {
		name := "inspect_current_code.md"
		if input.Attempt > 1 {
			name = fmt.Sprintf("inspect_current_code_(%d).md", input.Attempt)
		}
		meta.ReportPath = "projects/project_123abc/reports/t_1234abcd/" + name
		r.reports[input.ID] = append(r.reports[input.ID], input.Report)
	}
	r.entries[input.ID] = meta
	return meta, nil
}

func runHarness(t *testing.T, provider model.Provider) (*runSet, *rpc, *runtimeRuns) {
	t.Helper()
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	runtime := &runtimeRuns{entries: make(map[string]runMeta), reports: make(map[string][]string)}
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.model_key": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]string{"value": "test-key"}, nil
		},
		"runtime.run": func(ctx context.Context, peer *rpc, raw json.RawMessage) (any, error) {
			var action struct {
				Action string `json:"action"`
			}
			if err := json.Unmarshal(raw, &action); err != nil {
				return nil, err
			}
			if action.Action == "create" {
				return runtime.create(ctx, peer, raw)
			}
			return runtime.update(ctx, peer, raw)
		},
	})
	peer := state.connection()
	if peer == nil {
		t.Fatal("runtime connection was not attached")
	}
	runs := newRunSet(provider)
	t.Cleanup(func() {
		runs.shutdown()
		client.close()
		server.Close()
	})
	return runs, peer, runtime
}

func spawnArgs(role string) subagentArgs {
	return subagentArgs{
		Action: "spawn", Role: role, TaskID: "t_1234abcd", Title: "inspect current code",
		Request: "Inspect the requested behavior.", Constraints: []string{"be precise"},
	}
}

func TestSubagentPauseResumeWaitAndReportVersions(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 8), partial: "partial analysis"}
	runs, peer, runtime := runHarness(t, provider)
	created, err := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("worker"))
	if err != nil {
		t.Fatal(err)
	}
	id := created.(runOutput).ID
	first := receive(t, provider.started)
	paused, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{Action: "pause", ID: id})
	if err != nil || paused.(runOutput).Status != runPaused {
		t.Fatalf("pause = %#v, %v", paused, err)
	}
	resumed, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{
		Action: "resume", ID: id, Amend: "Check the retry path.",
	})
	if err != nil || resumed.(runOutput).Status != runRunning {
		t.Fatalf("resume = %#v, %v", resumed, err)
	}
	second := receive(t, provider.started)
	if len(second.request.History) != 2 || second.request.History[0].Text != "partial analysis" ||
		second.request.History[1].Text != "Check the retry path." {
		t.Fatalf("paused continuation = %#v", second.request.History)
	}
	second.release <- "complete report"
	waited, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{Action: "wait", IDs: []string{id}})
	if err != nil {
		t.Fatal(err)
	}
	done := waited.(map[string]any)["runs"].([]runOutput)[0]
	if done.Status != runDone || !strings.Contains(done.Report, "complete report\n\nThis same report is written at projects/") {
		t.Fatalf("wait = %#v", done)
	}
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{
		Action: "resume", ID: id, Answers: []string{"Use exponential backoff."},
	}); err != nil {
		t.Fatal(err)
	}
	third := receive(t, provider.started)
	if len(third.request.History) != 4 || !strings.Contains(third.request.History[2].Text, "complete report") ||
		third.request.History[3].Text != "Use exponential backoff." {
		t.Fatalf("terminal continuation = %#v", third.request.History)
	}
	third.release <- "revised report"
	outputs, err := runs.wait(context.Background(), "", []string{id})
	if err != nil || !strings.Contains(outputs[0].Report, "revised report") {
		t.Fatalf("revised wait = %#v, %v", outputs, err)
	}
	runtime.mu.Lock()
	meta := runtime.entries[id]
	runtime.mu.Unlock()
	if meta.Attempt != 2 || meta.Status != runDone {
		t.Fatalf("durable metadata = %#v", meta)
	}
	if len(runtime.reports[id]) != 2 || runtime.reports[id][0] != "complete report" ||
		runtime.reports[id][1] != "revised report" {
		t.Fatalf("report versions = %#v", runtime.reports[id])
	}
	_ = first
}

func TestSubagentAssemblesPathsWithoutReportContents(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 2)}
	runs, peer, runtime := runHarness(t, provider)
	runtime.refs = []runReport{
		{Path: "projects/pippo_123abc/reports/t_old/old_(1).md", Central: true},
		{Path: "projects/pippo_123abc/reports/t_old/latest.md"},
	}
	args := spawnArgs("explorer")
	args.Request = "Trace the retry flow."
	args.Constraints = []string{"Read only", "Cite exact lines"}
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", args); err != nil {
		t.Fatal(err)
	}
	attempt := receive(t, provider.started)
	want := "<request>\nTrace the retry flow.\n</request>\n<constraints>\n" +
		"- Read only\n- Cite exact lines\n</constraints>\n<reports>\n" +
		"- [central] projects/pippo_123abc/reports/t_old/old_(1).md\n" +
		"- projects/pippo_123abc/reports/t_old/latest.md\n</reports>"
	if len(attempt.request.Blocks) != 1 || attempt.request.Blocks[0].Kind != model.Query ||
		attempt.request.Blocks[0].Text != want || strings.Contains(attempt.request.Blocks[0].Text, "report body") {
		t.Fatalf("assembled request = %#v", attempt.request.Blocks)
	}
	attempt.release <- "done"
}

func TestSubagentMediaSelectionIsolationAndResume(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 8)}
	runs, peer, _ := runHarness(t, provider)
	origin := callID{Turn: "turn-media", Request: "request-media"}
	media, err := prepareMedia([]mediaInput{
		{MIME: "image/png", Data: []byte{1, 2}},
		{MIME: "image/jpg", Data: []byte{3, 4}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runs.attach(origin, media)
	args := spawnArgs("planner")
	args.Media, args.origin = []int{2}, origin
	created, err := runs.act(context.Background(), peer, "orchestrator", "", "", args)
	if err != nil {
		t.Fatal(err)
	}
	id := created.(runOutput).ID
	first := receive(t, provider.started)
	if len(first.request.Media) != 1 || first.request.Media[0].Label != "attachment 2 · image/jpeg" ||
		!reflect.DeepEqual(first.request.Media[0].Data, []byte{3, 4}) ||
		strings.Contains(first.request.Media[0].Label, "/cache/") {
		t.Fatalf("selected media = %#v", first.request.Media)
	}
	child := spawnArgs("worker")
	child.Media = []int{1}
	if _, err := runs.act(context.Background(), peer, "planner", id, args.TaskID, child); err == nil {
		t.Fatal("planner accessed media it did not receive")
	}
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{
		Action: "pause", ID: id,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", subagentArgs{
		Action: "resume", ID: id,
	}); err != nil {
		t.Fatal(err)
	}
	second := receive(t, provider.started)
	if !reflect.DeepEqual(second.request.Blocks, first.request.Blocks) ||
		!reflect.DeepEqual(second.request.Media, first.request.Media) {
		t.Fatalf("resumed request changed: %#v", second.request)
	}
	second.release <- "done"
}

func TestMediaValidationRejectsUnsupportedOversizeAndUnavailable(t *testing.T) {
	if _, err := prepareMedia([]mediaInput{{MIME: "audio/mpeg", Data: []byte{1}}}); err == nil {
		t.Fatal("audio was accepted")
	}
	if _, err := prepareMedia([]mediaInput{{MIME: "image/png", Data: make([]byte, maxMediaBytes+1)}}); err == nil {
		t.Fatal("oversize media was accepted")
	}
	provider := &controlledProvider{started: make(chan runAttempt, 1)}
	runs, peer, runtime := runHarness(t, provider)
	args := spawnArgs("worker")
	args.Media = []int{1}
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", args); err == nil {
		t.Fatal("missing originating attachment was accepted")
	}
	if runtime.next != 0 {
		t.Fatal("invalid media created a durable run")
	}
}

func TestSubagentNestingLimitsOwnershipAndDescendantStop(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 8)}
	runs, peer, runtime := runHarness(t, provider)
	runs.configure(2, 2)
	parentAny, err := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("planner"))
	if err != nil {
		t.Fatal(err)
	}
	parent := parentAny.(runOutput).ID
	receive(t, provider.started)
	runs.configure(2, 1)
	if _, err := runs.act(context.Background(), peer, "planner", parent, "t_1234abcd", spawnArgs("explorer")); err == nil {
		t.Fatal("nesting limit was bypassed")
	}
	runs.configure(2, 2)
	childAny, err := runs.act(context.Background(), peer, "planner", parent, "t_1234abcd", spawnArgs("explorer"))
	if err != nil {
		t.Fatal(err)
	}
	child := childAny.(runOutput).ID
	receive(t, provider.started)
	if _, err := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("worker")); err == nil {
		t.Fatal("parallel limit was bypassed")
	}
	if _, err := runs.pause(context.Background(), "", child); err == nil {
		t.Fatal("orchestrator controlled a planner-owned child")
	}
	if _, err := runs.act(context.Background(), peer, "planner", child, "t_1234abcd", spawnArgs("worker")); err == nil {
		t.Fatal("non-planner child or depth limit spawned a descendant")
	}
	stopped, err := runs.stop(context.Background(), "", parent)
	if err != nil || stopped.Status != runStopped {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	children, err := runs.wait(context.Background(), parent, []string{child})
	if err != nil || children[0].Status != runStopped {
		t.Fatalf("child stop = %#v, %v", children, err)
	}
	runtime.mu.Lock()
	create := runtime.creates[1]
	runtime.mu.Unlock()
	if create.ParentID != parent || create.TaskID != "t_1234abcd" {
		t.Fatalf("nested runtime request = %#v", create)
	}
}

func TestSubagentMultiWaitOrderStopRaceAndShutdown(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 8)}
	runs, peer, _ := runHarness(t, provider)
	runs.configure(3, 3)
	firstAny, _ := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("worker"))
	first := firstAny.(runOutput).ID
	firstAttempt := receive(t, provider.started)
	secondAny, _ := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("explorer"))
	second := secondAny.(runOutput).ID
	secondAttempt := receive(t, provider.started)
	waited := make(chan []runOutput, 1)
	go func() {
		output, _ := runs.wait(context.Background(), "", []string{second, first})
		waited <- output
	}()
	firstAttempt.release <- "first"
	select {
	case <-waited:
		t.Fatal("multi-wait returned before every run")
	case <-time.After(20 * time.Millisecond):
	}
	secondAttempt.release <- "second"
	output := receive(t, waited)
	if output[0].ID != second || output[1].ID != first {
		t.Fatalf("wait order = %#v", output)
	}
	if _, err := runs.wait(context.Background(), "", []string{first, first}); err == nil {
		t.Fatal("duplicate wait ids were accepted")
	}
	thirdAny, _ := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("worker"))
	third := thirdAny.(runOutput).ID
	receive(t, provider.started)
	runs.shutdown()
	interrupted, err := runs.wait(context.Background(), "", []string{third})
	if err != nil || interrupted[0].Status != runInterrupted {
		t.Fatalf("shutdown wait = %#v, %v", interrupted, err)
	}
}

func TestSubagentToolExecutesARealManagedRun(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 2)}
	runs, peer, _ := runHarness(t, provider)
	result := make(chan model.Result, 1)
	go func() {
		result <- execTool(context.Background(), peer, runs, "orchestrator",
			callID{Turn: "turn-a", Request: "request-a"}, "", model.Call{
				ID: "subagent-a", Name: "subagent", Args: map[string]any{
					"action": "spawn", "role": "worker", "task_id": "t_1234abcd",
					"title": "perform focused work", "request": "Perform the requested work.", "wait": true,
				},
			})
	}()
	attempt := receive(t, provider.started)
	attempt.release <- "verified report"
	got := receive(t, result)
	output, ok := got.Data["output"].(runOutput)
	if !ok || output.Status != runDone || !strings.Contains(output.Report, "verified report") {
		t.Fatalf("tool result = %#v", got)
	}
}

func TestSubagentPauseStopRaceSettlesOnce(t *testing.T) {
	provider := &controlledProvider{started: make(chan runAttempt, 2)}
	runs, peer, _ := runHarness(t, provider)
	created, err := runs.act(context.Background(), peer, "orchestrator", "", "", spawnArgs("worker"))
	if err != nil {
		t.Fatal(err)
	}
	id := created.(runOutput).ID
	receive(t, provider.started)
	errs := make(chan error, 2)
	go func() {
		_, err := runs.pause(context.Background(), "", id)
		errs <- err
	}()
	go func() {
		_, err := runs.stop(context.Background(), "", id)
		errs <- err
	}()
	first, second := receive(t, errs), receive(t, errs)
	if first != nil && second != nil {
		t.Fatalf("pause and stop both failed: %v; %v", first, second)
	}
	output, err := runs.wait(context.Background(), "", []string{id})
	if err != nil || output[0].Status != runStopped {
		t.Fatalf("race outcome = %#v, %v", output, err)
	}
}

func TestSubagentRejectsInvalidActionShapes(t *testing.T) {
	for _, args := range []subagentArgs{
		{Action: "spawn", Role: "worker", ID: "r_12345678"},
		{Action: "wait"},
		{Action: "pause", IDs: []string{"r_12345678"}},
		{Action: "resume"},
		{Action: "unknown", ID: "r_12345678"},
	} {
		if checkSubagent(args) == nil {
			t.Fatalf("accepted invalid action shape: %#v", args)
		}
	}
	if err := checkSpawn("orchestrator", "", subagentArgs{
		Action: "spawn", Role: "explorer", Title: "find project", Request: "Find it.",
	}); err != nil {
		t.Fatal(err)
	}
	if err := checkSpawn("planner", "t_1234abcd", spawnArgs("planner")); err == nil {
		t.Fatal("planner spawned a planner")
	}
}
