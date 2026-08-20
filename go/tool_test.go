// Checks tool inputs before they cross the runtime boundary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"pippo/go/model"
)

func TestClarifyRejectsAmbiguousOptions(t *testing.T) {
	tests := []clarifyArgs{
		{Question: "Two\nquestions?"},
		{Question: "Choose?", Options: []clarifyOption{{Label: "Same"}, {Label: "Same"}}},
		{Question: "Choose?", Options: []clarifyOption{
			{Label: "First", Recommended: true}, {Label: "Second", Recommended: true},
		}},
	}
	for _, input := range tests {
		if checkClarify(input) == nil {
			t.Fatalf("accepted invalid clarification: %#v", input)
		}
	}
}

func TestClarifyAllowsFreeformWithoutOptions(t *testing.T) {
	if err := checkClarify(clarifyArgs{Question: "What should change?"}); err != nil {
		t.Fatal(err)
	}
}

func TestFindValidatesSearchAndReadShapes(t *testing.T) {
	zero := 0
	for _, input := range []findArgs{
		{},
		{Query: "needle", Path: "file.txt"},
		{Query: "needle", In: "somewhere"},
		{Query: "needle", Cap: &zero},
		{Path: "file.txt", Root: "."},
		{Path: "file.txt", Range: &findRange{Start: 3, End: 2}},
	} {
		if checkFind(input) == nil {
			t.Fatalf("accepted invalid find arguments: %#v", input)
		}
	}
	if err := checkFind(findArgs{Query: "needle", In: "both"}); err != nil {
		t.Fatal(err)
	}
	if err := checkFind(findArgs{Path: "file.txt", Range: &findRange{Start: 2, End: 4}}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteAndEditRequireFlatCompleteArguments(t *testing.T) {
	empty := ""
	content := "new text"
	if checkWrite(writeArgs{Path: "file.txt", Content: &empty}) != nil {
		t.Fatal("write rejected empty content")
	}
	if checkWrite(writeArgs{Path: "file.txt"}) == nil || checkWrite(writeArgs{Content: &content}) == nil {
		t.Fatal("write accepted incomplete arguments")
	}
	if checkEdit(editArgs{Path: "file.txt", Target: "old", Replacement: &empty}) != nil {
		t.Fatal("edit rejected an empty replacement")
	}
	if checkEdit(editArgs{Path: "file.txt", Replacement: &content}) == nil ||
		checkEdit(editArgs{Target: "old", Replacement: &content}) == nil {
		t.Fatal("edit accepted incomplete arguments")
	}
}

func TestShellValidatesCommandTimeoutCwdAndEnvironment(t *testing.T) {
	zero, tooLong, blank := 0, 381, " "
	for _, input := range []shellArgs{
		{},
		{Command: "true", Timeout: &zero},
		{Command: "true", Timeout: &tooLong},
		{Command: "true", Cwd: &blank},
		{Command: "true", Env: map[string]string{"BAD=NAME": "value"}},
	} {
		if checkShell(input) == nil {
			t.Fatalf("accepted invalid shell arguments: %#v", input)
		}
	}
	timeout, cwd := 30, "subdir"
	if err := checkShell(shellArgs{
		Command: "printf '%s' \"$VALUE\"", Timeout: &timeout, Cwd: &cwd,
		Env: map[string]string{"VALUE": "ok"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestShellDispatchesToRuntimeWithoutJoiningOrchestratorTools(t *testing.T) {
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	requests := make(chan shellRequest, 1)
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.shell": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input shellRequest
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			requests <- input
			return map[string]any{
				"ok": true, "kind": "shell", "stdout": "done", "stderr": "", "exit_code": 0,
			}, nil
		},
	})
	defer client.close()
	peer := state.connection()
	if peer == nil {
		t.Fatal("runtime connection was not attached")
	}
	timeout, cwd := 12, "sub"
	result := execTool(context.Background(), peer, nil, "worker",
		callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
			ID: "shell-1", Name: "shell", Args: map[string]any{
				"command": "printf done", "timeout": timeout, "cwd": cwd,
				"env": map[string]any{"MODE": "test"},
			},
		})
	request := <-requests
	if request.Turn != "run-a" || request.Request != "request-a" || request.CallID != "shell-1" ||
		request.TaskID != "t_1234abcd" || request.Role != "worker" || request.Command != "printf done" ||
		request.Timeout == nil || *request.Timeout != timeout || request.Cwd == nil || *request.Cwd != cwd ||
		request.Env["MODE"] != "test" {
		t.Fatalf("runtime request = %#v", request)
	}
	if ok, _ := result.Data["ok"].(bool); !ok || result.Data["stdout"] != "done" {
		t.Fatalf("tool result = %#v", result)
	}
	result = execTool(context.Background(), peer, nil, "explorer",
		callID{Turn: "run-read", Request: "request-read"}, "t_1234abcd", model.Call{
			ID: "shell-read", Name: "shell", Args: map[string]any{"command": "git status"},
		})
	if request = <-requests; request.Role != "explorer" || request.Command != "git status" {
		t.Fatalf("explorer runtime identity = %#v", request)
	}
	if ok, _ := result.Data["ok"].(bool); !ok {
		t.Fatalf("explorer result = %#v", result)
	}
	var spoofed shellArgs
	if decodeArgs(map[string]any{"command": "pwd", "role": "worker"}, &spoofed) == nil {
		t.Fatal("model supplied a runtime role")
	}
	tools, err := declarations([]model.Tool{taskTool, subagentTool, clarifyTool})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(tools, `"Name":"shell"`) {
		t.Fatal("shell leaked into the orchestrator tool set")
	}
	if !strings.Contains(tools, `"Name":"subagent"`) {
		t.Fatal("subagent was omitted from the orchestrator tool set")
	}
}

func TestShellCancellationNotifiesTheRuntime(t *testing.T) {
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	cancelled := make(chan shellCancel, 1)
	approvalCancelled := make(chan shellCancel, 1)
	var once sync.Once
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.shell": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			once.Do(func() { close(started) })
			<-release
			return map[string]any{"ok": false}, nil
		},
		"runtime.shell.cancel": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input shellCancel
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			cancelled <- input
			close(release)
			return map[string]bool{"cancelled": true}, nil
		},
		"runtime.approval.cancel": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input shellCancel
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			approvalCancelled <- input
			return map[string]bool{"cancelled": true}, nil
		},
	})
	defer client.close()
	peer := state.connection()
	if peer == nil {
		t.Fatal("runtime connection was not attached")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan model.Result, 1)
	go func() {
		done <- execTool(ctx, peer, nil, "worker", callID{Turn: "run-cancel", Request: "request-cancel"}, "task-a", model.Call{
			ID: "shell-cancel", Name: "shell", Args: map[string]any{"command": "sleep 30"},
		})
	}()
	<-started
	cancel()
	result := <-done
	input := <-cancelled
	approval := <-approvalCancelled
	if input.Turn != "run-cancel" || input.Request != "request-cancel" || input.CallID != "shell-cancel" {
		t.Fatalf("shell cancellation = %#v", input)
	}
	if approval != input {
		t.Fatalf("approval cancellation = %#v", approval)
	}
	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("cancelled result = %#v", result)
	}
}

func failureReason(result model.Result) string {
	failure, _ := result.Data["error"].(map[string]any)
	reason, _ := failure["reason"].(string)
	return reason
}

func TestFindDispatchesToRuntimeWithoutJoiningOrchestratorTools(t *testing.T) {
	if findTool.Name != "find" {
		t.Fatalf("find declaration = %#v", findTool)
	}
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	requests := make(chan findRequest, 1)
	writes := make(chan writeRequest, 1)
	edits := make(chan editRequest, 1)
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.find": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input findRequest
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			requests <- input
			return map[string]any{
				"ok": true, "kind": "search",
				"hits": []any{map[string]any{"path": "src/main.rs", "line": 7}},
			}, nil
		},
		"runtime.write": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input writeRequest
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			writes <- input
			return map[string]any{"ok": true, "kind": "write", "created": true}, nil
		},
		"runtime.edit": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input editRequest
			if err := json.Unmarshal(raw, &input); err != nil {
				return nil, err
			}
			edits <- input
			return map[string]any{"ok": true, "kind": "edit", "replacements": 1}, nil
		},
	})
	defer client.close()
	peer := state.connection()
	if peer == nil {
		t.Fatal("runtime connection was not attached")
	}
	result := execTool(context.Background(), peer, nil, "worker", callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
		ID: "find-1", Name: "find", Args: map[string]any{"query": "needle", "in": "content"},
	})
	request := <-requests
	if request.Turn != "run-a" || request.Request != "request-a" || request.TaskID != "t_1234abcd" ||
		request.Query != "needle" || request.In != "content" {
		t.Fatalf("runtime request = %#v", request)
	}
	if ok, _ := result.Data["ok"].(bool); !ok || result.Name != "find" {
		t.Fatalf("tool result = %#v", result)
	}
	content := "new\n"
	result = execTool(context.Background(), peer, nil, "worker", callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
		ID: "write-1", Name: "write", Args: map[string]any{"path": "new.txt", "content": content},
	})
	write := <-writes
	if write.Turn != "run-a" || write.Request != "request-a" || write.CallID != "write-1" ||
		write.TaskID != "t_1234abcd" ||
		write.Path != "new.txt" || write.Content == nil || *write.Content != content {
		t.Fatalf("write request = %#v", write)
	}
	if ok, _ := result.Data["ok"].(bool); !ok {
		t.Fatalf("write result = %#v", result)
	}
	replacement := "new"
	result = execTool(context.Background(), peer, nil, "worker", callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
		ID: "edit-1", Name: "edit", Args: map[string]any{
			"path": "new.txt", "target": "old", "replacement": replacement,
		},
	})
	edit := <-edits
	if edit.Turn != "run-a" || edit.Request != "request-a" || edit.CallID != "edit-1" || edit.Target != "old" ||
		edit.Replacement == nil || *edit.Replacement != replacement {
		t.Fatalf("edit request = %#v", edit)
	}
	if ok, _ := result.Data["ok"].(bool); !ok {
		t.Fatalf("edit result = %#v", result)
	}
}

func TestEveryToolValidationFailureUsesOneTypedShape(t *testing.T) {
	tests := []struct {
		role, reason string
		call         model.Call
	}{
		{orchestratorRole, "bad_args", model.Call{ID: "task", Name: "task", Args: map[string]any{}}},
		{orchestratorRole, "busy", model.Call{ID: "subagent", Name: "subagent", Args: map[string]any{
			"action": "spawn", "role": "explorer", "title": "find project", "request": "Find it.",
		}}},
		{orchestratorRole, "bad_args", model.Call{ID: "clarify", Name: "clarify", Args: map[string]any{
			"question": "two\nquestions",
		}}},
		{workerRole, "bad_args", model.Call{ID: "find", Name: "find", Args: map[string]any{}}},
		{workerRole, "bad_args", model.Call{ID: "write", Name: "write", Args: map[string]any{}}},
		{workerRole, "bad_args", model.Call{ID: "edit", Name: "edit", Args: map[string]any{}}},
		{workerRole, "bad_args", model.Call{ID: "shell", Name: "shell", Args: map[string]any{}}},
		{plannerRole, "bad_args", model.Call{ID: "plan", Name: "plan", Args: map[string]any{
			"action": "create", "task_id": "task", "goal": "goal", "steps": []any{map[string]any{
				"title": "one", "detail": "detail", "files": []any{"a"}, "verify": "test", "risk": "none",
			}},
		}}},
	}
	for _, test := range tests {
		result := execTool(t.Context(), nil, nil, test.role, callID{}, "", test.call)
		failure, ok := result.Data["error"].(map[string]any)
		if result.Err != nil || result.Data["ok"] != false || !ok || failureReason(result) != test.reason ||
			strings.TrimSpace(failure["message"].(string)) == "" {
			t.Fatalf("%s result = %#v", test.call.Name, result)
		}
	}
	for message, reason := range map[string]string{
		"path outside scope": "outside_scope", "approval denied": "denied",
		"run not found": "not_found", "command timed out": "timeout",
		"parallel limit reached": "limit", "run is active": "busy", "invalid shape": "bad_args",
	} {
		if got := toolReason(message); got != reason {
			t.Fatalf("reason for %q = %q", message, got)
		}
	}
}

func TestFindRetriesOneReadButSideEffectsRunAtMostOnce(t *testing.T) {
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	var finds, tasks, writes, shells atomic.Int32
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.find": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			if finds.Add(1) == 1 {
				return map[string]any{"ok": false, "error": map[string]any{
					"reason": "busy", "message": "temporary read failure",
				}}, nil
			}
			return map[string]any{"ok": true, "kind": "read", "lines": []any{}}, nil
		},
		"runtime.task": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			tasks.Add(1)
			return nil, errors.New("task t_missing is not registered")
		},
		"runtime.write": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			writes.Add(1)
			return nil, errors.New("approval denied")
		},
		"runtime.shell": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			shells.Add(1)
			return nil, errors.New("command timed out")
		},
	})
	defer client.close()
	peer := state.connection()
	read := execTool(t.Context(), peer, nil, workerRole, callID{}, "task", model.Call{
		ID: "read", Name: "find", Args: map[string]any{"path": "file.txt", "range": map[string]any{"start": 1, "end": 1}},
	})
	if read.Err != nil || read.Data["ok"] != true || read.Data["attempts"] != float64(2) && read.Data["attempts"] != 2 || finds.Load() != 2 {
		t.Fatalf("read retry = %#v, calls=%d", read, finds.Load())
	}
	requests := []struct {
		role, reason string
		call         model.Call
	}{
		{orchestratorRole, "not_found", model.Call{ID: "task", Name: "task", Args: map[string]any{
			"action": "update", "id": "t_missing", "status": "failed", "note": "missing",
		}}},
		{workerRole, "denied", model.Call{ID: "write", Name: "write", Args: map[string]any{
			"path": "file", "content": "value",
		}}},
		{workerRole, "timeout", model.Call{ID: "shell", Name: "shell", Args: map[string]any{"command": "sleep 2"}}},
	}
	for _, request := range requests {
		result := execTool(t.Context(), peer, nil, request.role, callID{}, "task", request.call)
		if result.Err != nil || failureReason(result) != request.reason {
			t.Fatalf("%s failure = %#v", request.call.Name, result)
		}
	}
	if tasks.Load() != 1 || writes.Load() != 1 || shells.Load() != 1 {
		t.Fatalf("side-effect calls task=%d write=%d shell=%d", tasks.Load(), writes.Load(), shells.Load())
	}
	client.close()
	transport := execTool(t.Context(), peer, nil, workerRole, callID{}, "task", model.Call{
		ID: "transport", Name: "find", Args: map[string]any{"query": "x"},
	})
	if transport.Err == nil {
		t.Fatalf("transport failure was relabeled: %#v", transport)
	}
}

type recoverProvider struct {
	round int
	err   error
}

func (p *recoverProvider) Stream(_ context.Context, _ string, request model.Request, yield func(model.Chunk) error) error {
	p.round++
	if p.round == 1 {
		call := model.Call{ID: "duplicate", Name: "task", Args: map[string]any{
			"action": "create", "title": "create typed task", "path": "/tmp/project",
		}}
		if err := yield(model.Chunk{Call: &call}); err != nil {
			return err
		}
		return yield(model.Chunk{Call: &call})
	}
	if len(request.History) != 2 || len(request.History[1].Results) != 1 ||
		failureReason(request.History[1].Results[0]) != "busy" {
		p.err = errors.New("typed failure did not continue as one result")
		return p.err
	}
	return yield(model.Chunk{Text: "recovered"})
}

func TestLoopContinuesAfterTypedFailureAndDeduplicatesStreamedCall(t *testing.T) {
	provider := &recoverProvider{}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	var tasks atomic.Int32
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.live_env": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return liveState{Date: "2026-08-20"}, nil
		},
		"runtime.task": func(context.Context, *rpc, json.RawMessage) (any, error) {
			tasks.Add(1)
			return nil, errors.New("task t_active is still active")
		},
		"turn.chunk": func(context.Context, *rpc, json.RawMessage) (any, error) { return nil, nil },
	})
	defer client.close()
	current := roleDefaults(limits{})[orchestratorRole]
	request := model.Request{Model: current.Model, Tools: current.Tools}
	err = state.loop.run(t.Context(), state.connection(), "key", &request, callID{Turn: "turn", Request: "request"}, current)
	if err != nil || provider.err != nil || provider.round != 2 || tasks.Load() != 1 {
		t.Fatalf("loop err=%v provider=%v rounds=%d calls=%d", err, provider.err, provider.round, tasks.Load())
	}
}

func TestLostSideEffectResponseDoesNotRetry(t *testing.T) {
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	var edits atomic.Int32
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.edit": func(_ context.Context, peer *rpc, _ json.RawMessage) (any, error) {
			edits.Add(1)
			peer.close()
			return map[string]any{"ok": true}, nil
		},
	})
	defer client.close()
	result := execTool(t.Context(), state.connection(), nil, workerRole, callID{}, "task", model.Call{
		ID: "edit", Name: "edit", Args: map[string]any{
			"path": "file", "target": "old", "replacement": "new",
		},
	})
	if result.Err == nil || edits.Load() != 1 {
		t.Fatalf("lost response result=%#v calls=%d", result, edits.Load())
	}
}
