// Checks tool inputs before they cross the runtime boundary.
package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
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
	if !strings.Contains(result.Data["error"].(string), "context canceled") {
		t.Fatalf("cancelled result = %#v", result)
	}
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
