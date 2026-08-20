// Checks tool inputs before they cross the runtime boundary.
package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
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
	result := execTool(context.Background(), peer, callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
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
	result = execTool(context.Background(), peer, callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
		ID: "write-1", Name: "write", Args: map[string]any{"path": "new.txt", "content": content},
	})
	write := <-writes
	if write.Turn != "run-a" || write.Request != "request-a" || write.TaskID != "t_1234abcd" ||
		write.Path != "new.txt" || write.Content == nil || *write.Content != content {
		t.Fatalf("write request = %#v", write)
	}
	if ok, _ := result.Data["ok"].(bool); !ok {
		t.Fatalf("write result = %#v", result)
	}
	replacement := "new"
	result = execTool(context.Background(), peer, callID{Turn: "run-a", Request: "request-a"}, "t_1234abcd", model.Call{
		ID: "edit-1", Name: "edit", Args: map[string]any{
			"path": "new.txt", "target": "old", "replacement": replacement,
		},
	})
	edit := <-edits
	if edit.Turn != "run-a" || edit.Request != "request-a" || edit.Target != "old" ||
		edit.Replacement == nil || *edit.Replacement != replacement {
		t.Fatalf("edit request = %#v", edit)
	}
	if ok, _ := result.Data["ok"].(bool); !ok {
		t.Fatalf("edit result = %#v", result)
	}
}
