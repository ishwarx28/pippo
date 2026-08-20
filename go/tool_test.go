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
	})
	defer client.close()
	peer := state.connection()
	if peer == nil {
		t.Fatal("runtime connection was not attached")
	}
	result := execTool(context.Background(), peer, callID{}, "t_1234abcd", model.Call{
		ID: "find-1", Name: "find", Args: map[string]any{"query": "needle", "in": "content"},
	})
	request := <-requests
	if request.TaskID != "t_1234abcd" || request.Query != "needle" || request.In != "content" {
		t.Fatalf("runtime request = %#v", request)
	}
	if ok, _ := result.Data["ok"].(bool); !ok || result.Name != "find" {
		t.Fatalf("tool result = %#v", result)
	}
}
