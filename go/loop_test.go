// Checks ordered prompt assembly and correlated stream control.
package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"pippo/go/model"
)

func TestAssembleUsesFixedBlockOrder(t *testing.T) {
	request := assemble("test-model", prompt{
		SystemPrompt:      "system\r\nline",
		ToolDeclarations:  "tools",
		StaticEnvironment: "static",
		SkillsIndex:       "skills",
		Summary:           "summary",
		GlobalPreferences: "preferences",
		Transcript:        "transcript",
		Query:             "query",
		LiveEnvironment:   "live",
	})
	want := []model.Block{
		{Kind: model.SystemPrompt, Text: "system\nline"},
		{Kind: model.ToolDeclarations, Text: "tools"},
		{Kind: model.StaticEnvironment, Text: "static"},
		{Kind: model.SkillsIndex, Text: "skills"},
		{Kind: model.Summary, Text: "summary"},
		{Kind: model.GlobalPreferences, Text: "preferences"},
		{Kind: model.Transcript, Text: "transcript"},
		{Kind: model.Query, Text: "query"},
		{Kind: model.LiveEnvironment, Text: "live"},
	}
	if request.Model != "test-model" || !reflect.DeepEqual(request.Blocks, want) {
		t.Fatalf("request = %#v", request)
	}

	omitted := assemble("test-model", prompt{SystemPrompt: "system", SkillsIndex: " \t", Query: "query"})
	want = []model.Block{
		{Kind: model.SystemPrompt, Text: "system"},
		{Kind: model.Query, Text: "query"},
	}
	if !reflect.DeepEqual(omitted.Blocks, want) {
		t.Fatalf("blocks with empty input = %#v", omitted.Blocks)
	}
}

func TestStreamIsCorrelatedAndCancellable(t *testing.T) {
	provider := &blockingProvider{started: make(chan model.Request, 1)}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()

	chunks := make(chan chunk, 1)
	closedEvents := make(chan closed, 1)
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.ping": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]bool{"ready": true}, nil
		},
		"runtime.model_key": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]string{"value": "temporary-test-key"}, nil
		},
		"turn.chunk": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var value chunk
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			chunks <- value
			return nil, nil
		},
		"turn.closed": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var value closed
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			closedEvents <- value
			return nil, nil
		},
	})
	defer client.close()

	settings := json.RawMessage(`{
		"max_parallel_runs":4,
		"max_background_jobs":4,
		"max_steps":{"orchestrator":300},
		"max_depth":3
	}`)
	var ready struct {
		Ready bool `json:"ready"`
	}
	if err := client.call(context.Background(), "hello", hello{
		Paths:    paths{Runtime: "/runtime", Cache: "/cache", Agent: "/agent"},
		Platform: platform{OS: "test", Arch: "test"},
		Settings: settings,
	}, &ready); err != nil {
		t.Fatal(err)
	}
	id := callID{Turn: "turn-a", Request: "request-b"}
	var start accepted
	if err := client.call(context.Background(), "turn.start", startRequest{
		callID: id,
		Query:  "hello",
	}, &start); err != nil {
		t.Fatal(err)
	}
	if !start.Accepted || start.callID != id {
		t.Fatalf("accepted = %#v", start)
	}
	request := receive(t, provider.started)
	if request.Model != defaultModel {
		t.Fatalf("model = %q", request.Model)
	}
	if got := receive(t, chunks); got.callID != id || got.Text != "first" {
		t.Fatalf("chunk = %#v", got)
	}
	var cancel struct {
		Cancelled bool `json:"cancelled"`
	}
	if err := client.call(context.Background(), "turn.cancel", id, &cancel); err != nil {
		t.Fatal(err)
	}
	if !cancel.Cancelled {
		t.Fatal("active stream was not cancelled")
	}
	if got := receive(t, closedEvents); got.callID != id || got.Status != "cancelled" || got.Error != "" {
		t.Fatalf("closed = %#v", got)
	}
	provider.mu.Lock()
	keyOK := provider.keyOK
	provider.mu.Unlock()
	if !keyOK {
		t.Fatal("provider did not receive the request-scoped key")
	}
}

type blockingProvider struct {
	started chan model.Request
	mu      sync.Mutex
	keyOK   bool
}

func (p *blockingProvider) Stream(
	ctx context.Context,
	key string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	p.mu.Lock()
	p.keyOK = key == "temporary-test-key"
	p.mu.Unlock()
	p.started <- request
	if err := yield(model.Chunk{Text: "first"}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		var zero T
		t.Fatal("timed out waiting for stream event")
		return zero
	}
}
