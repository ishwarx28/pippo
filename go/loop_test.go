// Checks ordered prompt assembly and correlated stream control.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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
		"runtime.steer": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string][]string{"messages": nil}, nil
		},
		"runtime.live_env": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return liveState{Date: "2026-08-20"}, nil
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
		Preset:   testPreset(),
	}, &ready); err != nil {
		t.Fatal(err)
	}
	id := callID{Turn: "turn-a", Request: "request-b"}
	var start accepted
	if err := client.call(context.Background(), "turn.start", startRequest{
		callID:      id,
		Query:       "hello",
		Attachments: []mediaInput{{MIME: "image/jpg", Data: []byte{7, 8}}},
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
	if len(request.Media) != 1 || request.Media[0].Label != "attachment 1 · image/jpeg" ||
		!reflect.DeepEqual(request.Media[0].Data, []byte{7, 8}) {
		t.Fatalf("turn media = %#v", request.Media)
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

func TestLoopStopCancelsActiveWorkAndRefusesMore(t *testing.T) {
	current := newLoop(&blockingProvider{started: make(chan model.Request, 1)})
	id := callID{Turn: "turn-stop", Request: "request-stop"}
	ctx, started := current.start(context.Background(), id)
	if !started {
		t.Fatal("work did not start")
	}
	finished := make(chan struct{})
	go func() {
		<-ctx.Done()
		current.finish(id)
		close(finished)
	}()
	current.stop()
	<-finished
	if _, started := current.start(context.Background(), callID{Turn: "late", Request: "late"}); started {
		t.Fatal("work started after shutdown")
	}
}

func TestTurnExecutesTaskThroughRuntimeAndContinues(t *testing.T) {
	provider := &taskProvider{}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	tasks := make(chan taskArgs, 1)
	chunks := make(chan chunk, 1)
	closedEvents := make(chan closed, 1)
	var liveCalls atomic.Int32
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.ping": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]bool{"ready": true}, nil
		},
		"runtime.model_key": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]string{"value": "temporary-test-key"}, nil
		},
		"runtime.steer": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string][]string{"messages": nil}, nil
		},
		"runtime.live_env": func(context.Context, *rpc, json.RawMessage) (any, error) {
			if liveCalls.Add(1) == 1 {
				return liveState{Date: "2026-08-20"}, nil
			}
			return liveState{
				Date: "2026-08-20",
				Task: &liveTask{
					ID: "t_1234abcd", Title: "add upload retry", Status: "running", Active: true,
				},
				Project: &liveProject{ID: "pippo_123abc", Name: "pippo", Path: "/work/pippo"},
			}, nil
		},
		"runtime.task": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var value taskArgs
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			tasks <- value
			return map[string]any{
				"task_id": "t_1234abcd", "project_id": "pippo_123abc",
				"project_registered": true, "status": "running",
			}, nil
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
		"max_parallel_runs":4,"max_background_jobs":4,
		"max_steps":{"orchestrator":300},"max_depth":3
	}`)
	var ready struct {
		Ready bool `json:"ready"`
	}
	if err := client.call(context.Background(), "hello", hello{
		Paths:    paths{Runtime: "/runtime", Cache: "/cache", Agent: "/agent"},
		Platform: platform{OS: "test", Arch: "test"}, Settings: settings, Preset: testPreset(),
	}, &ready); err != nil {
		t.Fatal(err)
	}
	id := callID{Turn: "turn-task", Request: "request-task"}
	var start accepted
	if err := client.call(context.Background(), "turn.start", startRequest{
		callID: id, Query: "make a change",
	}, &start); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, tasks); got.Action != "create" || got.Title != "add upload retry" || got.Path != "/work/pippo" {
		t.Fatalf("task request = %#v", got)
	}
	if got := receive(t, chunks); got.Text != "task registered" || got.callID != id {
		t.Fatalf("chunk = %#v", got)
	}
	if got := receive(t, closedEvents); got.Status != "done" || got.callID != id {
		t.Fatalf("closed = %#v", got)
	}
	if provider.err != nil {
		t.Fatal(provider.err)
	}
}

func TestTurnWaitsForClarificationAndContinuesWithAnswer(t *testing.T) {
	provider := &clarifyProvider{}
	state := &state{loop: newLoop(provider)}
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	questions := make(chan clarifyRequest, 1)
	answers := make(chan string, 1)
	chunks := make(chan chunk, 1)
	closedEvents := make(chan closed, 1)
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"runtime.ping": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]bool{"ready": true}, nil
		},
		"runtime.model_key": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]string{"value": "temporary-test-key"}, nil
		},
		"runtime.steer": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string][]string{"messages": nil}, nil
		},
		"runtime.live_env": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return liveState{Date: "2026-08-20"}, nil
		},
		"runtime.clarify": func(ctx context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var value clarifyRequest
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			questions <- value
			select {
			case answer := <-answers:
				return map[string]string{"answer": answer}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
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
		"max_parallel_runs":4,"max_background_jobs":4,
		"max_steps":{"orchestrator":300},"max_depth":3
	}`)
	var ready struct {
		Ready bool `json:"ready"`
	}
	if err := client.call(context.Background(), "hello", hello{
		Paths:    paths{Runtime: "/runtime", Cache: "/cache", Agent: "/agent"},
		Platform: platform{OS: "test", Arch: "test"}, Settings: settings, Preset: testPreset(),
	}, &ready); err != nil {
		t.Fatal(err)
	}
	id := callID{Turn: "turn-clarify", Request: "request-clarify"}
	var start accepted
	if err := client.call(context.Background(), "turn.start", startRequest{
		callID: id, Query: "add retry",
	}, &start); err != nil {
		t.Fatal(err)
	}
	question := receive(t, questions)
	if question.callID != id || question.CallID != "clarify-1" ||
		question.Question != "Which failures should retry?" || len(question.Options) != 2 ||
		!question.Options[0].Recommended {
		t.Fatalf("clarification = %#v", question)
	}
	select {
	case got := <-chunks:
		t.Fatalf("model continued before the answer: %#v", got)
	default:
	}
	answers <- "Transient failures"
	if got := receive(t, chunks); got.Text != "I will use transient failures." || got.callID != id {
		t.Fatalf("chunk = %#v", got)
	}
	if got := receive(t, closedEvents); got.Status != "done" || got.callID != id {
		t.Fatalf("closed = %#v", got)
	}
	if provider.err != nil {
		t.Fatal(provider.err)
	}
}

func testPreset() json.RawMessage {
	return json.RawMessage(`{"all":{}}`)
}

type blockingProvider struct {
	started chan model.Request
	mu      sync.Mutex
	keyOK   bool
}

type taskProvider struct {
	mu    sync.Mutex
	round int
	err   error
}

type clarifyProvider struct {
	mu    sync.Mutex
	round int
	err   error
}

func (p *clarifyProvider) Stream(
	_ context.Context,
	key string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.round++
	if key != "temporary-test-key" || len(request.Tools) != 3 ||
		request.Tools[0].Name != "task" || request.Tools[1].Name != "subagent" ||
		request.Tools[2].Name != "clarify" {
		p.err = errors.New("orchestrator tools were not declared in fixed order")
		return p.err
	}
	if p.round == 1 {
		return yield(model.Chunk{Call: &model.Call{
			ID: "clarify-1", Name: "clarify", Args: map[string]any{
				"question": "Which failures should retry?",
				"options": []any{
					map[string]any{"label": "Transient failures", "recommended": true},
					map[string]any{"label": "All failures"},
				},
			},
		}})
	}
	if p.round != 2 || len(request.History) != 2 || len(request.History[1].Results) != 1 {
		p.err = errors.New("clarification result was not returned to the model")
		return p.err
	}
	if answer, _ := request.History[1].Results[0].Data["answer"].(string); answer != "Transient failures" {
		p.err = errors.New("model did not receive the clarification answer")
		return p.err
	}
	return yield(model.Chunk{Text: "I will use transient failures."})
}

func (p *taskProvider) Stream(
	_ context.Context,
	key string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.round++
	if key != "temporary-test-key" || len(request.Tools) != 3 || request.Tools[0].Name != "task" ||
		request.Tools[1].Name != "subagent" || request.Tools[2].Name != "clarify" {
		p.err = errors.New("orchestrator tools were not declared in fixed order")
		return p.err
	}
	if len(request.Blocks) == 0 || request.Blocks[len(request.Blocks)-1].Kind != model.LiveEnvironment {
		p.err = errors.New("live environment was not placed last")
		return p.err
	}
	live := request.Blocks[len(request.Blocks)-1].Text
	if p.round == 1 {
		if !strings.Contains(live, "active task: none") {
			p.err = errors.New("first model call did not get current empty task state")
			return p.err
		}
		return yield(model.Chunk{Call: &model.Call{
			ID: "call-1", Name: "task", Args: map[string]any{
				"action": "create", "title": "add upload retry", "path": "/work/pippo",
			},
		}})
	}
	if p.round != 2 || len(request.History) != 2 || len(request.History[1].Results) != 1 {
		p.err = errors.New("task result was not returned to the model")
		return p.err
	}
	if !strings.Contains(live, "active task: t_1234abcd") || !strings.Contains(live, "project dir: /work/pippo") {
		p.err = errors.New("second model call did not refresh task state")
		return p.err
	}
	output := request.History[1].Results[0].Data["output"]
	if output == nil {
		p.err = errors.New("task result has no output")
		return p.err
	}
	return yield(model.Chunk{Text: "task registered"})
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
