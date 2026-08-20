// Owns prompt assembly and model-stream orchestration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"pippo/go/model"
)

const (
	defaultModel = "gemini-3.7-flash"
	systemPrompt = "You are pippo, a collaborative desktop agent. Register concrete project work with task before delegating it, close it after verified work, and report only what you know."
)

type prompt struct {
	SystemPrompt      string
	ToolDeclarations  string
	StaticEnvironment string
	SkillsIndex       string
	Summary           string
	GlobalPreferences string
	Transcript        string
	Query             string
	LiveEnvironment   string
}

type callID struct {
	Turn    string `json:"turn_id"`
	Request string `json:"request_id"`
}

type startRequest struct {
	callID
	Query      string `json:"query"`
	Transcript string `json:"transcript,omitempty"`
	Model      string `json:"model,omitempty"`
}

type accepted struct {
	callID
	Accepted bool `json:"accepted"`
}

type chunk struct {
	callID
	Text string `json:"text"`
}

type closed struct {
	callID
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type limits struct {
	MaxParallelRuns   uint8 `json:"max_parallel_runs"`
	MaxBackgroundJobs uint8 `json:"max_background_jobs"`
	MaxSteps          struct {
		Orchestrator uint16 `json:"orchestrator"`
	} `json:"max_steps"`
	MaxDepth uint8 `json:"max_depth"`
}

type loop struct {
	provider model.Provider
	agents   *runSet
	mu       sync.Mutex
	active   map[callID]context.CancelFunc
	runs     sync.WaitGroup
	stopping bool
}

func newLoop(provider model.Provider) *loop {
	return &loop{provider: provider, agents: newRunSet(provider), active: make(map[callID]context.CancelFunc)}
}

func (l *loop) start(ctx context.Context, id callID) (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.active[id]; l.stopping || exists {
		return nil, false
	}
	ctx, cancel := context.WithCancel(ctx)
	l.active[id] = cancel
	l.runs.Add(1)
	return ctx, true
}

func (l *loop) finish(id callID) {
	l.mu.Lock()
	delete(l.active, id)
	l.mu.Unlock()
	l.runs.Done()
}

func (l *loop) cancel(id callID) bool {
	l.mu.Lock()
	cancel := l.active[id]
	l.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (l *loop) stop() {
	l.mu.Lock()
	l.stopping = true
	cancels := make([]context.CancelFunc, 0, len(l.active))
	for _, cancel := range l.active {
		cancels = append(cancels, cancel)
	}
	l.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	l.agents.shutdown()
	l.runs.Wait()
}

func startTurn(state *state) handler {
	return func(ctx context.Context, peer *rpc, raw json.RawMessage) (any, error) {
		var input startRequest
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, fmt.Errorf("decode turn start: %w", err)
		}
		input.Turn = strings.TrimSpace(input.Turn)
		input.Request = strings.TrimSpace(input.Request)
		if input.Turn == "" || input.Request == "" || strings.TrimSpace(input.Query) == "" {
			return nil, errors.New("turn id, request id and query are required")
		}
		startup := state.startup()
		if startup == nil || state.loop == nil || state.loop.provider == nil {
			return nil, errors.New("model loop is not ready")
		}
		modelName := strings.TrimSpace(input.Model)
		if modelName == "" {
			modelName = defaultModel
		}
		var settings limits
		if err := json.Unmarshal(startup.Settings, &settings); err != nil {
			return nil, fmt.Errorf("decode model limits: %w", err)
		}
		state.loop.agents.configure(int(settings.MaxParallelRuns), int(settings.MaxDepth))
		environment, err := staticEnvironment(startup, modelName)
		if err != nil {
			return nil, err
		}
		tools := []model.Tool{taskTool, subagentTool, clarifyTool}
		toolText, err := declarations(tools)
		if err != nil {
			return nil, err
		}
		request := assemble(modelName, prompt{
			SystemPrompt:      systemPrompt,
			ToolDeclarations:  toolText,
			StaticEnvironment: environment,
			Transcript:        input.Transcript,
			Query:             input.Query,
		})
		request.Tools = tools
		runCtx, ok := state.loop.start(ctx, input.callID)
		if !ok {
			return nil, errors.New("model request is already running")
		}
		go state.loop.stream(runCtx, peer, input.callID, request)
		return accepted{callID: input.callID, Accepted: true}, nil
	}
}

func cancelTurn(state *state) handler {
	return func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
		var id callID
		if err := json.Unmarshal(raw, &id); err != nil {
			return nil, fmt.Errorf("decode turn cancellation: %w", err)
		}
		if id.Turn == "" || id.Request == "" {
			return nil, errors.New("turn id and request id are required")
		}
		return map[string]bool{"cancelled": state.loop != nil && state.loop.cancel(id)}, nil
	}
}

func (l *loop) stream(ctx context.Context, peer *rpc, id callID, request model.Request) {
	defer l.finish(id)
	status, detail := "done", ""
	var secret struct {
		Value string `json:"value"`
	}
	if err := peer.call(ctx, "runtime.model_key", struct{}{}, &secret); err != nil {
		status, detail = "failed", err.Error()
	} else if secret.Value == "" {
		status, detail = "failed", "model key is missing"
	} else {
		err := l.run(ctx, peer, secret.Value, &request, id)
		secret.Value = ""
		if ctx.Err() != nil {
			status = "cancelled"
		} else if err != nil {
			status, detail = "failed", err.Error()
		}
	}
	if ctx.Err() != nil {
		status, detail = "cancelled", ""
	}
	if err := peer.notify("turn.closed", closed{callID: id, Status: status, Error: detail}); err != nil {
		log.Printf("finish turn %q request %q: %v", id.Turn, id.Request, err)
	}
}

func (l *loop) run(ctx context.Context, peer *rpc, key string, request *model.Request, id callID) error {
	for {
		if err := refreshLive(ctx, peer, request, "", l.agents); err != nil {
			return err
		}
		var text strings.Builder
		var calls []model.Call
		seen := make(map[string]bool)
		err := l.provider.Stream(ctx, key, *request, func(value model.Chunk) error {
			if value.Text != "" {
				text.WriteString(value.Text)
				if err := peer.notify("turn.chunk", chunk{callID: id, Text: value.Text}); err != nil {
					return err
				}
			}
			if value.Call != nil {
				identity, err := json.Marshal(value.Call)
				if err != nil {
					return fmt.Errorf("encode tool call: %w", err)
				}
				if !seen[string(identity)] {
					seen[string(identity)] = true
					calls = append(calls, *value.Call)
				}
			}
			return nil
		})
		if err != nil || len(calls) == 0 {
			return err
		}
		request.History = append(request.History, model.Message{
			Role: "model", Text: text.String(), Calls: calls,
		})
		results := make([]model.Result, 0, len(calls))
		for _, call := range calls {
			results = append(results, execTool(ctx, peer, l.agents, "orchestrator", id, "", call))
		}
		request.History = append(request.History, model.Message{Role: "user", Results: results})
	}
}

func assemble(modelName string, input prompt) model.Request {
	blocks := []model.Block{
		{Kind: model.SystemPrompt, Text: input.SystemPrompt},
		{Kind: model.ToolDeclarations, Text: input.ToolDeclarations},
		{Kind: model.StaticEnvironment, Text: input.StaticEnvironment},
		{Kind: model.SkillsIndex, Text: input.SkillsIndex},
		{Kind: model.Summary, Text: input.Summary},
		{Kind: model.GlobalPreferences, Text: input.GlobalPreferences},
		{Kind: model.Transcript, Text: input.Transcript},
		{Kind: model.Query, Text: input.Query},
		{Kind: model.LiveEnvironment, Text: input.LiveEnvironment},
	}
	request := model.Request{Model: modelName, Blocks: make([]model.Block, 0, len(blocks))}
	for _, block := range blocks {
		block.Text = strings.ReplaceAll(strings.ReplaceAll(block.Text, "\r\n", "\n"), "\r", "\n")
		if strings.TrimSpace(block.Text) != "" {
			request.Blocks = append(request.Blocks, block)
		}
	}
	return request
}

func staticEnvironment(startup *hello, modelName string) (string, error) {
	var settings limits
	if err := json.Unmarshal(startup.Settings, &settings); err != nil {
		return "", fmt.Errorf("decode model limits: %w", err)
	}
	return fmt.Sprintf(
		"agent dir: %s\ncache dir: %s\nplatform: %s/%s\nmodel: %s\nmax parallel runs: %d\nmax background jobs: %d\nmax orchestrator steps: %d\nmax depth: %d",
		startup.Paths.Agent,
		startup.Paths.Cache,
		startup.Platform.OS,
		startup.Platform.Arch,
		modelName,
		settings.MaxParallelRuns,
		settings.MaxBackgroundJobs,
		settings.MaxSteps.Orchestrator,
		settings.MaxDepth,
	), nil
}
