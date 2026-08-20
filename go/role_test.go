// Checks fixed role policy, preset inheritance and run budgets.
package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"

	"pippo/go/model"
)

func TestRoleDefaultsAreExact(t *testing.T) {
	roles := roleDefaults(limits{})
	tests := []struct {
		name      string
		model     string
		reasoning model.Reasoning
		steps     int
		tools     []string
		prompt    []string
	}{
		{orchestratorRole, "gemini-3.7-flash", model.ReasoningHigh, 300,
			[]string{"task", "subagent", "clarify"}, []string{"collaborative", "terse"}},
		{plannerRole, "gemini-3.7-flash", model.ReasoningHigh, 200,
			[]string{"subagent", "clarify", "plan"}, []string{"steps", "owners", "checks", "risks"}},
		{explorerRole, "gemini-3.5-flash", model.ReasoningLow, 150,
			[]string{"find", "shell"}, []string{"path:line", "never speculate", "read-only"}},
		{workerRole, "gemini-3.5-flash", model.ReasoningLow, 200,
			[]string{"find", "shell", "write", "edit"}, []string{"surgical", "verify", "partial success"}},
	}
	for _, test := range tests {
		current := roles[test.name]
		if current.Model != test.model || current.Reasoning != test.reasoning || current.Steps != test.steps ||
			!reflect.DeepEqual(toolNames(current.Tools), test.tools) {
			t.Fatalf("%s role = %#v", test.name, current)
		}
		for _, phrase := range test.prompt {
			if !strings.Contains(current.Prompt, phrase) {
				t.Fatalf("%s prompt lacks %q: %q", test.name, phrase, current.Prompt)
			}
		}
	}
	if _, exposed := roles["curator"]; exposed {
		t.Fatal("curator was exposed as a spawnable role")
	}
	for _, name := range []string{explorerRole, workerRole} {
		if !strings.Contains(roles[name].Prompt, blockedOpen) {
			t.Fatalf("%s lacks the blocked protocol", name)
		}
	}
	for _, name := range []string{orchestratorRole, plannerRole} {
		if strings.Contains(roles[name].Prompt, blockedOpen) || !strings.Contains(roles[name].Prompt, "together") {
			t.Fatalf("%s blocked guidance = %q", name, roles[name].Prompt)
		}
	}
}

func TestPresetAllInheritanceAndRoleOverride(t *testing.T) {
	startup := roleHello(`{
		"all":{"model":"shared","reasoning":"medium","temperature":0.4},
		"worker":{"model":"worker-model","reasoning":"off","temperature":0.8}
	}`)
	first, err := resolveRoles(startup)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveRoles(startup)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{orchestratorRole, plannerRole, explorerRole} {
		current := first[name]
		if current.Model != "shared" || current.Reasoning != model.ReasoningMedium ||
			current.Temperature == nil || *current.Temperature != float32(0.4) {
			t.Fatalf("inherited %s role = %#v", name, current)
		}
	}
	worker := first[workerRole]
	if worker.Model != "worker-model" || worker.Reasoning != model.ReasoningOff ||
		worker.Temperature == nil || *worker.Temperature != float32(0.8) || worker.Steps != 100 {
		t.Fatalf("worker override = %#v", worker)
	}
	if first[plannerRole].Steps != 211 || first[explorerRole].Steps != 151 ||
		first[orchestratorRole].Static != second[orchestratorRole].Static {
		t.Fatalf("limits or deterministic static environment changed: %#v", first)
	}
	if want := "role: worker\nmodel: worker-model\nreasoning: off\ntemperature: 0.8"; !strings.Contains(worker.Static, want) {
		t.Fatalf("worker environment = %q", worker.Static)
	}
}

func TestPresetRejectsMissingAndInvalidData(t *testing.T) {
	tests := []string{
		"", `{}`, `{`, `{"unknown":{}}`, `{"all":{"model":" "}}`,
		`{"all":{"reasoning":"extreme"}}`, `{"all":{"temperature":3}}`,
	}
	for _, preset := range tests {
		if _, err := resolveRoles(roleHello(preset)); err == nil {
			t.Fatalf("accepted preset %q", preset)
		}
	}
}

func TestRoleDispatchDeniesUndeclaredToolsBeforeRPC(t *testing.T) {
	for _, test := range []struct{ role, tool string }{
		{orchestratorRole, "find"}, {plannerRole, "task"}, {explorerRole, "write"},
		{workerRole, "clarify"}, {"curator", "find"},
	} {
		result := execTool(t.Context(), nil, nil, test.role, callID{}, "", model.Call{
			ID: "wrong-role", Name: test.tool,
		})
		if failureReason(result) != "denied" {
			t.Fatalf("%s/%s result = %#v", test.role, test.tool, result)
		}
	}
	result := execTool(t.Context(), nil, nil, plannerRole, callID{}, "t_00000001", model.Call{
		ID: "plan", Name: "plan", Args: map[string]any{
			"action": "create", "task_id": "t_00000002", "goal": "goal",
			"steps": []any{map[string]any{
				"title": "one", "detail": "detail", "files": []any{"a"},
				"verify": "test", "risk": "none",
			}},
		},
	})
	if failureReason(result) != "bad_args" {
		t.Fatalf("plan result = %#v", result)
	}
}

func TestStepBudgetWarnsOnceAndStops(t *testing.T) {
	budget := newGuard(workerRole, 5)
	warnings := 0
	for index := 0; index < 5; index++ {
		notice, err := budget.step()
		if err != nil {
			t.Fatal(err)
		}
		if notice != "" {
			warnings++
		}
	}
	if warnings != 1 || budget.room(1) {
		t.Fatalf("budget = %#v, warnings = %d", budget, warnings)
	}
	if _, err := budget.step(); err == nil || err.Error() != "limit: 5-step budget reached" {
		t.Fatalf("hard limit = %v", err)
	}
}

func TestSubagentLoopCarriesRoleConfigAndCountsEveryStep(t *testing.T) {
	provider := &budgetProvider{}
	runs, peer, runtime := runHarness(t, provider)
	temperature := float32(0.7)
	roles := roleDefaults(limits{})
	current := roles[explorerRole]
	current.Model, current.Reasoning, current.Temperature, current.Steps =
		"selected-model", model.ReasoningMedium, &temperature, 5
	current.Static = "fixed role environment"
	roles[explorerRole] = current
	runs.setRoles(roles)
	created, err := runs.act(context.Background(), peer, orchestratorRole, "", "", spawnArgs(explorerRole))
	if err != nil {
		t.Fatal(err)
	}
	output, err := runs.wait(context.Background(), "", []string{created.(runOutput).ID})
	if err != nil || output[0].Status != runDone || !strings.Contains(output[0].Report, "evidence report") {
		t.Fatalf("run output = %#v, %v", output, err)
	}
	provider.mu.Lock()
	requests := append([]model.Request(nil), provider.requests...)
	provider.mu.Unlock()
	if len(requests) != 3 || requests[0].Model != "selected-model" ||
		requests[0].Reasoning != model.ReasoningMedium || requests[0].Temperature == nil ||
		*requests[0].Temperature != temperature ||
		!reflect.DeepEqual(toolNames(requests[0].Tools), []string{"find", "shell"}) {
		t.Fatalf("model requests = %#v", requests)
	}
	if got := blockKinds(requests[0].Blocks); !reflect.DeepEqual(got, []model.BlockKind{
		model.SystemPrompt, model.ToolDeclarations, model.StaticEnvironment, model.Query, model.LiveEnvironment,
	}) {
		t.Fatalf("role block order = %#v", got)
	}
	warnings := 0
	for _, message := range requests[2].History {
		if message.Text == subagentBudgetNotice {
			warnings++
		}
	}
	runtime.mu.Lock()
	finds := runtime.finds
	runtime.mu.Unlock()
	if warnings != 1 || finds != 2 {
		t.Fatalf("warnings = %d, tool calls = %d", warnings, finds)
	}
}

func TestSubagentHardBudgetStopsBeforePartialToolBatch(t *testing.T) {
	provider := &budgetProvider{double: true}
	runs, peer, runtime := runHarness(t, provider)
	roles := roleDefaults(limits{})
	current := roles[explorerRole]
	current.Steps = 2
	roles[explorerRole] = current
	runs.setRoles(roles)
	created, err := runs.act(context.Background(), peer, orchestratorRole, "", "", spawnArgs(explorerRole))
	if err != nil {
		t.Fatal(err)
	}
	output, err := runs.wait(context.Background(), "", []string{created.(runOutput).ID})
	if err != nil || output[0].Status != runFailed || !strings.Contains(output[0].Report, "2-step budget reached") {
		t.Fatalf("hard-limit output = %#v, %v", output, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.finds != 0 {
		t.Fatalf("executed %d calls from an over-budget batch", runtime.finds)
	}
}

type budgetProvider struct {
	mu       sync.Mutex
	requests []model.Request
	double   bool
}

func (p *budgetProvider) Stream(
	_ context.Context,
	_ string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	round, double := len(p.requests), p.double
	p.mu.Unlock()
	if double {
		for index := 0; index < 2; index++ {
			if err := yield(model.Chunk{Call: &model.Call{
				ID: "find-double-" + string(rune('a'+index)), Name: "find",
				Args: map[string]any{"query": "needle"},
			}}); err != nil {
				return err
			}
		}
		return nil
	}
	if round < 3 {
		return yield(model.Chunk{Call: &model.Call{
			ID: "find", Name: "find", Args: map[string]any{"query": "needle"},
		}})
	}
	return yield(model.Chunk{Text: "evidence report"})
}

func toolNames(tools []model.Tool) []string {
	names := make([]string, len(tools))
	for index, tool := range tools {
		names[index] = tool.Name
	}
	return names
}

func blockKinds(blocks []model.Block) []model.BlockKind {
	kinds := make([]model.BlockKind, len(blocks))
	for index, block := range blocks {
		kinds[index] = block.Kind
	}
	return kinds
}

func roleHello(preset string) *hello {
	return &hello{
		Paths: paths{Agent: "/agent", Cache: "/cache"}, Platform: platform{OS: "test", Arch: "arch"},
		Settings: json.RawMessage(`{
			"max_parallel_runs":4,"max_background_jobs":4,
			"max_steps":{"orchestrator":300,"planner":211,"explorer":151,"worker":99},
			"max_depth":3
		}`),
		Preset: json.RawMessage(preset),
	}
}
