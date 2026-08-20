// Checks canonical repetition, empty replies and budget thresholds.
package main

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"pippo/go/model"
)

func TestGuardCanonicalRepetitionAndReset(t *testing.T) {
	a := model.Call{Name: "find", Args: map[string]any{
		"root": ".", "range": map[string]any{"end": 3, "start": 1},
	}}
	aReordered := model.Call{Name: "find", Args: map[string]any{
		"range": map[string]any{"start": 1, "end": 3}, "root": ".",
	}}
	b := model.Call{Name: "find", Args: map[string]any{"root": "src"}}
	keys, err := signatures([]model.Call{a, aReordered, b, a, a, a})
	if err != nil || keys[0] != keys[1] {
		t.Fatalf("canonical signatures = %#v, %v", keys, err)
	}
	current := newGuard(workerRole, 30)
	var notices []string
	for _, key := range keys {
		got, err := current.tool(key)
		if err != nil {
			t.Fatal(err)
		}
		notices = append(notices, got...)
	}
	if countText(notices, repeatNotice) != 1 || notices[len(notices)-1] != repeatNotice {
		t.Fatalf("notices = %#v", notices)
	}
	more, err := current.tool(keys[0])
	if err != nil || countText(more, repeatNotice) != 0 {
		t.Fatalf("fourth repeat = %#v, %v", more, err)
	}
}

func TestGuardTextDoesNotBreakToolExecutionOrder(t *testing.T) {
	key := "find\x00{}"
	current := newGuard(explorerRole, 20)
	for index := 0; index < 2; index++ {
		if _, err := current.tool(key); err != nil {
			t.Fatal(err)
		}
	}
	if notice, err := current.reply("I am still checking.", []model.Call{{Name: "find"}}); err != nil || notice != "" {
		t.Fatalf("text reply = %q, %v", notice, err)
	}
	notices, err := current.tool(key)
	if err != nil || !reflect.DeepEqual(notices, []string{repeatNotice}) {
		t.Fatalf("third call = %#v, %v", notices, err)
	}
}

func TestGuardEmptyReplyRetriesOnceAndResets(t *testing.T) {
	current := newGuard(orchestratorRole, 10)
	if notice, err := current.reply("", nil); err != nil || notice != emptyNotice {
		t.Fatalf("first empty = %q, %v", notice, err)
	}
	if notice, err := current.reply("done", nil); err != nil || notice != "" {
		t.Fatalf("success = %q, %v", notice, err)
	}
	if notice, err := current.reply(" \t", nil); err != nil || notice != emptyNotice {
		t.Fatalf("reset empty = %q, %v", notice, err)
	}
	if _, err := current.reply("", nil); err == nil || err.Error() != "model returned two consecutive empty responses" {
		t.Fatalf("second empty = %v", err)
	}
}

func TestGuardBudgetWarningRoundingAndHardCap(t *testing.T) {
	for _, test := range []struct {
		max, warnAt int
	}{
		{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 4}, {6, 5}, {99, 80}, {100, 80},
	} {
		current := newGuard(orchestratorRole, test.max)
		warnings := 0
		for step := 1; step <= test.max; step++ {
			notice, err := current.decision()
			if err != nil {
				t.Fatal(err)
			}
			if notice != "" {
				warnings++
				if step != test.warnAt || notice != orchestratorBudgetNotice {
					t.Fatalf("max %d warned at %d: %q", test.max, step, notice)
				}
			}
		}
		if warnings != 1 {
			t.Fatalf("max %d warnings = %d", test.max, warnings)
		}
		if _, err := current.decision(); err == nil || !strings.Contains(err.Error(), "step budget reached") {
			t.Fatalf("max %d hard cap = %v", test.max, err)
		}
	}
	subagent := newGuard(plannerRole, 1)
	if notice, err := subagent.decision(); err != nil || notice != subagentBudgetNotice {
		t.Fatalf("subagent warning = %q, %v", notice, err)
	}
}

func TestCollectReplyDeduplicatesOnlyWithinOneDecision(t *testing.T) {
	call := model.Call{ID: "same", Name: "find", Args: map[string]any{"query": "needle"}}
	provider := &scriptProvider{replies: []scriptReply{
		{calls: []model.Call{call, call}}, {calls: []model.Call{call}},
	}}
	request := model.Request{Model: "test"}
	first, err := collectReply(context.Background(), provider, "key", request, nil)
	if err != nil || len(first.calls) != 1 {
		t.Fatalf("first decision = %#v, %v", first, err)
	}
	second, err := collectReply(context.Background(), provider, "key", request, nil)
	if err != nil || len(second.calls) != 1 {
		t.Fatalf("second decision = %#v, %v", second, err)
	}
	keys, _ := signatures([]model.Call{first.calls[0], second.calls[0]})
	current := newGuard(explorerRole, 10)
	for _, key := range keys {
		if _, err := current.tool(key); err != nil {
			t.Fatal(err)
		}
	}
	if current.repeat != 2 {
		t.Fatalf("cross-decision repeats = %d", current.repeat)
	}
}

func TestMainLoopEmptyRecoveryAndFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		replies []scriptReply
		wantErr bool
	}{
		{"recovers", []scriptReply{{}, {text: "done"}}, false},
		{"fails", []scriptReply{{}, {}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &scriptProvider{replies: test.replies}
			runs, peer, _ := runHarness(t, provider)
			current := roleDefaults(limits{})[orchestratorRole]
			current.Steps = 10
			request := model.Request{Model: "test", Blocks: []model.Block{{Kind: model.Query, Text: "work"}}}
			main := &loop{provider: provider, agents: runs}
			err := main.run(context.Background(), peer, "key", &request, callID{Turn: "turn", Request: "request"}, current)
			if (err != nil) != test.wantErr || test.wantErr && !strings.Contains(err.Error(), "two consecutive") {
				t.Fatalf("main error = %v", err)
			}
			provider.mu.Lock()
			requests := append([]model.Request(nil), provider.requests...)
			provider.mu.Unlock()
			if len(requests) != 2 || len(requests[1].History) != 1 || requests[1].History[0].Text != emptyNotice {
				t.Fatalf("main requests = %#v", requests)
			}
		})
	}
}

func TestSubagentEmptyRecoveryAndPartialFailure(t *testing.T) {
	t.Run("recovers", func(t *testing.T) {
		provider := &scriptProvider{replies: []scriptReply{{}, {text: "complete report"}}}
		runs, peer, _ := runHarness(t, provider)
		created, err := runs.act(context.Background(), peer, orchestratorRole, "", "", spawnArgs(explorerRole))
		if err != nil {
			t.Fatal(err)
		}
		id := created.(runOutput).ID
		output, err := runs.wait(context.Background(), "", []string{id})
		if err != nil || output[0].Status != runDone || !strings.Contains(output[0].Report, "complete report") {
			t.Fatalf("recovered run = %#v, %v", output, err)
		}
		provider.mu.Lock()
		requests := append([]model.Request(nil), provider.requests...)
		provider.mu.Unlock()
		runs.mu.Lock()
		used := runs.runs[id].budget.used
		runs.mu.Unlock()
		if len(requests) != 2 || requests[1].History[0].Text != emptyNotice || used != 2 {
			t.Fatalf("recovered requests = %#v", requests)
		}
	})
	t.Run("persists partial", func(t *testing.T) {
		provider := &scriptProvider{replies: []scriptReply{
			{text: "Partial work completed.", calls: []model.Call{
				{ID: "find", Name: "find", Args: map[string]any{"query": "x"}},
			}},
			{}, {},
		}}
		runs, peer, runtime := runHarness(t, provider)
		created, err := runs.act(context.Background(), peer, orchestratorRole, "", "", spawnArgs(workerRole))
		if err != nil {
			t.Fatal(err)
		}
		id := created.(runOutput).ID
		output, err := runs.wait(context.Background(), "", []string{id})
		if err != nil || output[0].Status != runFailed || !strings.Contains(output[0].Report, "Partial work completed.") {
			t.Fatalf("partial run = %#v, %v", output, err)
		}
		runtime.mu.Lock()
		finds, reports := runtime.finds, append([]string(nil), runtime.reports[id]...)
		runtime.mu.Unlock()
		runs.mu.Lock()
		used := runs.runs[id].budget.used
		runs.mu.Unlock()
		if finds != 1 || used != 4 || len(reports) != 1 ||
			!strings.Contains(reports[0], "Partial work completed.\n\nStopped: model returned two consecutive") {
			t.Fatalf("partial side effects/reports = %d, %#v", finds, reports)
		}
	})
}

func TestSubagentRepeatedCallNoticeAppearsOnce(t *testing.T) {
	call := model.Call{Name: "find", Args: map[string]any{"query": "needle", "root": "."}}
	provider := &scriptProvider{replies: []scriptReply{
		{text: "checking", calls: []model.Call{call}},
		{text: "still checking", calls: []model.Call{call}},
		{text: "checking again", calls: []model.Call{call}},
		{text: "final report"},
	}}
	runs, peer, runtime := runHarness(t, provider)
	created, err := runs.act(context.Background(), peer, orchestratorRole, "", "", spawnArgs(explorerRole))
	if err != nil {
		t.Fatal(err)
	}
	output, err := runs.wait(context.Background(), "", []string{created.(runOutput).ID})
	if err != nil || output[0].Status != runDone {
		t.Fatalf("repeat run = %#v, %v", output, err)
	}
	provider.mu.Lock()
	requests := append([]model.Request(nil), provider.requests...)
	provider.mu.Unlock()
	runtime.mu.Lock()
	finds := runtime.finds
	runtime.mu.Unlock()
	if len(requests) != 4 || countMessages(requests[3].History, repeatNotice) != 1 || finds != 3 {
		t.Fatalf("repeat requests/finds = %#v, %d", requests, finds)
	}
}

func TestMainLoopRepeatedCallNoticeAppearsOnce(t *testing.T) {
	call := model.Call{Name: "task", Args: map[string]any{
		"action": "create", "title": "inspect retry policy", "path": "/work/project",
	}}
	provider := &scriptProvider{replies: []scriptReply{
		{text: "registering", calls: []model.Call{call}},
		{text: "still registering", calls: []model.Call{call}},
		{text: "registering again", calls: []model.Call{call}},
		{text: "done"},
	}}
	runs, peer, runtime := runHarness(t, provider)
	current := roleDefaults(limits{})[orchestratorRole]
	current.Steps = 20
	request := model.Request{Model: "test", Blocks: []model.Block{{Kind: model.Query, Text: "work"}}}
	main := &loop{provider: provider, agents: runs}
	if err := main.run(context.Background(), peer, "key", &request,
		callID{Turn: "turn-repeat", Request: "request-repeat"}, current); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	requests := append([]model.Request(nil), provider.requests...)
	provider.mu.Unlock()
	runtime.mu.Lock()
	tasks := runtime.tasks
	runtime.mu.Unlock()
	if len(requests) != 4 || countMessages(requests[3].History, repeatNotice) != 1 || tasks != 3 {
		t.Fatalf("main repeat requests/tasks = %#v, %d", requests, tasks)
	}
}

func TestMainLoopDeduplicatesChunksButCountsLaterCalls(t *testing.T) {
	call := model.Call{Name: "task", Args: map[string]any{
		"action": "create", "title": "inspect retry policy", "path": "/work/project",
	}}
	provider := &scriptProvider{replies: []scriptReply{
		{calls: []model.Call{call, call}}, {calls: []model.Call{call}},
		{calls: []model.Call{call, call}}, {text: "done"},
	}}
	runs, peer, runtime := runHarness(t, provider)
	current := roleDefaults(limits{})[orchestratorRole]
	current.Steps = 20
	request := model.Request{Model: "test", Blocks: []model.Block{{Kind: model.Query, Text: "work"}}}
	if err := (&loop{provider: provider, agents: runs}).run(context.Background(), peer, "key", &request,
		callID{Turn: "turn-dedup", Request: "request-dedup"}, current); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	requests := append([]model.Request(nil), provider.requests...)
	provider.mu.Unlock()
	runtime.mu.Lock()
	tasks := runtime.tasks
	runtime.mu.Unlock()
	if len(requests) != 4 || countMessages(requests[3].History, repeatNotice) != 1 || tasks != 3 {
		t.Fatalf("deduplicated requests/tasks = %#v, %d", requests, tasks)
	}
}

type scriptReply struct {
	text  string
	calls []model.Call
}

type scriptProvider struct {
	mu       sync.Mutex
	replies  []scriptReply
	requests []model.Request
}

func (p *scriptProvider) Stream(
	_ context.Context,
	_ string,
	request model.Request,
	yield func(model.Chunk) error,
) error {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	index := len(p.requests) - 1
	reply := scriptReply{}
	if index < len(p.replies) {
		reply = p.replies[index]
	}
	p.mu.Unlock()
	if reply.text != "" {
		if err := yield(model.Chunk{Text: reply.text}); err != nil {
			return err
		}
	}
	for _, call := range reply.calls {
		value := call
		if err := yield(model.Chunk{Call: &value}); err != nil {
			return err
		}
	}
	return nil
}

func countText(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func countMessages(messages []model.Message, want string) int {
	count := 0
	for _, message := range messages {
		if message.Text == want {
			count++
		}
	}
	return count
}
