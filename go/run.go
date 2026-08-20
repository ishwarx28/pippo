// Owns subagent lifecycle, ownership and concurrency.
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"pippo/go/model"
)

type runStatus string

const (
	runRunning     runStatus = "running"
	runPaused      runStatus = "paused"
	runDone        runStatus = "done"
	runFailed      runStatus = "failed"
	runStopped     runStatus = "stopped"
	runInterrupted runStatus = "interrupted"
)

type runMeta struct {
	ID          string    `json:"id"`
	ParentID    string    `json:"parent_id,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	ProjectID   string    `json:"project_id,omitempty"`
	Role        string    `json:"role"`
	Title       string    `json:"title"`
	Request     string    `json:"request"`
	Status      runStatus `json:"status"`
	Attempt     int       `json:"attempt"`
	Constraints []string  `json:"constraints"`
	Media       []int     `json:"media"`
	Related     []string  `json:"related"`
	Highlight   []string  `json:"highlight"`
	ReportPath  string    `json:"report_path,omitempty"`
}

type runCreate struct {
	Action      string   `json:"action"`
	ParentID    string   `json:"parent_id,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	Role        string   `json:"role"`
	Title       string   `json:"title"`
	Request     string   `json:"request"`
	Constraints []string `json:"constraints,omitempty"`
	Media       []int    `json:"media,omitempty"`
	Related     []string `json:"related,omitempty"`
	Highlight   []string `json:"highlight,omitempty"`
}

type runUpdate struct {
	Action  string    `json:"action"`
	ID      string    `json:"id"`
	Status  runStatus `json:"status"`
	Attempt int       `json:"attempt"`
	Report  string    `json:"report,omitempty"`
}

type runOutput struct {
	ID        string    `json:"run_id"`
	Status    runStatus `json:"status"`
	Report    string    `json:"report,omitempty"`
	Questions []string  `json:"questions,omitempty"`
}

type agentRun struct {
	meta      runMeta
	depth     int
	order     uint64
	peer      *rpc
	request   model.Request
	report    string
	questions []string
	cancel    context.CancelFunc
	settled   chan struct{}
	active    bool
	epoch     uint64
}

type runSet struct {
	mu          sync.Mutex
	provider    model.Provider
	runs        map[string]*agentRun
	changed     chan struct{}
	maxParallel int
	maxDepth    int
	next        uint64
	stopping    bool
	wg          sync.WaitGroup
}

func newRunSet(provider model.Provider) *runSet {
	return &runSet{
		provider: provider, runs: make(map[string]*agentRun), changed: make(chan struct{}),
		maxParallel: 4, maxDepth: 3,
	}
}

func (s *runSet) configure(parallel, depth int) {
	s.mu.Lock()
	s.maxParallel, s.maxDepth = parallel, depth
	s.mu.Unlock()
}

func (s *runSet) act(
	ctx context.Context,
	peer *rpc,
	callerRole string,
	callerID string,
	taskID string,
	args subagentArgs,
) (any, error) {
	if callerRole != "orchestrator" && callerRole != "planner" {
		return nil, errors.New("this role cannot control subagents")
	}
	parent := ""
	if callerRole == "planner" {
		parent = callerID
	}
	switch args.Action {
	case "spawn":
		return s.spawn(ctx, peer, callerRole, parent, taskID, args)
	case "wait":
		outputs, err := s.wait(ctx, parent, args.IDs)
		return map[string]any{"runs": outputs}, err
	case "pause":
		return s.pause(ctx, parent, args.ID)
	case "resume":
		return s.resume(ctx, parent, args)
	case "stop":
		return s.stop(ctx, parent, args.ID)
	default:
		return nil, fmt.Errorf("invalid subagent action %q", args.Action)
	}
}

func (s *runSet) spawn(
	ctx context.Context,
	peer *rpc,
	callerRole string,
	parent string,
	taskID string,
	args subagentArgs,
) (runOutput, error) {
	if err := checkSpawn(callerRole, taskID, args); err != nil {
		return runOutput{}, err
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return runOutput{}, errors.New("subagents are stopping")
	}
	depth := 1
	if parent != "" {
		owner := s.runs[parent]
		if owner == nil || owner.meta.Status != runRunning || owner.meta.Role != "planner" {
			s.mu.Unlock()
			return runOutput{}, errors.New("parent planner run is not active")
		}
		if owner.meta.TaskID != args.TaskID || args.TaskID != taskID {
			s.mu.Unlock()
			return runOutput{}, errors.New("child run must reuse its parent's task")
		}
		depth = owner.depth + 1
	}
	if depth > s.maxDepth {
		s.mu.Unlock()
		return runOutput{}, errors.New("subagent nesting limit reached")
	}
	if s.activeLocked() >= s.maxParallel {
		s.mu.Unlock()
		return runOutput{}, errors.New("parallel subagent limit reached")
	}
	var meta runMeta
	err := peer.call(context.WithoutCancel(ctx), "runtime.run", runCreate{
		Action: "create", ParentID: parent, TaskID: args.TaskID, Role: args.Role,
		Title: args.Title, Request: args.Request, Constraints: args.Constraints,
		Media: args.Media, Related: args.Related, Highlight: args.Highlight,
	}, &meta)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, fmt.Errorf("create subagent run: %w", err)
	}
	if meta.ID == "" || meta.Status != runRunning || meta.ParentID != parent || meta.TaskID != args.TaskID {
		s.mu.Unlock()
		return runOutput{}, errors.New("runtime returned inconsistent run metadata")
	}
	run := &agentRun{
		meta: meta, depth: depth, peer: peer,
		request: model.Request{Model: defaultModel, Blocks: []model.Block{{Kind: model.Query, Text: args.Request}}},
	}
	s.next++
	run.order = s.next
	s.runs[meta.ID] = run
	s.startLocked(run)
	result := output(run)
	s.mu.Unlock()
	if !args.Wait {
		return result, nil
	}
	results, err := s.wait(ctx, parent, []string{meta.ID})
	if err != nil {
		return runOutput{}, err
	}
	return results[0], nil
}

func (s *runSet) pause(ctx context.Context, parent, id string) (runOutput, error) {
	s.mu.Lock()
	run, err := s.ownedLocked(parent, id)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	if run.meta.Status == runPaused {
		settled := run.settled
		s.mu.Unlock()
		if settled != nil {
			select {
			case <-settled:
			case <-ctx.Done():
				return runOutput{}, ctx.Err()
			}
		}
		s.mu.Lock()
		result := output(run)
		s.mu.Unlock()
		return result, nil
	}
	if run.meta.Status != runRunning {
		s.mu.Unlock()
		return runOutput{}, fmt.Errorf("run %s is not running", id)
	}
	if _, err := s.persistLocked(ctx, run, runPaused, run.meta.Attempt, ""); err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	run.meta.Status = runPaused
	if run.cancel != nil {
		run.cancel()
	}
	settled := run.settled
	s.signalLocked()
	s.mu.Unlock()
	if settled != nil {
		select {
		case <-settled:
		case <-ctx.Done():
			return runOutput{}, ctx.Err()
		}
	}
	s.mu.Lock()
	result := output(run)
	s.mu.Unlock()
	return result, nil
}

func (s *runSet) resume(ctx context.Context, parent string, args subagentArgs) (runOutput, error) {
	s.mu.Lock()
	run, err := s.ownedLocked(parent, args.ID)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	if run.active {
		s.mu.Unlock()
		return runOutput{}, fmt.Errorf("run %s is still settling", args.ID)
	}
	if run.meta.Status == runRunning {
		s.mu.Unlock()
		return runOutput{}, fmt.Errorf("run %s is already running", args.ID)
	}
	if s.activeLocked() >= s.maxParallel {
		s.mu.Unlock()
		return runOutput{}, errors.New("parallel subagent limit reached")
	}
	attempt := run.meta.Attempt
	history := append([]model.Message(nil), run.request.History...)
	if run.report != "" {
		history = append(history, model.Message{Role: "model", Text: run.report})
	}
	if terminal(run.meta.Status) {
		attempt++
	}
	if note := resumeNote(args.Answers, args.Amend); note != "" {
		history = append(history, model.Message{Role: "user", Text: note})
	}
	if _, err := s.persistLocked(ctx, run, runRunning, attempt, ""); err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	run.request.History = history
	run.meta.Status, run.meta.Attempt, run.report = runRunning, attempt, ""
	s.startLocked(run)
	result := output(run)
	s.mu.Unlock()
	return result, nil
}

func (s *runSet) stop(ctx context.Context, parent, id string) (runOutput, error) {
	s.mu.Lock()
	target, err := s.ownedLocked(parent, id)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	ids := s.descendantsLocked(id)
	settled := make([]<-chan struct{}, 0, len(ids))
	for _, current := range ids {
		run := s.runs[current]
		if !terminal(run.meta.Status) {
			if _, err := s.persistLocked(ctx, run, runStopped, run.meta.Attempt, ""); err != nil {
				s.mu.Unlock()
				return runOutput{}, err
			}
			run.meta.Status = runStopped
		}
		if run.active {
			if run.cancel != nil {
				run.cancel()
			}
			settled = append(settled, run.settled)
		}
	}
	s.signalLocked()
	s.mu.Unlock()
	for _, done := range settled {
		select {
		case <-done:
		case <-ctx.Done():
			return runOutput{}, ctx.Err()
		}
	}
	s.mu.Lock()
	result := output(target)
	s.mu.Unlock()
	return result, nil
}

func (s *runSet) wait(ctx context.Context, parent string, ids []string) ([]runOutput, error) {
	for {
		s.mu.Lock()
		outputs := make([]runOutput, 0, len(ids))
		all := true
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				s.mu.Unlock()
				return nil, fmt.Errorf("run %s was repeated", id)
			}
			seen[id] = true
			run, err := s.ownedLocked(parent, id)
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			outputs = append(outputs, output(run))
			all = all && terminal(run.meta.Status)
		}
		changed := s.changed
		s.mu.Unlock()
		if all {
			return outputs, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *runSet) startLocked(run *agentRun) {
	ctx, cancel := context.WithCancel(context.Background())
	run.cancel, run.settled, run.active = cancel, make(chan struct{}), true
	run.epoch++
	epoch := run.epoch
	s.wg.Add(1)
	go s.execute(ctx, run.meta.ID, epoch)
	s.signalLocked()
}

func (s *runSet) execute(ctx context.Context, id string, epoch uint64) {
	defer s.wg.Done()
	s.mu.Lock()
	run := s.runs[id]
	request, peer := run.request, run.peer
	s.mu.Unlock()
	var secret struct {
		Value string `json:"value"`
	}
	err := peer.call(ctx, "runtime.model_key", struct{}{}, &secret)
	var report strings.Builder
	if err == nil {
		err = s.provider.Stream(ctx, secret.Value, request, func(value model.Chunk) error {
			if value.Call != nil {
				return fmt.Errorf("subagent returned undeclared tool %q", value.Call.Name)
			}
			report.WriteString(value.Text)
			return nil
		})
	}
	secret.Value = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	run = s.runs[id]
	if run == nil || run.epoch != epoch {
		return
	}
	run.report = report.String()
	run.active, run.cancel = false, nil
	close(run.settled)
	if run.meta.Status != runRunning {
		s.signalLocked()
		return
	}
	status := runDone
	if err != nil {
		status = runFailed
		if run.report == "" {
			run.report = err.Error()
		}
	}
	if saved, persist := s.persistLocked(
		context.Background(), run, status, run.meta.Attempt, run.report,
	); persist != nil {
		status = runFailed
		run.report = strings.TrimSpace(run.report + "\n" + persist.Error())
	} else if saved.ReportPath != "" {
		run.report = strings.TrimSpace(run.report) +
			"\n\nThis same report is written at " + saved.ReportPath
	}
	run.meta.Status = status
	s.signalLocked()
}

func (s *runSet) interrupt(peer *rpc) {
	s.mu.Lock()
	for _, run := range s.runs {
		if run.peer != peer || terminal(run.meta.Status) {
			continue
		}
		_, _ = s.persistLocked(context.Background(), run, runInterrupted, run.meta.Attempt, "")
		run.meta.Status = runInterrupted
		if run.cancel != nil {
			run.cancel()
		}
	}
	s.signalLocked()
	s.mu.Unlock()
}

func (s *runSet) shutdown() {
	s.mu.Lock()
	s.stopping = true
	peers := make(map[*rpc]bool)
	for _, run := range s.runs {
		peers[run.peer] = true
	}
	s.mu.Unlock()
	for peer := range peers {
		s.interrupt(peer)
	}
	s.wg.Wait()
}

func (s *runSet) persistLocked(
	ctx context.Context,
	run *agentRun,
	status runStatus,
	attempt int,
	report string,
) (runMeta, error) {
	var meta runMeta
	if err := run.peer.call(ctx, "runtime.run", runUpdate{
		Action: "update", ID: run.meta.ID, Status: status, Attempt: attempt, Report: report,
	}, &meta); err != nil {
		return runMeta{}, fmt.Errorf("persist run %s: %w", run.meta.ID, err)
	}
	if meta.ID != run.meta.ID || meta.Status != status || meta.Attempt != attempt {
		return runMeta{}, fmt.Errorf("runtime returned inconsistent state for run %s", run.meta.ID)
	}
	return meta, nil
}

func (s *runSet) ownedLocked(parent, id string) (*agentRun, error) {
	run := s.runs[id]
	if run == nil || run.meta.ParentID != parent {
		return nil, fmt.Errorf("run %s is not owned by this caller", id)
	}
	return run, nil
}

func (s *runSet) activeLocked() int {
	active := 0
	for _, run := range s.runs {
		if run.active {
			active++
		}
	}
	return active
}

func (s *runSet) live() []liveRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]liveRun, 0)
	for _, run := range s.runs {
		if run.meta.Status == runRunning || run.meta.Status == runPaused {
			result = append(result, liveRun{
				ID: run.meta.ID, Role: run.meta.Role, Title: run.meta.Title,
				Status: run.meta.Status, Order: run.order,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Order < result[j].Order ||
			result[i].Order == result[j].Order && result[i].ID < result[j].ID
	})
	return result
}

func (s *runSet) descendantsLocked(id string) []string {
	ids := []string{id}
	for at := 0; at < len(ids); at++ {
		children := make([]string, 0)
		for child, run := range s.runs {
			if run.meta.ParentID == ids[at] {
				children = append(children, child)
			}
		}
		sort.Strings(children)
		ids = append(ids, children...)
	}
	return ids
}

func (s *runSet) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func output(run *agentRun) runOutput {
	return runOutput{ID: run.meta.ID, Status: run.meta.Status, Report: run.report, Questions: run.questions}
}

func terminal(status runStatus) bool {
	return status == runDone || status == runFailed || status == runStopped || status == runInterrupted
}

func resumeNote(answers []string, amend string) string {
	parts := make([]string, 0, len(answers)+1)
	for _, answer := range answers {
		if answer = strings.TrimSpace(answer); answer != "" {
			parts = append(parts, answer)
		}
	}
	if amend = strings.TrimSpace(amend); amend != "" {
		parts = append(parts, amend)
	}
	return strings.Join(parts, "\n")
}
