// Owns subagent lifecycle, ownership and concurrency.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"pippo/go/model"
)

const maxMediaBytes = 20 << 20

type runStatus string

const (
	runRunning     runStatus = "running"
	runPaused      runStatus = "paused"
	runBlocked     runStatus = "blocked"
	runDone        runStatus = "done"
	runFailed      runStatus = "failed"
	runStopped     runStatus = "stopped"
	runInterrupted runStatus = "interrupted"
)

type runMeta struct {
	ID          string      `json:"id"`
	ParentID    string      `json:"parent_id,omitempty"`
	TaskID      string      `json:"task_id,omitempty"`
	ProjectID   string      `json:"project_id,omitempty"`
	Role        string      `json:"role"`
	Title       string      `json:"title"`
	Request     string      `json:"request"`
	Status      runStatus   `json:"status"`
	Attempt     int         `json:"attempt"`
	Constraints []string    `json:"constraints"`
	Media       []int       `json:"media"`
	Related     []string    `json:"related"`
	Highlight   []string    `json:"highlight"`
	ReportPath  string      `json:"report_path,omitempty"`
	Reports     []runReport `json:"reports,omitempty"`
}

type runReport struct {
	Path    string `json:"path"`
	Central bool   `json:"central"`
}

type mediaInput struct {
	MIME string `json:"mime"`
	Data []byte `json:"data"`
}

type numberedMedia struct {
	number int
	value  model.Media
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
	media     []numberedMedia
	role      role
	budget    steps
}

type runSet struct {
	mu          sync.Mutex
	provider    model.Provider
	runs        map[string]*agentRun
	origins     map[callID][]numberedMedia
	roles       map[string]role
	changed     chan struct{}
	maxParallel int
	maxDepth    int
	next        uint64
	stopping    bool
	wg          sync.WaitGroup
}

func newRunSet(provider model.Provider) *runSet {
	return &runSet{
		provider: provider, runs: make(map[string]*agentRun), origins: make(map[callID][]numberedMedia),
		changed:     make(chan struct{}),
		maxParallel: 4, maxDepth: 3,
	}
}

func (s *runSet) attach(id callID, media []numberedMedia) {
	s.mu.Lock()
	s.origins[id] = media
	s.mu.Unlock()
}

func (s *runSet) release(id callID) {
	s.mu.Lock()
	delete(s.origins, id)
	s.mu.Unlock()
}

func (s *runSet) configure(parallel, depth int) {
	s.mu.Lock()
	s.maxParallel, s.maxDepth = parallel, depth
	s.mu.Unlock()
}

func (s *runSet) setRoles(roles map[string]role) {
	s.mu.Lock()
	s.roles = roles
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
		return nil, issue("denied", "this role cannot control subagents")
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
		return runOutput{}, issue("busy", "subagents are stopping")
	}
	depth := 1
	if parent != "" {
		owner := s.runs[parent]
		if owner == nil {
			s.mu.Unlock()
			return runOutput{}, issue("not_found", "parent planner run was not found")
		}
		if owner.meta.Status != runRunning || owner.meta.Role != "planner" {
			s.mu.Unlock()
			return runOutput{}, issue("busy", "parent planner run is not active", map[string]any{"status": owner.meta.Status})
		}
		if owner.meta.TaskID != args.TaskID || args.TaskID != taskID {
			s.mu.Unlock()
			return runOutput{}, issue("denied", "child run must reuse its parent's task")
		}
		depth = owner.depth + 1
	}
	source := s.origins[args.origin]
	if parent != "" {
		source = s.runs[parent].media
	}
	media, err := selectMedia(source, args.Media)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	if depth > s.maxDepth {
		s.mu.Unlock()
		return runOutput{}, issue("limit", "subagent nesting limit reached")
	}
	if s.activeLocked() >= s.maxParallel {
		s.mu.Unlock()
		return runOutput{}, issue("limit", "parallel subagent limit reached")
	}
	current := roleDefaults(limits{})[args.Role]
	if configured, ok := s.roles[args.Role]; ok {
		current = configured
	}
	toolText, err := declarations(current.Tools)
	if err != nil {
		s.mu.Unlock()
		return runOutput{}, fmt.Errorf("declare %s tools: %w", args.Role, err)
	}
	var meta runMeta
	err = peer.call(context.WithoutCancel(ctx), "runtime.run", runCreate{
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
		meta: meta, depth: depth, peer: peer, media: media, role: current, budget: steps{max: current.Steps},
		request: assemble(current.Model, prompt{SystemPrompt: current.Prompt, ToolDeclarations: toolText,
			StaticEnvironment: current.Static, Query: runQuery(meta)}),
	}
	run.request.Tools, run.request.Reasoning, run.request.Temperature =
		current.Tools, current.Reasoning, current.Temperature
	run.request.Media = visibleMedia(media)
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
		status := run.meta.Status
		s.mu.Unlock()
		return runOutput{}, issue("busy", fmt.Sprintf("run %s is not running", id), map[string]any{"status": status})
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
		status := run.meta.Status
		s.mu.Unlock()
		return runOutput{}, issue("busy", fmt.Sprintf("run %s is still settling", args.ID), map[string]any{"status": status})
	}
	if run.meta.Status == runRunning {
		s.mu.Unlock()
		return runOutput{}, issue("busy", fmt.Sprintf("run %s is already running", args.ID), map[string]any{"status": runRunning})
	}
	blocked := run.meta.Status == runBlocked
	if blocked {
		if err := checkAnswers(run.questions, args.Answers); err != nil {
			s.mu.Unlock()
			return runOutput{}, err
		}
	} else if len(args.Answers) != 0 {
		s.mu.Unlock()
		return runOutput{}, issue("bad_args", "answers are only accepted for a blocked run")
	}
	if s.activeLocked() >= s.maxParallel {
		s.mu.Unlock()
		return runOutput{}, issue("limit", "parallel subagent limit reached")
	}
	restart := blocked || terminal(run.meta.Status)
	attempt := run.meta.Attempt
	history := append([]model.Message(nil), run.request.History...)
	if run.report != "" {
		history = append(history, model.Message{Role: "model", Text: run.report})
	}
	if restart {
		attempt++
	}
	if blocked {
		history = append(history, model.Message{Role: "user", Text: answerNote(run.questions, args.Answers)})
	}
	if amend := strings.TrimSpace(args.Amend); amend != "" {
		history = append(history, model.Message{Role: "user", Text: amend})
	}
	if _, err := s.persistLocked(ctx, run, runRunning, attempt, ""); err != nil {
		s.mu.Unlock()
		return runOutput{}, err
	}
	run.request.History = history
	run.meta.Status, run.meta.Attempt, run.report = runRunning, attempt, ""
	run.questions = nil
	if restart {
		run.budget = steps{max: run.role.Steps}
	}
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
				return nil, issue("bad_args", fmt.Sprintf("run %s was repeated", id))
			}
			seen[id] = true
			run, err := s.ownedLocked(parent, id)
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			outputs = append(outputs, output(run))
			all = all && settled(run.meta.Status)
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
	request, peer, current, budget := run.request, run.peer, run.role, run.budget
	task, attempt := run.meta.TaskID, run.meta.Attempt
	s.mu.Unlock()
	var secret struct {
		Value string `json:"value"`
	}
	err := peer.call(ctx, "runtime.model_key", struct{}{}, &secret)
	report := ""
	if err == nil {
		request, report, err = s.run(ctx, peer, secret.Value, id, task, attempt, current, request, &budget)
	}
	secret.Value = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	run = s.runs[id]
	if run == nil || run.epoch != epoch {
		return
	}
	run.request, run.report, run.budget = request, report, budget
	run.active, run.cancel = false, nil
	close(run.settled)
	if run.meta.Status != runRunning {
		s.signalLocked()
		return
	}
	status := runDone
	questions := []string(nil)
	if err == nil {
		var blocked bool
		run.report, questions, blocked, err = blockedReport(run.report, run.role.Name)
		if blocked {
			status = runBlocked
		}
	}
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
		questions = nil
		run.report = strings.TrimSpace(run.report + "\n" + persist.Error())
	} else if saved.ReportPath != "" {
		run.report = strings.TrimSpace(run.report) +
			"\n\nThis same report is written at " + saved.ReportPath
	}
	run.questions = questions
	run.meta.Status = status
	s.signalLocked()
}

func (s *runSet) run(
	ctx context.Context,
	peer *rpc,
	key, id, task string,
	attempt int,
	current role,
	request model.Request,
	budget *steps,
) (model.Request, string, error) {
	last := ""
	callID := callID{Turn: id, Request: fmt.Sprintf("%s_%d", id, attempt)}
	for {
		warn, err := budget.take()
		if err != nil {
			return request, last, err
		}
		if warn {
			request.History = append(request.History, model.Message{Role: "user", Text: convergeNotice})
		}
		if err := refreshLive(ctx, peer, &request, task, s); err != nil {
			return request, last, err
		}
		var text strings.Builder
		var calls []model.Call
		seen := make(map[string]bool)
		err = s.provider.Stream(ctx, key, request, func(value model.Chunk) error {
			text.WriteString(value.Text)
			if value.Call != nil {
				identity, encode := json.Marshal(value.Call)
				if encode != nil {
					return fmt.Errorf("encode tool call: %w", encode)
				}
				if !seen[string(identity)] {
					seen[string(identity)] = true
					calls = append(calls, *value.Call)
				}
			}
			return nil
		})
		last = text.String()
		if err != nil || len(calls) == 0 {
			return request, last, err
		}
		request.History = append(request.History, model.Message{Role: "model", Text: last, Calls: calls})
		if !budget.room(len(calls)) {
			return request, last, budget.limit()
		}
		results := make([]model.Result, 0, len(calls))
		warn = false
		for _, call := range calls {
			crossed, _ := budget.take()
			warn = warn || crossed
			result := execTool(ctx, peer, s, current.Name, callID, task, call)
			if result.Err != nil {
				return request, last, result.Err
			}
			results = append(results, result)
		}
		request.History = append(request.History, model.Message{Role: "user", Results: results})
		if warn {
			request.History = append(request.History, model.Message{Role: "user", Text: convergeNotice})
		}
	}
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
		return nil, issue("not_found", fmt.Sprintf("run %s is not owned by this caller", id))
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
		if run.meta.Status == runRunning || run.meta.Status == runPaused || run.meta.Status == runBlocked {
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

func settled(status runStatus) bool { return status == runBlocked || terminal(status) }

const blockedOpen, blockedClose = "<pippo-blocked>", "</pippo-blocked>"

func blockedReport(text, role string) (string, []string, bool, error) {
	report := strings.TrimSpace(text)
	if role != explorerRole && role != workerRole {
		return report, nil, false, nil
	}
	if !strings.Contains(report, blockedOpen) && !strings.Contains(report, blockedClose) {
		return report, nil, false, nil
	}
	cut := len(report)
	for _, marker := range []string{blockedOpen, blockedClose} {
		if index := strings.Index(report, marker); index >= 0 && index < cut {
			cut = index
		}
	}
	partial := strings.TrimSpace(report[:cut])
	protocolStart := "\n" + blockedOpen + "\n"
	start := strings.LastIndex(report, protocolStart)
	end := "\n" + blockedClose
	if start < 0 || strings.Count(report, blockedOpen) != 1 || strings.Count(report, blockedClose) != 1 ||
		!strings.HasSuffix(report, end) {
		return partial, nil, false, errors.New("invalid blocked report protocol")
	}
	start += len(protocolStart)
	var payload struct {
		Questions []string `json:"questions"`
	}
	decoder := json.NewDecoder(strings.NewReader(report[start : len(report)-len(end)]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return partial, nil, false, fmt.Errorf("invalid blocked questions: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return partial, nil, false, errors.New("invalid blocked questions: trailing data")
	}
	if partial == "" {
		return "", nil, false, errors.New("blocked report has no useful partial report")
	}
	if err := checkQuestions(payload.Questions); err != nil {
		return partial, nil, false, err
	}
	return partial, payload.Questions, true, nil
}

func checkQuestions(questions []string) error {
	if len(questions) == 0 || len(questions) > 4 {
		return errors.New("blocked report requires one to four questions")
	}
	seen := make(map[string]bool, len(questions))
	for index, question := range questions {
		questions[index] = strings.TrimSpace(question)
		key := strings.ToLower(questions[index])
		if err := checkClarify(clarifyArgs{Question: questions[index]}); err != nil || seen[key] {
			return errors.New("blocked questions must be plain, non-empty and distinct")
		}
		seen[key] = true
	}
	return nil
}

func checkAnswers(questions, answers []string) error {
	if len(answers) != len(questions) {
		return fmt.Errorf("blocked run requires %d answers in question order", len(questions))
	}
	for _, answer := range answers {
		if strings.TrimSpace(answer) == "" {
			return errors.New("blocked run answers must be non-empty")
		}
	}
	return nil
}

func answerNote(questions, answers []string) string {
	var text strings.Builder
	text.WriteString("Answers to blocked questions:")
	for index := range questions {
		fmt.Fprintf(&text, "\n%d. Question: %s\n   Answer: %s", index+1, questions[index],
			strings.TrimSpace(answers[index]))
	}
	return text.String()
}

func runQuery(meta runMeta) string {
	var text strings.Builder
	fmt.Fprintf(&text, "<request>\n%s\n</request>\n<constraints>\n", strings.TrimSpace(meta.Request))
	for _, constraint := range meta.Constraints {
		fmt.Fprintf(&text, "- %s\n", strings.TrimSpace(constraint))
	}
	text.WriteString("</constraints>\n<reports>\n")
	for _, report := range meta.Reports {
		if report.Central {
			fmt.Fprintf(&text, "- [central] %s\n", report.Path)
		} else {
			fmt.Fprintf(&text, "- %s\n", report.Path)
		}
	}
	text.WriteString("</reports>")
	return text.String()
}

func prepareMedia(input []mediaInput) ([]numberedMedia, error) {
	allowed := map[string]string{
		"image/png": "image/png", "image/jpeg": "image/jpeg", "image/jpg": "image/jpeg",
		"image/webp": "image/webp", "image/gif": "image/gif", "application/pdf": "application/pdf",
		"text/plain": "text/plain",
	}
	result := make([]numberedMedia, 0, len(input))
	total := 0
	for index, item := range input {
		mime := allowed[strings.ToLower(strings.TrimSpace(item.MIME))]
		if mime == "" || len(item.Data) == 0 {
			return nil, fmt.Errorf("attachment %d has an unsupported MIME type or no data", index+1)
		}
		total += len(item.Data)
		if total > maxMediaBytes {
			return nil, fmt.Errorf("attachments exceed the %d byte inline limit", maxMediaBytes)
		}
		data := append([]byte(nil), item.Data...)
		result = append(result, numberedMedia{number: index + 1, value: model.Media{
			Label: fmt.Sprintf("attachment %d · %s", index+1, mime), MIME: mime, Data: data,
		}})
	}
	return result, nil
}

func selectMedia(source []numberedMedia, numbers []int) ([]numberedMedia, error) {
	byNumber := make(map[int]numberedMedia, len(source))
	for _, item := range source {
		byNumber[item.number] = item
	}
	result := make([]numberedMedia, 0, len(numbers))
	for _, number := range numbers {
		item, ok := byNumber[number]
		if !ok {
			return nil, fmt.Errorf("attachment %d is not available to this run", number)
		}
		item.value.Data = append([]byte(nil), item.value.Data...)
		result = append(result, item)
	}
	return result, nil
}

func visibleMedia(input []numberedMedia) []model.Media {
	result := make([]model.Media, len(input))
	for index, item := range input {
		result[index] = item.value
	}
	return result
}
