// Owns fixed role prompts, tools and preset resolution.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"pippo/go/model"
)

const (
	defaultModel     = "gemini-3.7-flash"
	orchestratorRole = "orchestrator"
	plannerRole      = "planner"
	explorerRole     = "explorer"
	workerRole       = "worker"
)

const blockedInstruction = ` If genuinely blocked, first write a complete useful partial report, then end it with this block on its own lines:
<pippo-blocked>
{"questions":["First plain question?"]}
</pippo-blocked>
Include one to four distinct single-line questions. Never emit this block for a normal report.`

type role struct {
	Name        string
	Prompt      string
	Model       string
	Reasoning   model.Reasoning
	Temperature *float32
	Steps       int
	Tools       []model.Tool
	Static      string
}

type presetChoice struct {
	Model       *string          `json:"model,omitempty"`
	Reasoning   *model.Reasoning `json:"reasoning,omitempty"`
	Temperature *float32         `json:"temperature,omitempty"`
}

func resolveRoles(startup *hello) (map[string]role, error) {
	if startup == nil || len(startup.Preset) == 0 {
		return nil, errors.New("active preset data is missing")
	}
	var preset map[string]presetChoice
	if err := json.Unmarshal(startup.Preset, &preset); err != nil || len(preset) == 0 {
		return nil, errors.New("active preset data is invalid")
	}
	for name := range preset {
		if name != "all" && name != orchestratorRole && name != plannerRole &&
			name != explorerRole && name != workerRole && name != "curator" {
			return nil, fmt.Errorf("active preset has unknown role %q", name)
		}
	}
	var limits limits
	if err := json.Unmarshal(startup.Settings, &limits); err != nil {
		return nil, fmt.Errorf("decode model limits: %w", err)
	}
	roles := roleDefaults(limits)
	for _, name := range []string{orchestratorRole, plannerRole, explorerRole, workerRole} {
		current := roles[name]
		for _, choice := range []presetChoice{preset["all"], preset[name]} {
			if err := applyChoice(&current, choice); err != nil {
				return nil, fmt.Errorf("resolve %s preset: %w", name, err)
			}
		}
		current.Static = roleEnvironment(startup, current, limits)
		roles[name] = current
	}
	return roles, nil
}

func roleDefaults(input limits) map[string]role {
	steps := []int{int(input.MaxSteps.Orchestrator), int(input.MaxSteps.Planner),
		int(input.MaxSteps.Explorer), int(input.MaxSteps.Worker)}
	defaults := []int{300, 200, 150, 200}
	for index := range steps {
		if steps[index] == 0 {
			steps[index] = defaults[index]
		} else if steps[index] < 100 {
			steps[index] = 100
		}
	}
	return map[string]role{
		orchestratorRole: {Name: orchestratorRole, Model: "gemini-3.7-flash", Reasoning: model.ReasoningHigh,
			Steps: steps[0], Tools: []model.Tool{taskTool, subagentTool, clarifyTool},
			Prompt: "You are the orchestrator. Be collaborative and terse. Delegate concrete work, ask only outcome-changing questions, distinguish facts from uncertainty, and never use project tools directly. When a run returns blocked questions, ask them together in one clarification and resume with answers in the same order."},
		plannerRole: {Name: plannerRole, Model: "gemini-3.7-flash", Reasoning: model.ReasoningHigh,
			Steps: steps[1], Tools: []model.Tool{subagentTool, clarifyTool, planTool},
			Prompt: "You are the planner. Produce ordered steps with owners, checks, and named risks. Clarify only outcome-changing ambiguity, delegate investigation when useful, and sign off with an executable plan. Ask a run's blocked questions together and resume with answers in the same order."},
		explorerRole: {Name: explorerRole, Model: "gemini-3.5-flash", Reasoning: model.ReasoningLow,
			Steps: steps[2], Tools: []model.Tool{findTool, shellTool},
			Prompt: "You are the explorer. Investigate read-only, cite every finding as path:line, never speculate, and return a concise evidence report. You cannot ask the user." + blockedInstruction},
		workerRole: {Name: workerRole, Model: "gemini-3.5-flash", Reasoning: model.ReasoningLow,
			Steps: steps[3], Tools: []model.Tool{findTool, shellTool, writeTool, editTool},
			Prompt: "You are the worker. Make surgical changes, verify with the project's own build and tests, report partial success honestly, and never ask the user." + blockedInstruction},
	}
}

func applyChoice(current *role, choice presetChoice) error {
	if choice.Model != nil {
		current.Model = strings.TrimSpace(*choice.Model)
		if current.Model == "" {
			return errors.New("model is empty")
		}
	}
	if choice.Reasoning != nil {
		switch *choice.Reasoning {
		case model.ReasoningOff, model.ReasoningLow, model.ReasoningMedium, model.ReasoningHigh:
			current.Reasoning = *choice.Reasoning
		default:
			return fmt.Errorf("reasoning %q is invalid", *choice.Reasoning)
		}
	}
	if choice.Temperature != nil {
		if math.IsNaN(float64(*choice.Temperature)) || *choice.Temperature < 0 || *choice.Temperature > 2 {
			return errors.New("temperature must be between 0 and 2")
		}
		value := *choice.Temperature
		current.Temperature = &value
	}
	return nil
}

func roleEnvironment(startup *hello, current role, limits limits) string {
	temperature := "default"
	if current.Temperature != nil {
		temperature = fmt.Sprintf("%g", *current.Temperature)
	}
	return fmt.Sprintf(
		"agent dir: %s\ncache dir: %s\nplatform: %s/%s\nrole: %s\nmodel: %s\nreasoning: %s\ntemperature: %s\nmax parallel runs: %d\nmax background jobs: %d\nmax steps: %d\nmax depth: %d",
		startup.Paths.Agent, startup.Paths.Cache, startup.Platform.OS, startup.Platform.Arch,
		current.Name, current.Model, current.Reasoning, temperature, limits.MaxParallelRuns,
		limits.MaxBackgroundJobs, current.Steps, limits.MaxDepth,
	)
}

func allows(role, tool string) bool {
	switch role {
	case orchestratorRole:
		return tool == "task" || tool == "subagent" || tool == "clarify"
	case plannerRole:
		return tool == "subagent" || tool == "clarify" || tool == "plan"
	case explorerRole:
		return tool == "find" || tool == "shell"
	case workerRole:
		return tool == "find" || tool == "shell" || tool == "write" || tool == "edit"
	default:
		return false
	}
}

type steps struct {
	used, max int
	warned    bool
}

func (s *steps) take() (bool, error) {
	if s.used >= s.max {
		return false, s.limit()
	}
	s.used++
	if !s.warned && s.used*5 >= s.max*4 {
		s.warned = true
		return true, nil
	}
	return false, nil
}

func (s *steps) room(count int) bool { return count >= 0 && s.used+count <= s.max }

func (s *steps) limit() error { return fmt.Errorf("limit: %d-step budget reached", s.max) }

const convergeNotice = "Step budget is 80% used. Converge and write the final report."
