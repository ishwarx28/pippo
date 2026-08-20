// Owns dynamic context assembly from runtime-owned state.
package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"pippo/go/model"
)

type liveRequest struct {
	TaskID string `json:"task_id,omitempty"`
}

type liveTask struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"`
	Active bool   `json:"active"`
}

type liveProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type liveRun struct {
	ID     string    `json:"run_id"`
	Role   string    `json:"role"`
	Title  string    `json:"title"`
	Status runStatus `json:"status"`
	Order  uint64    `json:"-"`
}

type liveState struct {
	Date     string        `json:"date"`
	Task     *liveTask     `json:"task,omitempty"`
	Project  *liveProject  `json:"project,omitempty"`
	Git      []string      `json:"git,omitempty"`
	Agents   []string      `json:"agents"`
	Projects []liveProject `json:"projects"`
	Runs     []liveRun     `json:"-"`
}

func refreshLive(ctx context.Context, peer *rpc, request *model.Request, taskID string, runs *runSet) error {
	var state liveState
	if err := peer.call(ctx, "runtime.live_env", liveRequest{TaskID: taskID}, &state); err != nil {
		return fmt.Errorf("load live environment: %w", err)
	}
	if runs != nil {
		state.Runs = runs.live()
	}
	text, err := formatLive(state)
	if err != nil {
		return err
	}
	blocks := request.Blocks[:0]
	for _, block := range request.Blocks {
		if block.Kind != model.LiveEnvironment {
			blocks = append(blocks, block)
		}
	}
	request.Blocks = append(blocks, model.Block{Kind: model.LiveEnvironment, Text: text})
	return nil
}

func formatLive(state liveState) (string, error) {
	if strings.TrimSpace(state.Date) == "" {
		return "", fmt.Errorf("live environment has no date")
	}
	lines := []string{"date: " + oneLine(state.Date)}
	if state.Task == nil {
		lines = append(lines, "active task: none")
	} else {
		label := "task"
		if state.Task.Active {
			label = "active task"
		}
		lines = append(lines, fmt.Sprintf(
			"%s: %s · %s · %s",
			label, oneLine(state.Task.ID), oneLine(state.Task.Status), oneLine(state.Task.Title),
		))
	}
	if state.Project != nil {
		lines = append(lines, "project dir: "+oneLine(state.Project.Path))
	}
	if len(state.Git) != 0 {
		lines = append(lines, "git status:")
		for _, line := range state.Git {
			lines = append(lines, "  "+oneLine(line))
		}
	}
	sort.Strings(state.Agents)
	if len(state.Agents) != 0 {
		lines = append(lines, "agents files:")
		for _, path := range state.Agents {
			lines = append(lines, "  "+oneLine(path))
		}
	}
	sort.Slice(state.Projects, func(i, j int) bool {
		if state.Projects[i].ID == state.Projects[j].ID {
			return state.Projects[i].Path < state.Projects[j].Path
		}
		return state.Projects[i].ID < state.Projects[j].ID
	})
	if len(state.Projects) != 0 {
		lines = append(lines, "known projects:")
		for _, project := range state.Projects {
			lines = append(lines, "  "+oneLine(project.ID)+" · "+oneLine(project.Path))
		}
	}
	sort.Slice(state.Runs, func(i, j int) bool {
		return state.Runs[i].Order < state.Runs[j].Order ||
			state.Runs[i].Order == state.Runs[j].Order && state.Runs[i].ID < state.Runs[j].ID
	})
	if len(state.Runs) != 0 {
		lines = append(lines, "open runs:")
		for _, run := range state.Runs {
			lines = append(lines, "  "+oneLine(run.ID)+" · "+oneLine(string(run.Status))+" · "+
				oneLine(run.Role)+" · "+oneLine(run.Title))
		}
	}
	return strings.Join(lines, "\n"), nil
}

func oneLine(value string) string {
	value = strings.ReplaceAll(value, "\r", `\r`)
	return strings.ReplaceAll(value, "\n", `\n`)
}
