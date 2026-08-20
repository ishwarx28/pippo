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

type liveState struct {
	Date     string        `json:"date"`
	Task     *liveTask     `json:"task,omitempty"`
	Project  *liveProject  `json:"project,omitempty"`
	Git      []string      `json:"git,omitempty"`
	Agents   []string      `json:"agents"`
	Projects []liveProject `json:"projects"`
}

func refreshLive(ctx context.Context, peer *rpc, request *model.Request, taskID string) error {
	var state liveState
	if err := peer.call(ctx, "runtime.live_env", liveRequest{TaskID: taskID}, &state); err != nil {
		return fmt.Errorf("load live environment: %w", err)
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
	return strings.Join(lines, "\n"), nil
}

func oneLine(value string) string {
	value = strings.ReplaceAll(value, "\r", `\r`)
	return strings.ReplaceAll(value, "\n", `\n`)
}
