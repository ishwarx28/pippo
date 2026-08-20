// Owns model tool declarations and dispatch to the runtime.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"pippo/go/model"
)

var taskTool = model.Tool{
	Name:        "task",
	Description: "Register work before delegating it, then close that work with its outcome.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{"type": "string", "enum": []string{"create", "update"}},
			"title":  map[string]any{"type": "string", "description": "Two to eight words."},
			"path":   map[string]any{"type": "string", "description": "Absolute project path."},
			"id":     map[string]any{"type": "string", "description": "Task id returned by create."},
			"status": map[string]any{"type": "string", "enum": []string{"done", "failed", "abandoned"}},
			"note":   map[string]any{"type": "string", "description": "Concise outcome or failure detail."},
		},
		"required":             []string{"action"},
		"additionalProperties": false,
	},
}

type taskArgs struct {
	Action string `json:"action"`
	Title  string `json:"title,omitempty"`
	Path   string `json:"path,omitempty"`
	ID     string `json:"id,omitempty"`
	Status string `json:"status,omitempty"`
	Note   string `json:"note,omitempty"`
}

func declarations(tools []model.Tool) (string, error) {
	lines := make([]string, 0, len(tools))
	for _, tool := range tools {
		value, err := json.Marshal(tool)
		if err != nil {
			return "", fmt.Errorf("encode tool declaration %s: %w", tool.Name, err)
		}
		lines = append(lines, string(value))
	}
	return strings.Join(lines, "\n"), nil
}

func execTool(ctx context.Context, peer *rpc, call model.Call) model.Result {
	result := model.Result{ID: call.ID, Name: call.Name}
	if call.Name != taskTool.Name {
		result.Data = map[string]any{"error": "unknown tool"}
		return result
	}
	raw, err := json.Marshal(call.Args)
	if err != nil {
		result.Data = map[string]any{"error": "invalid task arguments"}
		return result
	}
	var args taskArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		result.Data = map[string]any{"error": "invalid task arguments"}
		return result
	}
	if err := checkTask(args); err != nil {
		result.Data = map[string]any{"error": err.Error()}
		return result
	}
	var output map[string]any
	if err := peer.call(ctx, "runtime.task", args, &output); err != nil {
		result.Data = map[string]any{"error": err.Error()}
	} else {
		result.Data = map[string]any{"output": output}
	}
	return result
}

func checkTask(args taskArgs) error {
	switch args.Action {
	case "create":
		if args.Title == "" || args.Path == "" || args.ID != "" || args.Status != "" || args.Note != "" {
			return errors.New("create requires only title and path")
		}
	case "update":
		if args.ID == "" || args.Status == "" || args.Note == "" || args.Title != "" || args.Path != "" {
			return errors.New("update requires only id, status and note")
		}
		if args.Status != "done" && args.Status != "failed" && args.Status != "abandoned" {
			return fmt.Errorf("invalid task status %q", args.Status)
		}
	default:
		return fmt.Errorf("invalid task action %q", args.Action)
	}
	return nil
}
