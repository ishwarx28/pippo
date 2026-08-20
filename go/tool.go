// Owns model tool declarations and dispatch to the runtime.
package main

import (
	"bytes"
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

var clarifyTool = model.Tool{
	Name: "clarify",
	Description: "Ask the user one outcome-changing question and wait for their answer. " +
		"Options are suggestions; the user can always type a free-form answer.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"question": map[string]any{"type": "string", "description": "One plain question."},
			"options": map[string]any{
				"type": "array", "maxItems": 8,
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"label":       map[string]any{"type": "string"},
						"recommended": map[string]any{"type": "boolean"},
					},
					"required": []string{"label"}, "additionalProperties": false,
				},
			},
		},
		"required": []string{"question"}, "additionalProperties": false,
	},
}

var findTool = model.Tool{
	Name:        "find",
	Description: "Search paths or text, or read a bounded line range. Relative paths use the run's project.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query":   map[string]any{"type": "string"},
			"regex":   map[string]any{"type": "boolean"},
			"in":      map[string]any{"type": "string", "enum": []string{"content", "path", "both"}},
			"root":    map[string]any{"type": "string"},
			"context": map[string]any{"type": "integer", "minimum": 0, "maximum": 20},
			"cap":     map[string]any{"type": "integer", "minimum": 1, "maximum": 200},
			"offset":  map[string]any{"type": "integer", "minimum": 0},
			"path":    map[string]any{"type": "string"},
			"range": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"start": map[string]any{"type": "integer", "minimum": 1},
					"end":   map[string]any{"type": "integer", "minimum": 1},
				},
				"required": []string{"start", "end"}, "additionalProperties": false,
			},
		},
		"additionalProperties": false,
	},
}

var writeTool = model.Tool{
	Name:        "write",
	Description: "Create a UTF-8 text file or replace one whole. Relative paths use the run's project.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":    map[string]any{"type": "string"},
			"content": map[string]any{"type": "string"},
		},
		"required": []string{"path", "content"}, "additionalProperties": false,
	},
}

var editTool = model.Tool{
	Name:        "edit",
	Description: "Replace exact text in a file already read by this run. The target must be unique unless all is true.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":        map[string]any{"type": "string"},
			"target":      map[string]any{"type": "string"},
			"replacement": map[string]any{"type": "string"},
			"all":         map[string]any{"type": "boolean"},
		},
		"required": []string{"path", "target", "replacement"}, "additionalProperties": false,
	},
}

var shellTool = model.Tool{
	Name:        "shell",
	Description: "Run a foreground command in its own process group. Relative cwd uses the run's project.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"command":    map[string]any{"type": "string"},
			"cwd":        map[string]any{"type": "string"},
			"timeout":    map[string]any{"type": "integer", "minimum": 1, "maximum": 380},
			"background": map[string]any{"type": "boolean"},
			"env": map[string]any{
				"type": "object", "additionalProperties": map[string]any{"type": "string"},
			},
		},
		"required": []string{"command"}, "additionalProperties": false,
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

type clarifyOption struct {
	Label       string `json:"label"`
	Recommended bool   `json:"recommended,omitempty"`
}

type clarifyArgs struct {
	Question string          `json:"question"`
	Options  []clarifyOption `json:"options,omitempty"`
}

type clarifyRequest struct {
	callID
	CallID string `json:"call_id"`
	clarifyArgs
}

type findRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type findArgs struct {
	Query   string     `json:"query,omitempty"`
	Regex   bool       `json:"regex,omitempty"`
	In      string     `json:"in,omitempty"`
	Root    string     `json:"root,omitempty"`
	Context *int       `json:"context,omitempty"`
	Cap     *int       `json:"cap,omitempty"`
	Offset  *int       `json:"offset,omitempty"`
	Path    string     `json:"path,omitempty"`
	Range   *findRange `json:"range,omitempty"`
}

type findRequest struct {
	callID
	TaskID string `json:"task_id,omitempty"`
	findArgs
}

type writeArgs struct {
	Path    string  `json:"path"`
	Content *string `json:"content"`
}

type editArgs struct {
	Path        string  `json:"path"`
	Target      string  `json:"target"`
	Replacement *string `json:"replacement"`
	All         bool    `json:"all,omitempty"`
}

type writeRequest struct {
	callID
	TaskID string `json:"task_id,omitempty"`
	writeArgs
}

type editRequest struct {
	callID
	TaskID string `json:"task_id,omitempty"`
	editArgs
}

type shellArgs struct {
	Command    string            `json:"command"`
	Cwd        *string           `json:"cwd,omitempty"`
	Timeout    *int              `json:"timeout,omitempty"`
	Background bool              `json:"background,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
}

type shellRequest struct {
	callID
	CallID string `json:"call_id"`
	TaskID string `json:"task_id,omitempty"`
	shellArgs
}

type shellCancel struct {
	callID
	CallID string `json:"call_id"`
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

func execTool(ctx context.Context, peer *rpc, id callID, taskID string, call model.Call) model.Result {
	result := model.Result{ID: call.ID, Name: call.Name}
	switch call.Name {
	case taskTool.Name:
		var args taskArgs
		if err := decodeArgs(call.Args, &args); err != nil {
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
	case clarifyTool.Name:
		var args clarifyArgs
		if err := decodeArgs(call.Args, &args); err != nil {
			result.Data = map[string]any{"error": "invalid clarify arguments"}
			return result
		}
		if err := checkClarify(args); err != nil {
			result.Data = map[string]any{"error": err.Error()}
			return result
		}
		input := clarifyRequest{callID: id, CallID: call.ID, clarifyArgs: args}
		var output struct {
			Answer string `json:"answer"`
		}
		if err := peer.call(ctx, "runtime.clarify", input, &output); err != nil {
			if ctx.Err() != nil {
				_ = peer.notify("runtime.clarify.cancel", input)
			}
			result.Data = map[string]any{"error": err.Error()}
		} else {
			result.Data = map[string]any{"answer": output.Answer}
		}
	case findTool.Name:
		var args findArgs
		if err := decodeArgs(call.Args, &args); err != nil {
			result.Data = map[string]any{"error": "invalid find arguments"}
			return result
		}
		if err := checkFind(args); err != nil {
			result.Data = map[string]any{"error": err.Error()}
			return result
		}
		var output map[string]any
		if err := peer.call(ctx, "runtime.find", findRequest{callID: id, TaskID: taskID, findArgs: args}, &output); err != nil {
			result.Data = map[string]any{"error": err.Error()}
		} else {
			result.Data = output
		}
	case writeTool.Name:
		var args writeArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkWrite(args) != nil {
			result.Data = map[string]any{"error": "write requires path and content"}
			return result
		}
		var output map[string]any
		if err := peer.call(ctx, "runtime.write", writeRequest{callID: id, TaskID: taskID, writeArgs: args}, &output); err != nil {
			result.Data = map[string]any{"error": err.Error()}
		} else {
			result.Data = output
		}
	case editTool.Name:
		var args editArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkEdit(args) != nil {
			result.Data = map[string]any{"error": "edit requires path, target and replacement"}
			return result
		}
		var output map[string]any
		if err := peer.call(ctx, "runtime.edit", editRequest{callID: id, TaskID: taskID, editArgs: args}, &output); err != nil {
			result.Data = map[string]any{"error": err.Error()}
		} else {
			result.Data = output
		}
	case shellTool.Name:
		var args shellArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkShell(args) != nil {
			result.Data = map[string]any{"error": "invalid shell arguments"}
			return result
		}
		input := shellRequest{callID: id, CallID: call.ID, TaskID: taskID, shellArgs: args}
		var output map[string]any
		if err := peer.call(ctx, "runtime.shell", input, &output); err != nil {
			if ctx.Err() != nil {
				_ = peer.notify("runtime.shell.cancel", shellCancel{callID: id, CallID: call.ID})
			}
			result.Data = map[string]any{"error": err.Error()}
		} else {
			result.Data = output
		}
	default:
		result.Data = map[string]any{"error": "unknown tool"}
	}
	return result
}

func checkShell(args shellArgs) error {
	if strings.TrimSpace(args.Command) == "" || strings.ContainsRune(args.Command, 0) {
		return errors.New("shell requires a valid command")
	}
	if args.Cwd != nil && (strings.TrimSpace(*args.Cwd) == "" || strings.ContainsRune(*args.Cwd, 0)) {
		return errors.New("shell cwd is invalid")
	}
	if args.Timeout != nil && (*args.Timeout < 1 || *args.Timeout > 380) {
		return errors.New("shell timeout is outside its limit")
	}
	for name, value := range args.Env {
		if name == "" || strings.ContainsAny(name, "=\x00") || strings.ContainsRune(value, 0) {
			return errors.New("shell environment is invalid")
		}
	}
	return nil
}

func checkWrite(args writeArgs) error {
	if strings.TrimSpace(args.Path) == "" || args.Content == nil {
		return errors.New("write requires path and content")
	}
	return nil
}

func checkEdit(args editArgs) error {
	if strings.TrimSpace(args.Path) == "" || args.Target == "" || args.Replacement == nil {
		return errors.New("edit requires path, target and replacement")
	}
	return nil
}

func checkFind(args findArgs) error {
	search := args.Query != ""
	read := args.Path != ""
	if search == read {
		return errors.New("find requires either query or path")
	}
	if search {
		if args.Range != nil {
			return errors.New("search does not accept a range")
		}
		if args.In != "" && args.In != "content" && args.In != "path" && args.In != "both" {
			return fmt.Errorf("invalid find target %q", args.In)
		}
		if args.Context != nil && (*args.Context < 0 || *args.Context > 20) ||
			args.Cap != nil && (*args.Cap < 1 || *args.Cap > 200) ||
			args.Offset != nil && *args.Offset < 0 {
			return errors.New("find paging value is outside its limit")
		}
		return nil
	}
	if args.Regex || args.In != "" || args.Root != "" || args.Context != nil || args.Cap != nil || args.Offset != nil {
		return errors.New("read accepts only path and range")
	}
	if args.Range != nil && (args.Range.Start < 1 || args.Range.End < args.Range.Start) {
		return errors.New("invalid read range")
	}
	return nil
}

func decodeArgs(input map[string]any, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
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

func checkClarify(args clarifyArgs) error {
	question := strings.TrimSpace(args.Question)
	if question == "" || strings.ContainsAny(question, "\r\n") {
		return errors.New("clarify requires one plain question")
	}
	if len(args.Options) > 8 {
		return errors.New("clarify accepts at most eight options")
	}
	recommended := 0
	seen := make(map[string]bool, len(args.Options))
	for _, option := range args.Options {
		label := strings.TrimSpace(option.Label)
		if label == "" || strings.ContainsAny(label, "\r\n") {
			return errors.New("clarify options must be plain non-empty labels")
		}
		if seen[label] {
			return errors.New("clarify options must be unique")
		}
		seen[label] = true
		if option.Recommended {
			recommended++
		}
	}
	if recommended > 1 {
		return errors.New("clarify accepts at most one recommended option")
	}
	return nil
}
