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

var subagentTool = model.Tool{
	Name:        "subagent",
	Description: "Spawn or control a planner, explorer, or worker run.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action":      map[string]any{"type": "string", "enum": []string{"spawn", "wait", "pause", "resume", "stop"}},
			"role":        map[string]any{"type": "string", "enum": []string{"planner", "explorer", "worker"}},
			"task_id":     map[string]any{"type": "string"},
			"title":       map[string]any{"type": "string"},
			"request":     map[string]any{"type": "string"},
			"constraints": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"media":       map[string]any{"type": "array", "items": map[string]any{"type": "integer", "minimum": 1}},
			"related":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"highlight":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"wait":        map[string]any{"type": "boolean"},
			"id":          map[string]any{"type": "string"},
			"ids":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"answers":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"amend":       map[string]any{"type": "string"},
		},
		"required": []string{"action"}, "additionalProperties": false,
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

var planTool = model.Tool{
	Name:        "plan",
	Description: "Create an ordered task plan or update one step's progress.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action":  map[string]any{"type": "string", "enum": []string{"create", "update"}},
			"task_id": map[string]any{"type": "string"},
			"goal":    map[string]any{"type": "string"},
			"steps": map[string]any{"type": "array", "items": map[string]any{
				"type": "object", "properties": map[string]any{
					"title": map[string]any{"type": "string"}, "detail": map[string]any{"type": "string"},
					"files":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"verify": map[string]any{"type": "string"}, "risk": map[string]any{"type": "string"},
				}, "required": []string{"title", "detail", "files", "verify", "risk"}, "additionalProperties": false,
			}},
			"step_id": map[string]any{"type": "string"}, "status": map[string]any{"type": "string"},
			"note": map[string]any{"type": "string"},
		},
		"required": []string{"action"}, "additionalProperties": false,
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

type subagentArgs struct {
	Action      string   `json:"action"`
	Role        string   `json:"role,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`
	Title       string   `json:"title,omitempty"`
	Request     string   `json:"request,omitempty"`
	Constraints []string `json:"constraints,omitempty"`
	Media       []int    `json:"media,omitempty"`
	Related     []string `json:"related,omitempty"`
	Highlight   []string `json:"highlight,omitempty"`
	Wait        bool     `json:"wait,omitempty"`
	ID          string   `json:"id,omitempty"`
	IDs         []string `json:"ids,omitempty"`
	Answers     []string `json:"answers,omitempty"`
	Amend       string   `json:"amend,omitempty"`
	origin      callID
}

type clarifyOption struct {
	Label       string `json:"label"`
	Recommended bool   `json:"recommended,omitempty"`
}

type clarifyArgs struct {
	Question string          `json:"question"`
	Options  []clarifyOption `json:"options,omitempty"`
}

type planStep struct {
	Title, Detail, Verify, Risk string
	Files                       []string `json:"files"`
}

type planArgs struct {
	Action string     `json:"action"`
	TaskID string     `json:"task_id,omitempty"`
	Goal   string     `json:"goal,omitempty"`
	Steps  []planStep `json:"steps,omitempty"`
	StepID string     `json:"step_id,omitempty"`
	Status string     `json:"status,omitempty"`
	Note   string     `json:"note,omitempty"`
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
	CallID string `json:"call_id"`
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
	CallID string `json:"call_id"`
	TaskID string `json:"task_id,omitempty"`
	writeArgs
}

type editRequest struct {
	callID
	CallID string `json:"call_id"`
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
	Role   string `json:"role"`
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

func execTool(
	ctx context.Context,
	peer *rpc,
	agents *runSet,
	role string,
	id callID,
	taskID string,
	call model.Call,
) model.Result {
	result := model.Result{ID: call.ID, Name: call.Name}
	if !allows(role, call.Name) {
		failTool(&result, "denied", fmt.Sprintf("tool %s is unavailable to role %s", call.Name, role))
		return result
	}
	switch call.Name {
	case taskTool.Name:
		var args taskArgs
		if err := decodeArgs(call.Args, &args); err != nil {
			failTool(&result, "bad_args", "invalid task arguments")
			return result
		}
		if err := checkTask(args); err != nil {
			failTool(&result, "bad_args", err.Error())
			return result
		}
		var output map[string]any
		rpcTool(&result, peer.call(ctx, "runtime.task", args, &output), map[string]any{"output": output})
	case subagentTool.Name:
		var args subagentArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkSubagent(args) != nil {
			failTool(&result, "bad_args", "invalid subagent arguments")
			return result
		}
		if agents == nil {
			failTool(&result, "busy", "subagent lifecycle is unavailable")
			return result
		}
		args.origin = id
		caller := ""
		if role == "planner" {
			caller = id.Turn
		}
		output, err := agents.act(ctx, peer, role, caller, taskID, args)
		if err != nil {
			localTool(&result, err)
		} else {
			result.Data = map[string]any{"output": output}
		}
	case clarifyTool.Name:
		var args clarifyArgs
		if err := decodeArgs(call.Args, &args); err != nil {
			failTool(&result, "bad_args", "invalid clarify arguments")
			return result
		}
		if err := checkClarify(args); err != nil {
			failTool(&result, "bad_args", err.Error())
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
			rpcTool(&result, err, nil)
		} else {
			result.Data = map[string]any{"answer": output.Answer}
		}
	case findTool.Name:
		var args findArgs
		if err := decodeArgs(call.Args, &args); err != nil {
			failTool(&result, "bad_args", "invalid find arguments")
			return result
		}
		if err := checkFind(args); err != nil {
			failTool(&result, "bad_args", err.Error())
			return result
		}
		input := findRequest{callID: id, CallID: call.ID, TaskID: taskID, findArgs: args}
		output, err := callFind(ctx, peer, input)
		if err != nil && ctx.Err() != nil {
			_ = peer.notify("runtime.approval.cancel", shellCancel{callID: id, CallID: call.ID})
		}
		rpcTool(&result, err, output)
	case writeTool.Name:
		var args writeArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkWrite(args) != nil {
			failTool(&result, "bad_args", "write requires path and content")
			return result
		}
		var output map[string]any
		input := writeRequest{callID: id, CallID: call.ID, TaskID: taskID, writeArgs: args}
		if err := peer.call(ctx, "runtime.write", input, &output); err != nil {
			if ctx.Err() != nil {
				_ = peer.notify("runtime.approval.cancel", shellCancel{callID: id, CallID: call.ID})
			}
			rpcTool(&result, err, nil)
		} else {
			result.Data = output
		}
	case editTool.Name:
		var args editArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkEdit(args) != nil {
			failTool(&result, "bad_args", "edit requires path, target and replacement")
			return result
		}
		var output map[string]any
		input := editRequest{callID: id, CallID: call.ID, TaskID: taskID, editArgs: args}
		if err := peer.call(ctx, "runtime.edit", input, &output); err != nil {
			if ctx.Err() != nil {
				_ = peer.notify("runtime.approval.cancel", shellCancel{callID: id, CallID: call.ID})
			}
			rpcTool(&result, err, nil)
		} else {
			result.Data = output
		}
	case shellTool.Name:
		var args shellArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkShell(args) != nil {
			failTool(&result, "bad_args", "invalid shell arguments")
			return result
		}
		input := shellRequest{callID: id, CallID: call.ID, TaskID: taskID, Role: role, shellArgs: args}
		var output map[string]any
		if err := peer.call(ctx, "runtime.shell", input, &output); err != nil {
			if ctx.Err() != nil {
				_ = peer.notify("runtime.shell.cancel", shellCancel{callID: id, CallID: call.ID})
				_ = peer.notify("runtime.approval.cancel", shellCancel{callID: id, CallID: call.ID})
			}
			rpcTool(&result, err, nil)
		} else {
			result.Data = output
		}
	case planTool.Name:
		var args planArgs
		if err := decodeArgs(call.Args, &args); err != nil || checkPlan(args) != nil {
			failTool(&result, "bad_args", "invalid plan arguments")
		} else {
			failTool(&result, "busy", "plan storage is not available yet")
		}
	default:
		failTool(&result, "bad_args", "unknown tool")
	}
	return result
}

func failTool(result *model.Result, reason, message string) {
	result.Data = map[string]any{"ok": false, "error": map[string]any{"reason": reason, "message": message}}
}

type toolIssue struct {
	reason, message string
	fields          map[string]any
}

func (e *toolIssue) Error() string { return e.message }

func issue(reason, message string, fields ...map[string]any) error {
	value := &toolIssue{reason: reason, message: message}
	if len(fields) != 0 {
		value.fields = fields[0]
	}
	return value
}

func rpcTool(result *model.Result, err error, success map[string]any) {
	if err == nil {
		result.Data = success
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result.Err = err
		return
	}
	var remote *remoteError
	if errors.As(err, &remote) {
		remoteTool(result, remote)
		return
	}
	result.Err = err
}

func localTool(result *model.Result, err error) {
	var remote *remoteError
	var typed *toolIssue
	message := err.Error()
	if errors.As(err, &typed) {
		failTool(result, typed.reason, typed.message)
		failure := result.Data["error"].(map[string]any)
		for key, value := range typed.fields {
			failure[key] = value
		}
	} else if errors.As(err, &remote) {
		remoteTool(result, remote)
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(message, " rpc") || strings.Contains(message, "connection") ||
		strings.Contains(message, "runtime returned inconsistent") || strings.Contains(message, "declare ") {
		result.Err = err
	} else {
		failTool(result, toolReason(message), message)
	}
}

func remoteTool(result *model.Result, remote *remoteError) {
	reason := toolReason(remote.Message)
	if reason == "bad_args" && remote.Code != -32602 {
		reason = "busy"
	}
	failTool(result, reason, remote.Message)
}

func toolReason(message string) string {
	message = strings.ToLower(message)
	switch {
	case strings.Contains(message, "outside scope"):
		return "outside_scope"
	case strings.Contains(message, "denied") || strings.Contains(message, "cannot") || strings.Contains(message, "cancel"):
		return "denied"
	case strings.Contains(message, "not found") || strings.Contains(message, "not registered") ||
		strings.Contains(message, "not owned") || strings.Contains(message, "not active") || strings.Contains(message, "missing"):
		return "not_found"
	case strings.Contains(message, "timeout") || strings.Contains(message, "timed out"):
		return "timeout"
	case strings.Contains(message, "limit"):
		return "limit"
	case strings.Contains(message, "active") || strings.Contains(message, "running") || strings.Contains(message, "settling") ||
		strings.Contains(message, "stopping") || strings.Contains(message, "busy") || strings.Contains(message, "lock") ||
		strings.Contains(message, "sheet") || strings.Contains(message, "unavailable"):
		return "busy"
	default:
		return "bad_args"
	}
}

func callFind(ctx context.Context, peer *rpc, input findRequest) (map[string]any, error) {
	var output map[string]any
	err := peer.call(ctx, "runtime.find", input, &output)
	if err != nil || input.Path == "" || !busyResult(output) {
		return output, err
	}
	output = nil
	err = peer.call(ctx, "runtime.find", input, &output)
	if output != nil {
		output["attempts"] = 2
	}
	return output, err
}

func busyResult(output map[string]any) bool {
	failure, _ := output["error"].(map[string]any)
	reason, _ := failure["reason"].(string)
	message, _ := failure["message"].(string)
	return reason == "busy" && !strings.Contains(strings.ToLower(message), "lock")
}

func checkPlan(args planArgs) error {
	if args.Action == "update" {
		if args.StepID == "" || args.Status == "" || args.Note == "" || args.TaskID != "" ||
			args.Goal != "" || len(args.Steps) != 0 {
			return errors.New("plan update requires step id, status and note")
		}
		return nil
	}
	if args.Action != "create" || args.TaskID == "" || args.Goal == "" || len(args.Steps) == 0 ||
		args.StepID != "" || args.Status != "" || args.Note != "" {
		return errors.New("plan create requires task, goal and steps")
	}
	for _, step := range args.Steps {
		if step.Title == "" || step.Detail == "" || step.Verify == "" || step.Risk == "" {
			return errors.New("plan step is incomplete")
		}
	}
	return nil
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

func checkSubagent(args subagentArgs) error {
	spawn := args.Role != "" || args.TaskID != "" || args.Title != "" || args.Request != "" ||
		len(args.Constraints) != 0 || len(args.Media) != 0 || len(args.Related) != 0 ||
		len(args.Highlight) != 0 || args.Wait
	control := args.ID != "" || len(args.IDs) != 0 || len(args.Answers) != 0 || args.Amend != ""
	switch args.Action {
	case "spawn":
		if control || !spawn {
			return errors.New("spawn accepts only run request fields")
		}
	case "wait":
		if spawn || args.ID != "" || len(args.IDs) == 0 || len(args.Answers) != 0 || args.Amend != "" {
			return errors.New("wait requires only run ids")
		}
	case "pause", "stop":
		if spawn || args.ID == "" || len(args.IDs) != 0 || len(args.Answers) != 0 || args.Amend != "" {
			return fmt.Errorf("%s requires only one run id", args.Action)
		}
	case "resume":
		if spawn || args.ID == "" || len(args.IDs) != 0 {
			return errors.New("resume requires a run id and optional answers or amendment")
		}
	default:
		return fmt.Errorf("invalid subagent action %q", args.Action)
	}
	return nil
}

func checkSpawn(callerRole, callerTask string, args subagentArgs) error {
	if args.Role != "planner" && args.Role != "explorer" && args.Role != "worker" {
		return errors.New("spawn requires a planner, explorer, or worker role")
	}
	if callerRole == "planner" && args.Role == "planner" {
		return errors.New("a planner cannot spawn another planner")
	}
	if strings.TrimSpace(args.Title) == "" || strings.ContainsAny(args.Title, "\r\n") ||
		strings.TrimSpace(args.Request) == "" {
		return errors.New("spawn requires a plain title and request")
	}
	if args.TaskID == "" {
		if callerRole != "orchestrator" || args.Role != "explorer" {
			return errors.New("only an orchestrator scout explorer may omit the task")
		}
		if len(args.Related) != 0 || len(args.Highlight) != 0 {
			return errors.New("a scout cannot receive reports")
		}
	} else if len(args.TaskID) != 10 || !strings.HasPrefix(args.TaskID, "t_") {
		return errors.New("spawn task id is invalid")
	}
	if callerRole == "planner" && args.TaskID != callerTask {
		return errors.New("a planner must reuse its task")
	}
	for _, values := range [][]string{args.Constraints, args.Related, args.Highlight} {
		seen := make(map[string]bool, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) == "" || seen[value] {
				return errors.New("spawn list values must be non-empty and unique")
			}
			seen[value] = true
		}
	}
	seenMedia := make(map[int]bool, len(args.Media))
	for _, number := range args.Media {
		if number < 1 || seenMedia[number] {
			return errors.New("spawn media numbers must be positive and unique")
		}
		seenMedia[number] = true
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
