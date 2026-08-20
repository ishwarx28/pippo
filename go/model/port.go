// Owns provider-neutral model stream types.
package model

import "context"

type BlockKind string

const (
	SystemPrompt      BlockKind = "system_prompt"
	ToolDeclarations  BlockKind = "tool_declarations"
	StaticEnvironment BlockKind = "static_environment"
	SkillsIndex       BlockKind = "skills_index"
	Summary           BlockKind = "summary"
	GlobalPreferences BlockKind = "global_preferences"
	Transcript        BlockKind = "transcript"
	Query             BlockKind = "query"
	LiveEnvironment   BlockKind = "live_environment"
)

type Block struct {
	Kind BlockKind
	Text string
}

type Request struct {
	Model       string
	Reasoning   Reasoning
	Temperature *float32
	Blocks      []Block
	Tools       []Tool
	History     []Message
	Media       []Media
}

type Reasoning string

const (
	ReasoningOff    Reasoning = "off"
	ReasoningLow    Reasoning = "low"
	ReasoningMedium Reasoning = "medium"
	ReasoningHigh   Reasoning = "high"
)

type Media struct {
	Label string
	MIME  string
	Data  []byte
}

type Chunk struct {
	Text string
	Call *Call
}

type Tool struct {
	Name        string
	Description string
	Parameters  map[string]any
}

type Call struct {
	ID   string
	Name string
	Args map[string]any
}

type Result struct {
	ID   string
	Name string
	Data map[string]any
	Err  error
}

type Message struct {
	Role    string
	Text    string
	Calls   []Call
	Results []Result
}

type Provider interface {
	Stream(context.Context, string, Request, func(Chunk) error) error
}
