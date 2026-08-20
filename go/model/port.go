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
	Model  string
	Blocks []Block
}

type Chunk struct {
	Text string
}

type Provider interface {
	Stream(context.Context, string, Request, func(Chunk) error) error
}
