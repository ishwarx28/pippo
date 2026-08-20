// Adapts provider-neutral streams to Google GenAI.
package model

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/genai"
)

type Gemini struct{}

func (Gemini) Stream(
	ctx context.Context,
	key string,
	request Request,
	yield func(Chunk) error,
) error {
	if key == "" {
		return errors.New("model key is missing")
	}
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  key,
		Backend: genai.BackendGeminiAPI,
	})
	key = ""
	if err != nil {
		return fmt.Errorf("create Gemini client: %w", err)
	}
	prefix, prompt, tail := content(request.Blocks)
	if len(prompt) == 0 && len(tail) == 0 {
		return errors.New("model request has no live content")
	}
	config := &genai.GenerateContentConfig{Tools: tools(request.Tools)}
	if len(prefix) != 0 {
		config.SystemInstruction = &genai.Content{Parts: prefix}
	}
	contents := conversation(prompt, request.History, request.Media, tail)
	for response, streamErr := range client.Models.GenerateContentStream(
		ctx,
		request.Model,
		contents,
		config,
	) {
		if streamErr != nil {
			return fmt.Errorf("stream Gemini response: %w", streamErr)
		}
		if len(response.Candidates) == 0 || response.Candidates[0].Content == nil {
			continue
		}
		for _, part := range response.Candidates[0].Content.Parts {
			chunk := Chunk{Text: part.Text}
			if part.FunctionCall != nil {
				chunk.Call = &Call{
					ID: part.FunctionCall.ID, Name: part.FunctionCall.Name, Args: part.FunctionCall.Args,
				}
			}
			if chunk.Text != "" || chunk.Call != nil {
				if err := yield(chunk); err != nil {
					return fmt.Errorf("emit Gemini response: %w", err)
				}
			}
		}
	}
	return nil
}

func tools(input []Tool) []*genai.Tool {
	if len(input) == 0 {
		return nil
	}
	declarations := make([]*genai.FunctionDeclaration, 0, len(input))
	for _, tool := range input {
		declarations = append(declarations, &genai.FunctionDeclaration{
			Name: tool.Name, Description: tool.Description, ParametersJsonSchema: tool.Parameters,
		})
	}
	return []*genai.Tool{{FunctionDeclarations: declarations}}
}

func history(input []Message) []*genai.Content {
	contents := make([]*genai.Content, 0, len(input))
	for _, message := range input {
		parts := make([]*genai.Part, 0, 1+len(message.Calls)+len(message.Results))
		if message.Text != "" {
			parts = append(parts, genai.NewPartFromText(message.Text))
		}
		for _, call := range message.Calls {
			parts = append(parts, &genai.Part{FunctionCall: &genai.FunctionCall{
				ID: call.ID, Name: call.Name, Args: call.Args,
			}})
		}
		for _, result := range message.Results {
			parts = append(parts, &genai.Part{FunctionResponse: &genai.FunctionResponse{
				ID: result.ID, Name: result.Name, Response: result.Data,
			}})
		}
		var role genai.Role = genai.RoleUser
		if message.Role == "model" {
			role = genai.RoleModel
		}
		contents = append(contents, genai.NewContentFromParts(parts, role))
	}
	return contents
}

func content(blocks []Block) (prefix, prompt, tail []*genai.Part) {
	for _, block := range blocks {
		part := genai.NewPartFromText(block.Text)
		switch block.Kind {
		case SystemPrompt, ToolDeclarations, StaticEnvironment, SkillsIndex, Summary, GlobalPreferences:
			prefix = append(prefix, part)
		case LiveEnvironment:
			tail = append(tail, part)
		default:
			prompt = append(prompt, part)
		}
	}
	return prefix, prompt, tail
}

func conversation(prompt []*genai.Part, messages []Message, media []Media, tail []*genai.Part) []*genai.Content {
	for _, item := range media {
		prompt = append(prompt, genai.NewPartFromText(item.Label), genai.NewPartFromBytes(item.Data, item.MIME))
	}
	contents := make([]*genai.Content, 0, 1+len(messages))
	if len(prompt) != 0 {
		contents = append(contents, genai.NewContentFromParts(prompt, genai.RoleUser))
	}
	contents = append(contents, history(messages)...)
	if len(tail) == 0 {
		return contents
	}
	if len(contents) != 0 && contents[len(contents)-1].Role == genai.RoleUser {
		last := contents[len(contents)-1]
		last.Parts = append(last.Parts, tail...)
		return contents
	}
	return append(contents, genai.NewContentFromParts(tail, genai.RoleUser))
}
