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
	prefix, live := content(request.Blocks)
	if len(live) == 0 {
		return errors.New("model request has no live content")
	}
	config := &genai.GenerateContentConfig{}
	if len(prefix) != 0 {
		config.SystemInstruction = &genai.Content{Parts: prefix}
	}
	contents := []*genai.Content{genai.NewContentFromParts(live, genai.RoleUser)}
	for response, streamErr := range client.Models.GenerateContentStream(
		ctx,
		request.Model,
		contents,
		config,
	) {
		if streamErr != nil {
			return fmt.Errorf("stream Gemini response: %w", streamErr)
		}
		if text := response.Text(); text != "" {
			if err := yield(Chunk{Text: text}); err != nil {
				return fmt.Errorf("emit Gemini response: %w", err)
			}
		}
	}
	return nil
}

func content(blocks []Block) (prefix, live []*genai.Part) {
	for _, block := range blocks {
		part := genai.NewPartFromText(block.Text)
		switch block.Kind {
		case SystemPrompt, ToolDeclarations, StaticEnvironment, SkillsIndex, Summary, GlobalPreferences:
			prefix = append(prefix, part)
		default:
			live = append(live, part)
		}
	}
	return prefix, live
}
