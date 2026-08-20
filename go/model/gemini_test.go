// Checks provider request ordering at the model boundary.
package model

import (
	"reflect"
	"testing"

	"google.golang.org/genai"
)

func TestConversationPlacesLiveEnvironmentAfterToolResults(t *testing.T) {
	_, prompt, tail := content([]Block{
		{Kind: Query, Text: "start"},
		{Kind: LiveEnvironment, Text: "fresh state"},
	})
	contents := conversation(prompt, []Message{
		{Role: "model", Calls: []Call{{ID: "call-1", Name: "task"}}},
		{Role: "user", Results: []Result{{ID: "call-1", Name: "task"}}},
	}, nil, tail)
	if len(contents) != 3 || contents[2].Role != genai.RoleUser || len(contents[2].Parts) != 2 {
		t.Fatalf("contents = %#v", contents)
	}
	last := contents[2].Parts[1]
	if last.Text != "fresh state" || last.FunctionResponse != nil {
		t.Fatalf("last part = %#v", last)
	}
}

func TestConversationSendsLabeledMediaAsInlineBytes(t *testing.T) {
	_, prompt, tail := content([]Block{{Kind: Query, Text: "inspect this"}})
	contents := conversation(prompt, nil, []Media{{
		Label: "attachment 2 · image/png", MIME: "image/png", Data: []byte{1, 2, 3},
	}}, tail)
	parts := contents[0].Parts
	if len(parts) != 3 || parts[1].Text != "attachment 2 · image/png" ||
		parts[2].InlineData == nil || parts[2].InlineData.MIMEType != "image/png" ||
		!reflect.DeepEqual(parts[2].InlineData.Data, []byte{1, 2, 3}) {
		t.Fatalf("media parts = %#v", parts)
	}
}
