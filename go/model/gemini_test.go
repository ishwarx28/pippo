// Checks provider request ordering at the model boundary.
package model

import (
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
	}, tail)
	if len(contents) != 3 || contents[2].Role != genai.RoleUser || len(contents[2].Parts) != 2 {
		t.Fatalf("contents = %#v", contents)
	}
	last := contents[2].Parts[1]
	if last.Text != "fresh state" || last.FunctionResponse != nil {
		t.Fatalf("last part = %#v", last)
	}
}
