// Checks tool inputs before they cross the runtime boundary.
package main

import "testing"

func TestClarifyRejectsAmbiguousOptions(t *testing.T) {
	tests := []clarifyArgs{
		{Question: "Two\nquestions?"},
		{Question: "Choose?", Options: []clarifyOption{{Label: "Same"}, {Label: "Same"}}},
		{Question: "Choose?", Options: []clarifyOption{
			{Label: "First", Recommended: true}, {Label: "Second", Recommended: true},
		}},
	}
	for _, input := range tests {
		if checkClarify(input) == nil {
			t.Fatalf("accepted invalid clarification: %#v", input)
		}
	}
}

func TestClarifyAllowsFreeformWithoutOptions(t *testing.T) {
	if err := checkClarify(clarifyArgs{Question: "What should change?"}); err != nil {
		t.Fatal(err)
	}
}
