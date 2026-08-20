// Checks deterministic live context formatting.
package main

import (
	"strings"
	"testing"
)

func TestFormatLiveUsesFixedOrderAndSortedSets(t *testing.T) {
	text, err := formatLive(liveState{
		Date:    "2026-08-20",
		Task:    &liveTask{ID: "t_1234abcd", Title: "fix\nretry", Status: "running", Active: true},
		Project: &liveProject{ID: "z_222222", Name: "z", Path: "/work/z"},
		Git:     []string{"## main", " M tracked.txt"},
		Agents:  []string{"/work/z/CLAUDE.md", "/work/AGENTS.md"},
		Projects: []liveProject{
			{ID: "z_222222", Path: "/work/z"},
			{ID: "a_111111", Path: "/work/a"},
		},
		Runs: []liveRun{
			{ID: "r_22222222", Role: "worker", Title: "second", Status: runPaused, Order: 2},
			{ID: "r_11111111", Role: "explorer", Title: "first", Status: runRunning, Order: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"date: 2026-08-20",
		"active task: t_1234abcd · running · fix\\nretry",
		"project dir: /work/z",
		"git status:",
		"  ## main",
		"   M tracked.txt",
		"agents files:",
		"  /work/AGENTS.md",
		"  /work/z/CLAUDE.md",
		"known projects:",
		"  a_111111 · /work/a",
		"  z_222222 · /work/z",
		"open runs:",
		"  r_11111111 · running · explorer · first",
		"  r_22222222 · paused · worker · second",
	}, "\n")
	if text != want {
		t.Fatalf("live environment:\n%s\nwant:\n%s", text, want)
	}
	if _, err := formatLive(liveState{}); err == nil {
		t.Fatal("missing date was accepted")
	}
}

func TestFormatLiveDistinguishesClosedAndMissingTasks(t *testing.T) {
	closed, err := formatLive(liveState{
		Date: "2026-08-20",
		Task: &liveTask{ID: "t_1234abcd", Title: "finished work", Status: "done"},
	})
	if err != nil || !strings.Contains(closed, "task: t_1234abcd · done · finished work") {
		t.Fatalf("closed task environment = %q, %v", closed, err)
	}
	empty, err := formatLive(liveState{Date: "2026-08-20"})
	if err != nil || empty != "date: 2026-08-20\nactive task: none" {
		t.Fatalf("empty environment = %q, %v", empty, err)
	}
}
