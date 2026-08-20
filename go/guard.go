// Owns shared model-loop stall and step guards.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"pippo/go/model"
)

const (
	repeatNotice             = "The same tool call occurred three times in a row. Change approach or stop."
	emptyNotice              = "Your last response was empty. Respond with text or a tool call."
	orchestratorBudgetNotice = "Step budget is 80% used. Converge and respond to the user."
	subagentBudgetNotice     = "Step budget is 80% used. Converge and write the complete report."
)

var emptyFailure = errors.New("model returned two consecutive empty responses")

type guard struct {
	role                     string
	used, max, empty, repeat int
	warned                   bool
	last                     string
}

func newGuard(role string, max int) guard { return guard{role: role, max: max} }

func (g *guard) decision() (string, error) { return g.step() }

func (g *guard) tool(signature string) ([]string, error) {
	notice, err := g.step()
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, 2)
	if notice != "" {
		result = append(result, notice)
	}
	if signature == g.last {
		g.repeat++
	} else {
		g.last, g.repeat = signature, 1
	}
	if g.repeat == 3 {
		result = append(result, repeatNotice)
	}
	return result, nil
}

func (g *guard) step() (string, error) {
	if g.used >= g.max {
		return "", g.limit()
	}
	g.used++
	if !g.warned && g.used*5 >= g.max*4 {
		g.warned = true
		if g.role == orchestratorRole {
			return orchestratorBudgetNotice, nil
		}
		return subagentBudgetNotice, nil
	}
	return "", nil
}

func (g *guard) reply(text string, calls []model.Call) (string, error) {
	if strings.TrimSpace(text) != "" || len(calls) != 0 {
		g.empty = 0
		return "", nil
	}
	g.empty++
	if g.empty == 1 {
		return emptyNotice, nil
	}
	return "", emptyFailure
}

func (g *guard) room(count int) bool { return count >= 0 && g.used+count <= g.max }

func (g *guard) limit() error { return fmt.Errorf("limit: %d-step budget reached", g.max) }

func signatures(calls []model.Call) ([]string, error) {
	result := make([]string, len(calls))
	for index, call := range calls {
		args := []byte("{}")
		var err error
		if len(call.Args) != 0 {
			args, err = json.Marshal(call.Args)
		}
		if err != nil {
			return nil, fmt.Errorf("encode tool call %s: %w", call.Name, err)
		}
		result[index] = call.Name + "\x00" + string(args)
	}
	return result, nil
}

type modelReply struct {
	text  string
	calls []model.Call
}

func collectReply(
	ctx context.Context,
	provider model.Provider,
	key string,
	request model.Request,
	emit func(string) error,
) (modelReply, error) {
	var reply modelReply
	var text strings.Builder
	seen := make(map[string]bool)
	err := provider.Stream(ctx, key, request, func(value model.Chunk) error {
		text.WriteString(value.Text)
		if value.Text != "" && emit != nil {
			if err := emit(value.Text); err != nil {
				return err
			}
		}
		if value.Call != nil {
			identity, err := json.Marshal(value.Call)
			if err != nil {
				return fmt.Errorf("encode tool call: %w", err)
			}
			if !seen[string(identity)] {
				seen[string(identity)] = true
				reply.calls = append(reply.calls, *value.Call)
			}
		}
		return nil
	})
	reply.text = text.String()
	return reply, err
}
