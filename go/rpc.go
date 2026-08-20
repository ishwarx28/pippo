// Owns the authenticated websocket and concurrent JSON-RPC dispatch.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"pippo/go/model"
)

const maxMessage = 1 << 20

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type remoteError struct {
	method string
	*rpcError
}

func (e *remoteError) Error() string {
	return fmt.Sprintf("%s: %s (%d)", e.method, e.Message, e.Code)
}

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *uint64         `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type handler func(context.Context, *rpc, json.RawMessage) (any, error)

type rpc struct {
	conn     *websocket.Conn
	handlers map[string]handler
	after    func(string)
	write    sync.Mutex
	pending  sync.Mutex
	waits    map[uint64]chan message
	next     atomic.Uint64
	done     chan struct{}
	once     sync.Once
}

func newRPC(conn *websocket.Conn, handlers map[string]handler) *rpc {
	conn.SetReadLimit(maxMessage)
	return &rpc{conn: conn, handlers: handlers, waits: make(map[uint64]chan message), done: make(chan struct{})}
}

func (r *rpc) serve(ctx context.Context) {
	defer r.close()
	for {
		var input message
		if err := r.conn.ReadJSON(&input); err != nil {
			return
		}
		go r.dispatch(ctx, input)
	}
}

func (r *rpc) dispatch(ctx context.Context, input message) {
	if input.JSONRPC != "2.0" {
		if input.ID != nil {
			r.respond(*input.ID, nil, &rpcError{Code: -32600, Message: "invalid request"})
		}
		return
	}
	if input.Method == "" {
		if input.ID == nil {
			return
		}
		r.pending.Lock()
		wait := r.waits[*input.ID]
		delete(r.waits, *input.ID)
		r.pending.Unlock()
		if wait != nil {
			wait <- input
		}
		return
	}

	handle := r.handlers[input.Method]
	if handle == nil {
		if input.ID != nil {
			r.respond(*input.ID, nil, &rpcError{Code: -32601, Message: "method not found"})
		}
		return
	}
	result, err := handle(ctx, r, input.Params)
	if input.ID == nil {
		return
	}
	if err != nil {
		r.respond(*input.ID, nil, &rpcError{Code: -32603, Message: err.Error()})
		return
	}
	r.respond(*input.ID, result, nil)
	if r.after != nil {
		r.after(input.Method)
	}
}

func (r *rpc) call(ctx context.Context, method string, params, result any) error {
	id := r.next.Add(1)
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode %s parameters: %w", method, err)
	}
	wait := make(chan message, 1)
	r.pending.Lock()
	r.waits[id] = wait
	r.pending.Unlock()

	if err := r.send(message{JSONRPC: "2.0", ID: &id, Method: method, Params: raw}); err != nil {
		r.forget(id)
		return err
	}
	select {
	case output := <-wait:
		if output.Error != nil {
			return &remoteError{method: method, rpcError: output.Error}
		}
		if result != nil && len(output.Result) != 0 {
			if err := json.Unmarshal(output.Result, result); err != nil {
				return fmt.Errorf("decode %s result: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		r.forget(id)
		return fmt.Errorf("%s: %w", method, ctx.Err())
	case <-r.done:
		r.forget(id)
		return fmt.Errorf("%s: connection closed", method)
	}
}

func (r *rpc) notify(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode %s parameters: %w", method, err)
	}
	return r.send(message{JSONRPC: "2.0", Method: method, Params: raw})
}

func (r *rpc) respond(id uint64, result any, failure *rpcError) {
	var raw json.RawMessage
	if failure == nil {
		var err error
		raw, err = json.Marshal(result)
		if err != nil {
			failure = &rpcError{Code: -32603, Message: "encode response"}
		}
	}
	if err := r.send(message{JSONRPC: "2.0", ID: &id, Result: raw, Error: failure}); err != nil {
		r.close()
	}
}

func (r *rpc) send(output message) error {
	r.write.Lock()
	defer r.write.Unlock()
	if err := r.conn.WriteJSON(output); err != nil {
		return fmt.Errorf("write rpc message: %w", err)
	}
	return nil
}

func (r *rpc) forget(id uint64) {
	r.pending.Lock()
	delete(r.waits, id)
	r.pending.Unlock()
}

func (r *rpc) close() {
	r.once.Do(func() {
		close(r.done)
		if err := r.conn.Close(); err != nil && !errors.Is(err, websocket.ErrCloseSent) {
			log.Printf("close rpc websocket: %v", err)
		}
	})
}

type paths struct {
	Runtime string `json:"runtime"`
	Cache   string `json:"cache"`
	Agent   string `json:"agent"`
}

type platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

type hello struct {
	Paths    paths           `json:"paths"`
	Platform platform        `json:"platform"`
	Settings json.RawMessage `json:"settings"`
	Preset   json.RawMessage `json:"preset"`
}

type state struct {
	mu       sync.RWMutex
	hello    *hello
	peer     *rpc
	loop     *loop
	stop     chan struct{}
	ack      chan struct{}
	halt     sync.Once
	ackClose sync.Once
}

func newState(provider model.Provider) *state {
	return &state{
		loop: newLoop(provider),
		stop: make(chan struct{}),
		ack:  make(chan struct{}),
	}
}

func (s *state) attach(peer *rpc) {
	s.mu.Lock()
	s.peer = peer
	s.mu.Unlock()
}

func (s *state) detach(peer *rpc) {
	s.mu.Lock()
	detached := false
	if s.peer == peer {
		s.peer = nil
		detached = true
	}
	s.mu.Unlock()
	if detached && s.loop != nil {
		s.loop.agents.interrupt(peer)
	}
}

func (s *state) set(value hello) {
	s.mu.Lock()
	s.hello = &value
	s.mu.Unlock()
}

func (s *state) ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hello != nil
}

func (s *state) connection() *rpc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peer
}

func (s *state) startup() *hello {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.hello == nil {
		return nil
	}
	value := *s.hello
	value.Settings = append(json.RawMessage(nil), s.hello.Settings...)
	value.Preset = append(json.RawMessage(nil), s.hello.Preset...)
	return &value
}

func (s *state) beginShutdown() bool {
	started := false
	s.halt.Do(func() {
		started = true
		go func() {
			if s.loop != nil {
				s.loop.stop()
			}
			<-s.ack
			close(s.stop)
		}()
	})
	return started
}

func (s *state) responseSent(method string) {
	if method == "shutdown" {
		s.ackClose.Do(func() { close(s.ack) })
	}
}

func routes(guard *auth, state *state) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !websocket.IsWebSocketUpgrade(request) {
			response.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		value := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		if !guard.claim(value) {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(response, request, nil)
		if err != nil {
			return
		}
		peer := newRPC(conn, handlers(state))
		peer.after = state.responseSent
		state.attach(peer)
		defer state.detach(peer)
		peer.serve(request.Context())
	})
	return mux
}

func handlers(state *state) map[string]handler {
	return map[string]handler{
		"hello": func(ctx context.Context, peer *rpc, raw json.RawMessage) (any, error) {
			var value hello
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, fmt.Errorf("decode hello: %w", err)
			}
			if value.Paths.Runtime == "" || value.Paths.Cache == "" || value.Paths.Agent == "" ||
				value.Platform.OS == "" || value.Platform.Arch == "" || len(value.Settings) == 0 {
				return nil, errors.New("hello is incomplete")
			}
			var pong struct {
				Ready bool `json:"ready"`
			}
			if err := peer.call(ctx, "runtime.ping", struct{}{}, &pong); err != nil {
				return nil, fmt.Errorf("verify runtime rpc: %w", err)
			}
			if !pong.Ready {
				return nil, errors.New("runtime is not ready")
			}
			state.set(value)
			return map[string]bool{"ready": true}, nil
		},
		"health": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			return map[string]bool{"ready": state.ready()}, nil
		},
		"shutdown": func(_ context.Context, _ *rpc, _ json.RawMessage) (any, error) {
			return map[string]bool{"accepted": state.beginShutdown()}, nil
		},
		"run.resume": func(_ context.Context, _ *rpc, raw json.RawMessage) (any, error) {
			var input struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(raw, &input); err != nil || input.RunID == "" {
				return nil, errors.New("run id is required")
			}
			if state.loop == nil {
				return nil, errors.New("model loop is not ready")
			}
			return state.loop.agents.reopen(input.RunID)
		},
		"turn.start":  startTurn(state),
		"turn.cancel": cancelTurn(state),
	}
}
