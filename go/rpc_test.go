// Checks bidirectional and concurrent RPC behavior over a real websocket.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRPCHandlesConcurrentCallsBothWays(t *testing.T) {
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	state := &state{}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	client := dialRPC(t, server.URL, validToken)
	defer client.close()

	settings, err := json.Marshal(map[string]any{"preset": "default", "sync": false})
	if err != nil {
		t.Fatal(err)
	}
	start := hello{
		Paths:    paths{Runtime: "/runtime", Cache: "/cache", Agent: "/agent"},
		Platform: platform{OS: "test", Arch: "test"},
		Settings: settings,
	}
	var ready struct {
		Ready bool `json:"ready"`
	}
	if err := client.call(context.Background(), "hello", start, &ready); err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || !state.ready() {
		t.Fatal("hello was not retained")
	}
	startup := state.startup()
	if startup == nil || startup.Paths.Runtime != "/runtime" || startup.Platform.OS != "test" ||
		!bytes.Contains(startup.Settings, []byte(`"preset":"default"`)) {
		t.Fatalf("startup = %#v", startup)
	}
	serverPeer := state.connection()
	if serverPeer == nil {
		t.Fatal("server peer is missing")
	}

	errs := make(chan error, 40)
	var calls sync.WaitGroup
	for index := 0; index < 20; index++ {
		calls.Add(1)
		go func(reverse bool) {
			defer calls.Done()
			var result struct {
				Ready bool `json:"ready"`
			}
			if reverse {
				errs <- serverPeer.call(context.Background(), "runtime.ping", struct{}{}, &result)
			} else {
				errs <- client.call(context.Background(), "health", struct{}{}, &result)
			}
			if !result.Ready {
				errs <- errors.New("call returned not ready")
			}
		}(index%2 == 0)
	}
	calls.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingRPCCallerUnblocksWhenConnectionCloses(t *testing.T) {
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	state := &state{}
	server := httptest.NewServer(routes(&auth{token: token}, state))
	defer server.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	client := dialRPCWith(t, server.URL, validToken, map[string]handler{
		"wait": func(context.Context, *rpc, json.RawMessage) (any, error) {
			close(started)
			<-release
			return map[string]bool{"done": true}, nil
		},
	})
	defer client.close()
	peer := state.connection()
	if peer == nil {
		t.Fatal("server peer is missing")
	}
	finished := make(chan error, 1)
	go func() {
		finished <- peer.call(context.Background(), "wait", struct{}{}, nil)
	}()
	<-started
	peer.close()
	select {
	case err := <-finished:
		if err == nil || !strings.Contains(err.Error(), "connection closed") {
			t.Fatalf("call error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending RPC caller stayed blocked")
	}
	close(release)
}

func dialRPC(t *testing.T, address, token string) *rpc {
	return dialRPCWith(t, address, token, map[string]handler{
		"runtime.ping": func(context.Context, *rpc, json.RawMessage) (any, error) {
			return map[string]bool{"ready": true}, nil
		},
	})
}

func dialRPCWith(t *testing.T, address, token string, handlers map[string]handler) *rpc {
	t.Helper()
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, response, err := websocket.DefaultDialer.Dial(wsURL(address), header)
	if err != nil {
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		t.Fatal(err)
	}
	client := newRPC(conn, handlers)
	go client.serve(context.Background())
	return client
}
