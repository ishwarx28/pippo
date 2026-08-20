// Owns bootstrap boundary checks that leave no listener behind.
package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const validToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestTokenIsClaimedOnce(t *testing.T) {
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	guard := &auth{token: token}
	if !guard.claim(validToken) {
		t.Fatal("valid token was rejected")
	}
	if guard.claim(validToken) {
		t.Fatal("token was accepted twice")
	}
}

func TestWebsocketAuthenticatesOnlyOnce(t *testing.T) {
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(routes(&auth{token: token}, &state{}))
	defer server.Close()
	header := http.Header{"Authorization": []string{"Bearer " + validToken}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(server.URL), header)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	_, response, err := websocket.DefaultDialer.Dial(wsURL(server.URL), header)
	if err == nil {
		t.Fatal("startup token was accepted twice")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("second response = %#v", response)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsNonLoopbackBeforeListening(t *testing.T) {
	err := run([]string{"--listen", "0.0.0.0:0"}, strings.NewReader(validToken+"\n"), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("error = %v", err)
	}
}

func TestProtocolShutdownStopsRun(t *testing.T) {
	readyRead, readyWrite := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- run(
			[]string{"--listen", "127.0.0.1:0"},
			strings.NewReader(validToken+"\n"),
			readyWrite,
		)
	}()
	address, err := bufio.NewReader(readyRead).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	client := dialRPC(t, "http://"+strings.TrimSpace(address), validToken)
	var reply struct {
		Accepted bool `json:"accepted"`
	}
	if err := client.call(context.Background(), "shutdown", struct{}{}, &reply); err != nil {
		t.Fatal(err)
	}
	if !reply.Accepted {
		t.Fatal("shutdown was not accepted")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("service did not stop after protocol shutdown")
	}
	client.close()
	if err := readyRead.Close(); err != nil {
		t.Fatal(err)
	}
	if err := readyWrite.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadTokenRejectsMalformedInput(t *testing.T) {
	for _, input := range []string{"", "abc\n", validToken + "00\n"} {
		if _, err := readToken(bytes.NewBufferString(input)); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func wsURL(address string) string {
	return "ws" + strings.TrimPrefix(address, "http") + "/rpc"
}
