// Owns bootstrap boundary checks that leave no listener behind.
package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
