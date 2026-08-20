// Owns bootstrap boundary checks that leave no listener behind.
package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestRoutesAuthenticateBeforeUpgrade(t *testing.T) {
	token, err := readToken(strings.NewReader(validToken + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	handler := routes(&auth{token: token})

	request := httptest.NewRequest(http.MethodPost, "/rpc", nil)
	request.Header.Set("Authorization", "Bearer "+validToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("status = %d", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("second status = %d", response.Code)
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
