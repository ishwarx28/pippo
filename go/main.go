// Owns child bootstrap, its loopback socket and startup authentication.
package main

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"pippo/go/model"
)

const shutdownTimeout = 3 * time.Second

type auth struct {
	mu    sync.Mutex
	token []byte
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, ready io.Writer) (result error) {
	flags := flag.NewFlagSet("pippod", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("listen", "", "IPv4 loopback address")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse arguments: %w", err)
	}
	if *address == "" || flags.NArg() != 0 {
		return errors.New("listen address is required")
	}
	resolved, err := net.ResolveTCPAddr("tcp4", *address)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}
	if resolved.IP == nil || !resolved.IP.IsLoopback() {
		return errors.New("listen address must be IPv4 loopback")
	}
	token, err := readToken(stdin)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp4", resolved)
	if err != nil {
		return fmt.Errorf("listen on loopback: %w", err)
	}
	defer func() {
		if err := listener.Close(); result == nil && err != nil && !errors.Is(err, net.ErrClosed) {
			result = fmt.Errorf("close loopback listener: %w", err)
		}
	}()

	state := newState(model.Gemini{})
	server := &http.Server{Handler: routes(&auth{token: token}, state)}
	if _, err := fmt.Fprintln(ready, listener.Addr().String()); err != nil {
		return fmt.Errorf("report listen address: %w", err)
	}
	watch, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		select {
		case <-state.stop:
			ctx, stop := context.WithTimeout(context.Background(), shutdownTimeout)
			err := server.Shutdown(ctx)
			stop()
			stopped <- err
		case <-watch.Done():
			stopped <- nil
		}
	}()
	serveErr := server.Serve(listener)
	cancel()
	stopErr := <-stopped
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve loopback: %w", serveErr)
	}
	if stopErr != nil {
		return fmt.Errorf("shut down loopback: %w", stopErr)
	}
	return nil
}

func readToken(input io.Reader) ([]byte, error) {
	line, err := bufio.NewReader(io.LimitReader(input, 66)).ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read startup token: %w", err)
	}
	token, err := hex.DecodeString(strings.TrimSuffix(line, "\n"))
	if err != nil || len(token) != 32 {
		return nil, errors.New("startup token must be 32 bytes of hex")
	}
	return token, nil
}

func (guard *auth) claim(value string) bool {
	presented, err := hex.DecodeString(value)
	guard.mu.Lock()
	defer guard.mu.Unlock()
	valid := err == nil && len(guard.token) == 32 && len(presented) == len(guard.token) &&
		subtle.ConstantTimeCompare(presented, guard.token) == 1
	if valid {
		clear(guard.token)
		guard.token = nil
	}
	return valid
}
