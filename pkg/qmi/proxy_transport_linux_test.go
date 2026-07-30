//go:build linux

package qmi

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxySocketAddressNormalizesCommonAbstractSocketNames(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default", in: "", want: "\x00qmi-proxy"},
		{name: "plain", in: "qmi-proxy", want: "\x00qmi-proxy"},
		{name: "at prefix", in: "@qmi-proxy", want: "\x00qmi-proxy"},
		{name: "nul prefix", in: "\x00qmi-proxy", want: "\x00qmi-proxy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := proxySocketAddress(tt.in); got != tt.want {
				t.Fatalf("proxySocketAddress(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestOpenProxyTransportRetriesUntilContextDeadline(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		proxyRetryDelay = oldRetryDelay
	})

	attempts := 0
	starts := 0
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		attempts++
		return nil, errors.New("proxy socket not ready")
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	proxyRetryDelay = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := openProxyTransport(ctx, ClientOptions{
		ProxyPath:       "@qmi-proxy",
		ProxyExecutable: proxyExecutable,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("openProxyTransport() error=%v, want context deadline exceeded", err)
	}
	if starts != 1 {
		t.Fatalf("start attempts=%d, want 1", starts)
	}
	if attempts < 3 {
		t.Fatalf("dial attempts=%d, want at least 3 retries before deadline", attempts)
	}
}

func TestOpenProxyTransportRetriesUntilProxyIsReady(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		proxyRetryDelay = oldRetryDelay
	})

	attempts := 0
	starts := 0
	var serverConn net.Conn
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		attempts++
		if attempts < 4 {
			return nil, errors.New("proxy socket not ready")
		}
		clientConn, conn := net.Pipe()
		serverConn = conn
		return clientConn, nil
	}
	startProxyProcessHook = func(string) error {
		starts++
		return nil
	}
	proxyRetryDelay = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := openProxyTransport(ctx, ClientOptions{
		ProxyPath:       "\x00qmi-proxy",
		ProxyExecutable: proxyExecutable,
	})
	if err != nil {
		t.Fatalf("openProxyTransport() error=%v", err)
	}
	defer conn.Close()
	if serverConn != nil {
		defer serverConn.Close()
	}
	if starts != 1 {
		t.Fatalf("start attempts=%d, want 1", starts)
	}
	if attempts != 4 {
		t.Fatalf("dial attempts=%d, want 4", attempts)
	}
}

type cancelBetweenGateChecksContext struct {
	context.Context
	checks atomic.Int32
}

func (c *cancelBetweenGateChecksContext) Done() <-chan struct{} {
	return nil
}

func (c *cancelBetweenGateChecksContext) Err() error {
	if c.checks.Add(1) == 1 {
		return nil
	}
	return context.Canceled
}

func TestAcquireProxyStartGateReturnsTokenWhenCancellationWinsAfterReceipt(t *testing.T) {
	ctx := &cancelBetweenGateChecksContext{Context: context.Background()}
	if err := acquireProxyStartGate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquireProxyStartGate() error=%v, want context canceled", err)
	}

	select {
	case <-proxyStartGate:
		releaseProxyStartGate()
	default:
		t.Fatal("proxy start gate token was not returned after cancellation")
	}
}

func TestOpenProxyTransportGateWaitIsCancelable(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
	})

	select {
	case <-proxyStartGate:
		t.Cleanup(releaseProxyStartGate)
	default:
		t.Fatal("proxy start gate is unexpectedly held")
	}

	firstDialDone := make(chan struct{})
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		select {
		case <-firstDialDone:
		default:
			close(firstDialDone)
		}
		return nil, errors.New("proxy socket not ready")
	}
	startProxyProcessHook = func(string) error {
		t.Error("startProxyProcessHook called while gate was held")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := openProxyTransport(ctx, ClientOptions{
			ProxyPath:       "@qmi-proxy",
			ProxyExecutable: proxyExecutable,
		})
		result <- err
	}()

	<-firstDialDone
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("openProxyTransport() error=%v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("openProxyTransport() did not return while the gate remained held")
	}
}

func TestOpenProxyTransportDoesNotStartProxyAfterSecondDialCancels(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialAttempts := 0
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		dialAttempts++
		if dialAttempts == 2 {
			cancel()
		}
		return nil, errors.New("proxy socket not ready")
	}
	startAttempts := 0
	startProxyProcessHook = func(string) error {
		startAttempts++
		return nil
	}

	_, err := openProxyTransport(ctx, ClientOptions{
		ProxyPath:       "@qmi-proxy",
		ProxyExecutable: proxyExecutable,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("openProxyTransport() error=%v, want context canceled", err)
	}
	if dialAttempts != 2 {
		t.Fatalf("dial attempts=%d, want 2", dialAttempts)
	}
	if startAttempts != 0 {
		t.Fatalf("proxy start attempts=%d, want 0 after caller cancellation", startAttempts)
	}
}

func TestOpenProxyTransportStartsSharedProxyOnce(t *testing.T) {
	proxyExecutable := filepath.Join(t.TempDir(), "qmi-proxy")
	if err := os.WriteFile(proxyExecutable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	oldDial := dialProxyHook
	oldStart := startProxyProcessHook
	oldRetryDelay := proxyRetryDelay
	t.Cleanup(func() {
		dialProxyHook = oldDial
		startProxyProcessHook = oldStart
		proxyRetryDelay = oldRetryDelay
	})

	const callers = 8
	var ready atomic.Bool
	var starts atomic.Int32
	var peersMu sync.Mutex
	var peers []net.Conn
	notReadyDialed := make(chan struct{}, callers+1)
	dialProxyHook = func(context.Context, string) (qmiTransport, error) {
		if !ready.Load() {
			notReadyDialed <- struct{}{}
			return nil, errors.New("proxy socket not ready")
		}
		client, server := net.Pipe()
		peersMu.Lock()
		peers = append(peers, server)
		peersMu.Unlock()
		return client, nil
	}

	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var signalStart sync.Once
	startProxyProcessHook = func(string) error {
		starts.Add(1)
		signalStart.Do(func() { close(startEntered) })
		<-releaseStart
		ready.Store(true)
		return nil
	}
	proxyRetryDelay = time.Millisecond

	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, err := openProxyTransport(ctx, ClientOptions{
				ProxyPath:       "@qmi-proxy",
				ProxyExecutable: proxyExecutable,
			})
			if conn != nil {
				_ = conn.Close()
			}
			errCh <- err
		}()
	}

	<-startEntered
	for i := 0; i < callers+1; i++ {
		<-notReadyDialed
	}
	close(releaseStart)
	for i := 0; i < callers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("openProxyTransport() error=%v", err)
		}
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("proxy starts=%d want 1", got)
	}
	peersMu.Lock()
	defer peersMu.Unlock()
	for _, peer := range peers {
		_ = peer.Close()
	}
}
