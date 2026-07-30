//go:build linux

package qmi

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

var (
	dialProxyHook         = dialProxy
	startProxyProcessHook = startProxyProcess
	proxyRetryDelay       = 100 * time.Millisecond
	proxyStartGate        = func() chan struct{} {
		gate := make(chan struct{}, 1)
		gate <- struct{}{}
		return gate
	}()
)

func openProxyTransport(ctx context.Context, opts ClientOptions) (qmiTransport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	proxyPath := opts.ProxyPath
	if proxyPath == "" {
		proxyPath = defaultProxyPath
	}
	proxyExecutable := opts.ProxyExecutable
	if proxyExecutable == "" {
		proxyExecutable = defaultProxyExecutable
	}

	conn, firstErr := dialProxyHook(ctx, proxyPath)
	if firstErr == nil {
		return conn, nil
	}

	if err := acquireProxyStartGate(ctx); err != nil {
		return nil, fmt.Errorf("wait to start qmi-proxy %q: %w", proxyPath, err)
	}
	defer releaseProxyStartGate()

	// Another caller may have started the shared proxy while this caller waited.
	conn, firstErr = dialProxyHook(ctx, proxyPath)
	if firstErr == nil {
		return conn, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect qmi-proxy %q after acquiring startup gate: %w", proxyPath, err)
	}

	if proxyExecutable == "" {
		return nil, fmt.Errorf("connect qmi-proxy %q: %w", proxyPath, firstErr)
	}
	if _, err := os.Stat(proxyExecutable); err != nil {
		return nil, fmt.Errorf("connect qmi-proxy %q failed: %w; proxy executable %s is unavailable: %v", proxyPath, firstErr, proxyExecutable, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect qmi-proxy %q before starting %s: %w", proxyPath, proxyExecutable, err)
	}
	if err := startProxyProcessHook(proxyExecutable); err != nil {
		return nil, fmt.Errorf("connect qmi-proxy %q failed and start %s failed: %w", proxyPath, proxyExecutable, err)
	}

	var lastErr error = firstErr
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("connect qmi-proxy %q after starting %s: last error: %v: %w", proxyPath, proxyExecutable, lastErr, err)
		}
		timer := time.NewTimer(proxyRetryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, fmt.Errorf("connect qmi-proxy %q after starting %s: last error: %v: %w", proxyPath, proxyExecutable, lastErr, ctx.Err())
		case <-timer.C:
		}
		conn, err := dialProxyHook(ctx, proxyPath)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
}

func acquireProxyStartGate(ctx context.Context) error {
	// Prefer an already-observed cancellation over a simultaneously available
	// gate token so a canceled caller never begins proxy startup work.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-proxyStartGate:
		if err := ctx.Err(); err != nil {
			releaseProxyStartGate()
			return err
		}
		return nil
	}
}

func releaseProxyStartGate() {
	proxyStartGate <- struct{}{}
}

func dialProxy(ctx context.Context, proxyPath string) (qmiTransport, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", proxySocketAddress(proxyPath))
}

func proxySocketAddress(proxyPath string) string {
	if proxyPath == "" {
		proxyPath = defaultProxyPath
	}
	if strings.HasPrefix(proxyPath, "\x00") {
		return proxyPath
	}
	if strings.HasPrefix(proxyPath, "@") {
		return "\x00" + strings.TrimPrefix(proxyPath, "@")
	}
	return "\x00" + proxyPath
}

func startProxyProcess(proxyExecutable string) error {
	cmd := exec.Command(proxyExecutable)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
