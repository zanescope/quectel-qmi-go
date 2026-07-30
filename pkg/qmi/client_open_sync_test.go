package qmi

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestClientRawOpenRunsInitialSyncByDefault(t *testing.T) {
	const devicePath = "/dev/cdc-wdm-raw-default"

	errCh := withRawTransportForTest(t, devicePath, func(conn net.Conn) error {
		return serveRawOpenHandshake(conn, true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClientWithOptions(ctx, devicePath, ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}
	defer client.Close()

	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestClientRawOpenCanExplicitlyDisableInitialSync(t *testing.T) {
	const devicePath = "/dev/cdc-wdm-raw-no-sync"

	errCh := withRawTransportForTest(t, devicePath, func(conn net.Conn) error {
		return serveRawOpenHandshake(conn, false)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClientWithOptions(ctx, devicePath, ClientOptions{
		DisableSyncOnOpen: true,
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}
	defer client.Close()

	if client.opts.SyncOnOpen {
		t.Fatal("client.opts.SyncOnOpen=true, want false")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestClientProxyDeviceOpenFallbackToRawRunsInitialSync(t *testing.T) {
	const devicePath = "/dev/cdc-wdm-proxy-fallback"

	proxyErrCh := make(chan error, 1)
	restoreProxy := replaceProxyTransportForTest(t, func(context.Context, ClientOptions) (qmiTransport, error) {
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			openReq, err := readQMIFrameFromConn(serverConn)
			if err != nil {
				proxyErrCh <- err
				return
			}
			if err := assertCTLRequest(openReq, CTLInternalProxyOpen); err != nil {
				proxyErrCh <- err
				return
			}
			proxyErrCh <- writeCTLResponse(serverConn, openReq, []TLV{
				{Type: 0x02, Value: []byte{0x01, 0x00, 0x01, 0x00}},
			})
		}()
		return clientConn, nil
	})
	defer restoreProxy()

	rawErrCh := withRawTransportForTest(t, devicePath, func(conn net.Conn) error {
		return serveRawOpenHandshake(conn, true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClientWithOptions(ctx, devicePath, ClientOptions{
		UseProxy:           true,
		ProxyFallbackToRaw: true,
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}
	defer client.Close()

	if client.opts.UseProxy {
		t.Fatal("client.opts.UseProxy=true after fallback, want false")
	}
	if !client.opts.SyncOnOpen {
		t.Fatal("client.opts.SyncOnOpen=false after raw fallback, want true")
	}
	if err := <-proxyErrCh; err != nil {
		t.Fatal(err)
	}
	if err := <-rawErrCh; err != nil {
		t.Fatal(err)
	}
}

func withRawTransportForTest(t *testing.T, wantPath string, server func(net.Conn) error) <-chan error {
	t.Helper()

	errCh := make(chan error, 1)
	restore := replaceRawTransportForTest(t, func(path string) (qmiTransport, error) {
		if path != wantPath {
			return nil, fmt.Errorf("raw path = %q, want %q", path, wantPath)
		}
		clientConn, serverConn := net.Pipe()
		t.Cleanup(func() {
			_ = serverConn.Close()
		})
		go func() {
			errCh <- server(serverConn)
		}()
		return clientConn, nil
	})
	t.Cleanup(restore)
	return errCh
}

func serveRawOpenHandshake(conn net.Conn, wantSync bool) error {
	request, err := readQMIFrameFromConn(conn)
	if err != nil {
		return err
	}
	if wantSync {
		if err := assertCTLRequest(request, CTLSync); err != nil {
			return err
		}
		if err := writeCTLSuccess(conn, request); err != nil {
			return err
		}
		request, err = readQMIFrameFromConn(conn)
		if err != nil {
			return err
		}
	}
	if err := assertCTLRequest(request, CTLGetVersionInfo); err != nil {
		return err
	}
	return writeCTLResponse(conn, request, []TLV{
		successTLV(),
		{Type: 0x01, Value: []byte{0}},
	})
}
