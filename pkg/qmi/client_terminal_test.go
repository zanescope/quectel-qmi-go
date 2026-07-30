package qmi

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestClientReadEOFTerminatesPendingRequest(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	client := newClientWithTransport("net-pipe", DefaultClientOptions(), clientConn)
	t.Cleanup(func() {
		_ = peerConn.Close()
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	requestErrCh := make(chan error, 1)
	go func() {
		_, err := client.SendRequest(ctx, ServiceDMS, 1, DMSGetDeviceRevID, nil)
		requestErrCh <- err
	}()

	if _, err := readQMIFrameFromConn(peerConn); err != nil {
		t.Fatalf("read request frame: %v", err)
	}
	if err := peerConn.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}

	select {
	case err := <-requestErrCh:
		assertTransportError(t, err, TransportOperationRead, io.EOF)
	case <-time.After(2 * time.Second):
		t.Fatal("pending request did not receive terminal read error")
	}

	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed after EOF")
	}
	assertTransportError(t, client.Err(), TransportOperationRead, io.EOF)
	assertEventsClosed(t, client.Events())
}

func TestClientReadResponseWithEOFPreservesResponse(t *testing.T) {
	transport := newResponseEOFTransport()
	client := newClientWithTransport("response-eof", DefaultClientOptions(), transport)
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.SendRequest(ctx, ServiceDMS, 1, DMSGetDeviceRevID, nil)
	if err != nil {
		t.Fatalf("SendRequest() error = %v, want response returned with EOF", err)
	}
	if resp == nil {
		t.Fatal("SendRequest() response = nil")
	}
	if resp.ServiceType != ServiceDMS || resp.MessageID != DMSGetDeviceRevID {
		t.Fatalf("response = service 0x%02x message 0x%04x, want DMS/0x%04x",
			resp.ServiceType, resp.MessageID, DMSGetDeviceRevID)
	}

	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed after response accompanied by EOF")
	}
	assertTransportError(t, client.Err(), TransportOperationRead, io.EOF)
	assertEventsClosed(t, client.Events())
}

func TestClientTerminalLogCallbackCanCloseClient(t *testing.T) {
	clientReady := make(chan *Client, 1)
	logCloseErrCh := make(chan error, 1)
	var terminalLogOnce sync.Once

	opts := DefaultClientOptions()
	opts.Logf = func(level ClientLogLevel, _ string, _ ...any) {
		if level != ClientLogLevelWarn {
			return
		}
		terminalLogOnce.Do(func() {
			client := <-clientReady
			logCloseErrCh <- client.Close()
		})
	}

	clientConn, peerConn := net.Pipe()
	client := newClientWithTransport("reentrant-terminal-log", opts, clientConn)
	clientReady <- client
	t.Cleanup(func() {
		_ = peerConn.Close()
		_ = client.Close()
	})

	if err := peerConn.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}

	select {
	case err := <-logCloseErrCh:
		if err != nil {
			t.Fatalf("Close() from Logf error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close() from terminal Logf deadlocked waiting for readLoop finalization")
	}

	select {
	case <-client.Done():
	default:
		t.Fatal("Done remains open after terminal Logf closed the client")
	}
	assertTransportError(t, client.Err(), TransportOperationRead, io.EOF)
	assertEventsClosed(t, client.Events())
}

func TestClientWriteEPIPEFailsAllPendingRequests(t *testing.T) {
	transport := newGatedWriteErrorTransport()
	opts := DefaultClientOptions()
	opts.TxQueueSize = 8
	client := newClientWithTransport("epipe", opts, transport)
	t.Cleanup(func() {
		_ = client.Close()
	})

	const requestCount = 4
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	requestErrCh := make(chan error, requestCount)
	for i := 0; i < requestCount; i++ {
		go func(msgID uint16) {
			_, err := client.SendRequest(ctx, ServiceDMS, 1, msgID, nil)
			requestErrCh <- err
		}(DMSGetDeviceRevID + uint16(i))
	}

	select {
	case <-transport.startedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not reach the injected EPIPE")
	}
	waitForPendingTransactions(t, client, requestCount)
	close(transport.releaseWrite)

	for i := 0; i < requestCount; i++ {
		select {
		case err := <-requestErrCh:
			assertTransportError(t, err, TransportOperationWrite, syscall.EPIPE)
		case <-time.After(2 * time.Second):
			t.Fatalf("pending request %d did not receive terminal write error", i)
		}
	}

	assertTransportError(t, client.Err(), TransportOperationWrite, syscall.EPIPE)
	assertEventsClosed(t, client.Events())

	client.mu.Lock()
	pending := len(client.transactions)
	client.mu.Unlock()
	if pending != 0 {
		t.Fatalf("terminal write left %d pending transaction(s)", pending)
	}
}

func TestClientExplicitCloseHasNilErrAndRejectsFutureRequests(t *testing.T) {
	clientConn, peerConn := net.Pipe()
	client := newClientWithTransport("explicit-close", DefaultClientOptions(), clientConn)
	defer peerConn.Close()

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("Err() after explicit Close = %v, want nil", err)
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("Done remains open after explicit Close")
	}
	assertEventsClosed(t, client.Events())

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.SendRequest(canceledCtx, ServiceDMS, 1, DMSGetDeviceRevID, nil)
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("SendRequest() after Close error = %v, want ErrClientClosed", err)
	}
}

func TestClientCloseEOFRacePublishesOneConsistentTerminalState(t *testing.T) {
	const attempts = 64
	for i := 0; i < attempts; i++ {
		clientConn, peerConn := net.Pipe()
		client := newClientWithTransport("close-eof-race", DefaultClientOptions(), clientConn)

		start := make(chan struct{})
		closeErrCh := make(chan error, 1)
		peerErrCh := make(chan error, 1)
		go func() {
			<-start
			closeErrCh <- client.Close()
		}()
		go func() {
			<-start
			peerErrCh <- peerConn.Close()
		}()
		close(start)

		if err := <-closeErrCh; err != nil {
			t.Fatalf("attempt %d: Close() error = %v", i, err)
		}
		if err := <-peerErrCh; err != nil {
			t.Fatalf("attempt %d: peer Close() error = %v", i, err)
		}
		select {
		case <-client.Done():
		default:
			t.Fatalf("attempt %d: Done remains open", i)
		}
		assertEventsClosed(t, client.Events())

		terminalErr := client.Err()
		_, requestErr := client.SendRequest(context.Background(), ServiceDMS, 1, DMSGetDeviceRevID, nil)
		if terminalErr == nil {
			if !errors.Is(requestErr, ErrClientClosed) {
				t.Fatalf("attempt %d: request error = %v, want ErrClientClosed", i, requestErr)
			}
			continue
		}
		assertTransportError(t, terminalErr, TransportOperationRead, nil)
		assertTransportError(t, requestErr, TransportOperationRead, nil)
	}
}

func TestNewClientRejectsTransportThatTerminatesDuringOpen(t *testing.T) {
	const devicePath = "/dev/cdc-wdm-terminal"
	restore := replaceRawTransportForTest(t, func(path string) (qmiTransport, error) {
		if path != devicePath {
			t.Fatalf("raw transport path = %q, want %q", path, devicePath)
		}
		return newImmediateEOFTransport(), nil
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := NewClientWithOptions(ctx, devicePath, ClientOptions{})
	if client != nil {
		_ = client.Close()
		t.Fatal("NewClientWithOptions() returned a terminal client")
	}
	assertTransportError(t, err, TransportOperationRead, io.EOF)
}

func assertTransportError(t *testing.T, err error, operation TransportOperation, cause error) {
	t.Helper()
	if !IsTransportError(err) {
		t.Fatalf("error = %v, want TransportError", err)
	}
	transportErr := GetTransportError(err)
	if transportErr == nil {
		t.Fatalf("GetTransportError(%v) = nil", err)
	}
	if transportErr.Operation != operation {
		t.Fatalf("transport operation = %q, want %q", transportErr.Operation, operation)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("error = %v, want errors.Is(..., %v)", err, cause)
	}
}

func assertEventsClosed(t *testing.T, events <-chan Event) {
	t.Helper()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("Events yielded a value after terminal shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Events was not closed after runtime loops exited")
	}
}

func waitForPendingTransactions(t *testing.T, client *Client, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		got := len(client.transactions)
		client.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	client.mu.Lock()
	got := len(client.transactions)
	client.mu.Unlock()
	t.Fatalf("pending transaction count = %d, want %d", got, want)
}

type responseEOFTransport struct {
	frameCh   chan []byte
	closeOnce sync.Once
	closedCh  chan struct{}
}

func newResponseEOFTransport() *responseEOFTransport {
	return &responseEOFTransport{
		frameCh:  make(chan []byte, 1),
		closedCh: make(chan struct{}),
	}
}

func (t *responseEOFTransport) Read(p []byte) (int, error) {
	select {
	case frame := <-t.frameCh:
		return copy(p, frame), io.EOF
	case <-t.closedCh:
		return 0, io.EOF
	}
}

func (t *responseEOFTransport) Write(p []byte) (int, error) {
	frame := append([]byte(nil), p...)
	select {
	case t.frameCh <- frame:
		return len(p), nil
	case <-t.closedCh:
		return 0, io.ErrClosedPipe
	}
}

func (t *responseEOFTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closedCh)
	})
	return nil
}

func (t *responseEOFTransport) SetReadDeadline(time.Time) error {
	return nil
}

type gatedWriteErrorTransport struct {
	writeStarted sync.Once
	startedCh    chan struct{}
	releaseWrite chan struct{}
	closeOnce    sync.Once
	closedCh     chan struct{}
}

func newGatedWriteErrorTransport() *gatedWriteErrorTransport {
	return &gatedWriteErrorTransport{
		startedCh:    make(chan struct{}),
		releaseWrite: make(chan struct{}),
		closedCh:     make(chan struct{}),
	}
}

func (t *gatedWriteErrorTransport) Read([]byte) (int, error) {
	<-t.closedCh
	return 0, io.EOF
}

func (t *gatedWriteErrorTransport) Write([]byte) (int, error) {
	t.writeStarted.Do(func() {
		close(t.startedCh)
	})
	<-t.releaseWrite
	return 0, syscall.EPIPE
}

func (t *gatedWriteErrorTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closedCh)
	})
	return nil
}

func (t *gatedWriteErrorTransport) SetReadDeadline(time.Time) error {
	return nil
}

type immediateEOFTransport struct {
	closeOnce sync.Once
	closedCh  chan struct{}
}

func newImmediateEOFTransport() *immediateEOFTransport {
	return &immediateEOFTransport{closedCh: make(chan struct{})}
}

func (t *immediateEOFTransport) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (t *immediateEOFTransport) Write(p []byte) (int, error) {
	select {
	case <-t.closedCh:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}

func (t *immediateEOFTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closedCh)
	})
	return nil
}

func (t *immediateEOFTransport) SetReadDeadline(time.Time) error {
	return nil
}
