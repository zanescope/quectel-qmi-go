package manager

import (
	"context"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func TestStopJoinsCanceledCleanupBeforeClosingTransport(t *testing.T) {
	m := New(Config{Timeouts: TimeoutConfig{Stop: 20 * time.Millisecond}}, NewNopLogger())
	client := &qmi.Client{}
	wds := &qmi.WDSService{}
	runCtx, runCancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.client = client
	m.wds = wds
	m.handleV4 = 7
	m.ctx = runCtx
	m.cancel = runCancel
	m.lifetimeActive = true
	m.coreReady = true
	m.controlReady = true
	m.state = StateConnected
	m.mu.Unlock()
	m.coreGeneration.Store(1)

	canceled := make(chan struct{})
	release := make(chan struct{})
	workerExited := make(chan struct{})
	m.stopWDSForCleanup = func(ctx context.Context, got *qmi.WDSService, handle uint32) error {
		if got != wds || handle != 7 {
			t.Errorf("cleanup target = (%p, %d), want (%p, 7)", got, handle, wds)
		}
		<-ctx.Done()
		close(canceled)
		<-release
		close(workerExited)
		return ctx.Err()
	}
	assertWorkerExited := func() {
		select {
		case <-workerExited:
		default:
			t.Error("transport/service closed before cleanup worker exited")
		}
	}
	m.closeWDSService = func(*qmi.WDSService) error {
		assertWorkerExited()
		return nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error {
		assertWorkerExited()
		return nil
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not receive Stop deadline")
	}
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned with orphaned cleanup worker: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after cleanup worker exited")
	}
}
