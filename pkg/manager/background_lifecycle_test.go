package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func TestStopCancelsAndJoinsPreWarmBeforeCleanup(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	client := &qmi.Client{}
	runCtx, runCancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.client = client
	m.ctx = runCtx
	m.cancel = runCancel
	m.lifetimeActive = true
	m.coreReady = true
	m.controlReady = true
	m.state = StateDisconnected
	m.mu.Unlock()
	m.coreGeneration.Store(1)
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }
	m.snapshot.UpdateIdentities(DeviceIdentities{IMEI: "existing-imei"})

	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	m.getICCIDStrictHook = func(ctx context.Context) (string, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return "", ctx.Err()
	}

	m.PreWarmIdentities(false)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("PreWarmIdentities did not enter the managed query")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the pre-warm lifetime context")
	}
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before pre-warm exited: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after pre-warm exited")
	}

	ids, _ := m.snapshot.Identities()
	if ids.IMEI != "existing-imei" || ids.ICCID != "" {
		t.Fatalf("post-stop identities = %+v, want original hardware identity and no stale SIM write", ids)
	}
	if !errors.Is(runCtx.Err(), context.Canceled) {
		t.Fatalf("manager lifetime context error = %v, want context.Canceled", runCtx.Err())
	}
}

func TestBackgroundAdmissionRejectsStoppedManager(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	called := false
	if launched := m.launchCoreBackgroundTask(func(context.Context, coreSessionToken) { called = true }); launched {
		t.Fatal("background task admitted after Stop")
	}
	if called {
		t.Fatal("background task ran after Stop")
	}
}
