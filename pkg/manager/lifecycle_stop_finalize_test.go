package manager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStopWinsBeforeStartupFinalPublication(t *testing.T) {
	m := New(Config{NoDial: true}, NewNopLogger())
	m.openClientAndAllocateServicesHook = func(context.Context) error { return nil }

	enteredSIMCheck := make(chan struct{})
	releaseSIMCheck := make(chan struct{})
	m.checkSIMHook = func() error {
		close(enteredSIMCheck)
		<-releaseSIMCheck
		return nil
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- m.StartCoreContext(context.Background())
	}()

	select {
	case <-enteredSIMCheck:
	case <-time.After(time.Second):
		t.Fatal("start did not reach the finalization window")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	deadline := time.Now().Add(time.Second)
	for m.State() != StateStopping && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.State(); got != StateStopping {
		t.Fatalf("state = %s, want stopping before releasing startup", got)
	}
	close(releaseSIMCheck)

	select {
	case err := <-startDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StartCoreContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartCoreContext did not abort final publication")
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}

	if m.IsCoreReady() {
		t.Fatal("core readiness resurrected after Stop won startup finalization")
	}
	if got := m.State(); got != StateDisconnected {
		t.Fatalf("state = %s, want disconnected", got)
	}
}
