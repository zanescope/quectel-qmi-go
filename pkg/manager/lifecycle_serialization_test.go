package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartCoreContextIsCanceledByConcurrentStop(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	entered := make(chan struct{})
	m.openClientAndAllocateServicesHook = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- m.StartCoreContext(context.Background())
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("start did not reach the open boundary")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	select {
	case err := <-startDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StartCoreContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartCoreContext did not stop after Stop canceled the lifetime")
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after canceling a blocked open")
	}

	if m.IsCoreReady() {
		t.Fatal("core readiness resurrected after Stop")
	}
	if got := m.State(); got != StateDisconnected {
		t.Fatalf("state = %s, want disconnected", got)
	}
}

func TestStopCancelsBlockedRecovery(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.coreReady = true
	m.state = StateDisconnected
	m.mu.Unlock()
	m.coreGeneration.Store(1)
	request := recoveryRequest{kind: recoveryKindSoftware, reason: recoveryReasonExplicitRequest, detail: "test_stop", generation: 1}

	entered := make(chan struct{})
	m.openClientAndAllocateServicesHook = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}

	recoveryDone := make(chan bool, 1)
	go func() {
		recoveryDone <- m.doRecoverCore(request)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach the open boundary")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	select {
	case recovered := <-recoveryDone:
		if recovered {
			t.Fatal("recovery succeeded after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not observe manager cancellation")
	}

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop remained blocked behind recovery")
	}

	if m.IsCoreReady() {
		t.Fatal("core readiness resurrected after recovery cancellation")
	}
}

func TestOpenGuardRejectsRecoveryBeforeTransportOpen(t *testing.T) {
	guardErr := errors.New("foreign transport owner")
	var attempts []OpenAttempt
	m := New(Config{
		Device: ModemDevice{ControlPath: "not-opened"},
		OpenGuard: func(_ context.Context, attempt OpenAttempt) error {
			attempts = append(attempts, attempt)
			return guardErr
		},
	}, NewNopLogger())

	err := m.openClientAndAllocateServices(context.Background(), OpenReasonRecovery)
	if !errors.Is(err, guardErr) {
		t.Fatalf("open error = %v, want guard error", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("guard attempts = %d, want 1", len(attempts))
	}
	if got := attempts[0]; got.Phase != OpenPhaseBefore ||
		got.Reason != OpenReasonRecovery ||
		got.Attempt != 1 ||
		got.DevicePath != "not-opened" {
		t.Fatalf("open attempt = %+v", got)
	}
}

func TestScheduledTimerIgnoresRetiredCoreGeneration(t *testing.T) {
	m := newRecoveryTestManager()
	m.coreGeneration.Store(7)

	var wrapped func()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer {
		wrapped = fn
		return time.NewTimer(time.Hour)
	}

	var calls atomic.Int32
	m.scheduleAfter(time.Second, func() {
		calls.Add(1)
	})
	if wrapped == nil {
		t.Fatal("scheduleAfter did not install a callback")
	}

	m.coreGeneration.Add(1)
	wrapped()

	if got := calls.Load(); got != 0 {
		t.Fatalf("retired timer calls = %d, want 0", got)
	}
	if got := m.staleTimerIgnored.Load(); got != 1 {
		t.Fatalf("stale timers ignored = %d, want 1", got)
	}
}

func TestOpContextStaysCanceledAfterManagerCancellation(t *testing.T) {
	m := newRecoveryTestManager()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.cancel()

	ctx, cancel := m.opContext(time.Second)
	defer cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("opContext error = %v, want context.Canceled", ctx.Err())
	}
}
