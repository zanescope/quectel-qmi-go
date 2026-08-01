package manager

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestFiredTimerWaitingToClaimIsRetiredByDrain(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()

	var wrapped func()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer {
		wrapped = fn
		return timer
	}

	beforeClaim := make(chan struct{})
	releaseClaim := make(chan struct{})
	m.scheduledTimerBeforeClaimHook = func() {
		close(beforeClaim)
		<-releaseClaim
	}
	var calls atomic.Int32
	m.scheduleAfter(time.Second, func() { calls.Add(1) })
	if wrapped == nil {
		t.Fatal("scheduleAfter did not install a callback")
	}

	callbackDone := make(chan struct{})
	go func() {
		wrapped()
		close(callbackDone)
	}()
	select {
	case <-beforeClaim:
	case <-time.After(time.Second):
		t.Fatal("timer did not reach the pre-claim hook")
	}

	// This is a non-terminal drain: the active lifetime is unpaused before the
	// fired wrapper is released. The epoch must still keep it retired.
	m.stopScheduledTimers()
	close(releaseClaim)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("retired timer wrapper did not return")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("retired pre-claim timer calls = %d, want 0", got)
	}
	if got := m.staleTimerIgnored.Load(); got != 1 {
		t.Fatalf("stale timers ignored = %d, want 1", got)
	}
}

func TestTimerCallbackCanScheduleAnotherTimer(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	defer m.stopScheduledTimers()

	var wrapped []func()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer {
		wrapped = append(wrapped, fn)
		return time.NewTimer(time.Hour)
	}
	m.scheduleAfter(time.Second, func() {
		m.scheduleAfter(time.Second, func() {})
	})
	if len(wrapped) != 1 {
		t.Fatalf("installed timers = %d, want 1", len(wrapped))
	}
	wrapped[0]()
	if len(wrapped) != 2 {
		t.Fatalf("installed timers after reentrant schedule = %d, want 2", len(wrapped))
	}
}
