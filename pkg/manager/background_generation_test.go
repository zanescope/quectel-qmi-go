package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func TestBackgroundTaskQueuedBehindRecoveryRejectsRetiredCoreSession(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	defer m.events.Close()
	client1 := &qmi.Client{}
	client2 := &qmi.Client{}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.mu.Lock()
	m.client = client1
	m.ctx = runCtx
	m.cancel = cancel
	m.lifetimeActive = true
	m.coreReady = true
	m.state = StateDisconnected
	m.mu.Unlock()
	m.coreGeneration.Store(1)

	m.lifecycleMu.Lock()
	var calls atomic.Int32
	if !m.launchCoreBackgroundTask(func(context.Context, coreSessionToken) { calls.Add(1) }) {
		m.lifecycleMu.Unlock()
		t.Fatal("active generation task was not admitted")
	}
	m.mu.Lock()
	m.client = client2
	m.coreGeneration.Store(2)
	m.mu.Unlock()
	m.lifecycleMu.Unlock()

	done := make(chan struct{})
	go func() {
		m.backgroundTaskMu.Lock()
		m.backgroundTaskMu.Unlock()
		m.backgroundTaskWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stale background task did not exit")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("retired core task calls = %d, want 0", got)
	}
}
