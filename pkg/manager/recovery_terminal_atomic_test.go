package manager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHardRecoveryTerminalClearsPendingWorkBeforeEmission(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.modemResetRecovering = true
	m.recoveryGeneration = 1
	m.currentRecoveryRequest = recoveryRequest{generation: 1, reason: "active"}
	m.modemResetPending = true
	m.modemResetEnqueued = true
	m.modemResetRequest = recoveryRequest{generation: 1, reason: "pending-reset"}
	m.coreRecoveryRetryPending = true
	m.coreRecoveryRetryRequest = recoveryRequest{generation: 1, reason: "pending-retry", retry: true}
	m.modemResetDeferred = true
	m.coreRecoveryEnqueued = true
	m.coreRecoveryRequest = recoveryRequest{generation: 1, reason: "pending-core"}

	result := recoveryAttemptResult{
		generation:     1,
		terminalReason: "device_removed",
		terminalErr:    errors.New("device gone"),
	}
	if !m.commitHardRecoveryTerminal(&result) {
		t.Fatal("hard terminal was not committed")
	}
	if !result.finished {
		t.Fatal("hard terminal result was not marked finished")
	}
	if m.modemResetRecovering || m.modemResetPending || m.modemResetEnqueued ||
		m.coreRecoveryEnqueued || m.coreRecoveryRetryPending || m.modemResetDeferred ||
		m.recoveryGeneration != 0 {
		t.Fatalf("terminal commit left recovery work: recovering=%v resetPending=%v resetEnqueued=%v coreEnqueued=%v retryPending=%v deferred=%v generation=%d",
			m.modemResetRecovering, m.modemResetPending, m.modemResetEnqueued,
			m.coreRecoveryEnqueued, m.coreRecoveryRetryPending, m.modemResetDeferred, m.recoveryGeneration)
	}

	m.finishRecovery()
	select {
	case envelope := <-m.eventCh:
		t.Fatalf("recovery work promoted after terminal commit: %+v", envelope)
	default:
	}
}

func TestHardRecoveryTerminalIsLastEventForAttempt(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.cfg.Device.ControlPath = "/dev/qmi-terminal-order-node-does-not-exist"
	m.cfg.Timeouts.Stop = time.Second
	m.recoveryGeneration = 1
	m.coreRecoveryEnqueued = true
	request := explicitRecoveryRequest("terminal-order")
	request.generation = 1
	m.coreRecoveryRequest = request
	m.openClientAndAllocateServicesHook = func(_ context.Context) error { return errors.New("device gone") }

	got := make(chan Event, 2)
	m.OnEvent(func(event Event) {
		if event.Type == EventCoreRecoveryFailed || event.Type == EventRecoveryExhausted {
			got <- event
		}
	})

	m.handleRecoveryEventForGeneration(eventCoreRecovery, 1)

	deadline := time.After(time.Second)
	want := []EventType{EventCoreRecoveryFailed, EventRecoveryExhausted}
	for i, wantType := range want {
		select {
		case event := <-got:
			if event.Type != wantType {
				t.Fatalf("event[%d] = %s, want %s", i, event.Type, wantType)
			}
			if event.Generation != 2 {
				t.Fatalf("event[%d] generation = %d, want 2", i, event.Generation)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event[%d] (%s)", i, wantType)
		}
	}
	select {
	case event := <-got:
		t.Fatalf("unexpected recovery event after terminal: %+v", event)
	default:
	}
}
