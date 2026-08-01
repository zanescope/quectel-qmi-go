package manager

import (
	"testing"
	"time"
)

func TestQueueFullReconnectIsPromotedAfterEventDrain(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.eventCh = make(chan internalEventEnvelope, 1)
	m.desiredConnection = true
	m.eventCh <- internalEventEnvelope{kind: eventCheckFull, generation: 1}

	if got := m.enqueueReconnectEvent(1); got != internalEventQueueFull {
		t.Fatalf("enqueue reconnect = %v, want queue full", got)
	}
	if !m.reconnectPending || m.reconnectGeneration != 1 {
		t.Fatalf("pending reconnect = (%v, %d), want (true, 1)", m.reconnectPending, m.reconnectGeneration)
	}

	<-m.eventCh
	m.promotePendingReconnect()
	select {
	case envelope := <-m.eventCh:
		if envelope.kind != eventStart || envelope.generation != 1 {
			t.Fatalf("promoted envelope = %+v, want eventStart generation 1", envelope)
		}
	case <-time.After(time.Second):
		t.Fatal("pending reconnect was not promoted after queue drain")
	}
	if m.reconnectPending || m.reconnectGeneration != 0 {
		t.Fatalf("pending reconnect not cleared: (%v, %d)", m.reconnectPending, m.reconnectGeneration)
	}
}

func TestPendingReconnectDoesNotUpgradeToNewGeneration(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.eventCh = make(chan internalEventEnvelope, 1)
	m.desiredConnection = true
	m.eventCh <- internalEventEnvelope{kind: eventCheckFull, generation: 1}

	if got := m.enqueueReconnectEvent(1); got != internalEventQueueFull {
		t.Fatalf("enqueue reconnect = %v, want queue full", got)
	}
	<-m.eventCh
	m.coreGeneration.Store(2)
	m.promotePendingReconnect()
	select {
	case envelope := <-m.eventCh:
		t.Fatalf("stale reconnect was upgraded and enqueued: %+v", envelope)
	default:
	}
	if m.reconnectPending || m.reconnectGeneration != 0 {
		t.Fatalf("stale pending reconnect not retired: (%v, %d)", m.reconnectPending, m.reconnectGeneration)
	}
}

func TestInitialRequestKeepsIntentWhenEventQueueIsFull(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.eventCh = make(chan internalEventEnvelope, 1)
	m.coreReady = true
	m.lifetimeActive = true
	m.eventCh <- internalEventEnvelope{kind: eventCheckFull, generation: 1}

	if err := m.requestDataConnection(); err != nil {
		t.Fatalf("requestDataConnection() error = %v, want durable pending intent", err)
	}
	if !m.desiredConnection || !m.reconnectPending || m.reconnectGeneration != 1 {
		t.Fatalf("connection intent = desired:%v pending:%v generation:%d",
			m.desiredConnection, m.reconnectPending, m.reconnectGeneration)
	}
}
