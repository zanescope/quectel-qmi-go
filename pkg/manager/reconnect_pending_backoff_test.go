package manager

import "testing"

func TestDequeuedStartConsumesSameGenerationPendingIntent(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.eventCh = make(chan internalEventEnvelope, 1)
	m.desiredConnection = true
	m.eventCh <- internalEventEnvelope{kind: eventStart, generation: 1}

	if got := m.enqueueReconnectEvent(1); got != internalEventQueueFull {
		t.Fatalf("second reconnect enqueue = %v, want queue full", got)
	}
	envelope := <-m.eventCh
	m.consumeQueuedReconnect(envelope)
	if m.reconnectPending || m.reconnectGeneration != 0 {
		t.Fatalf("dequeued start left duplicate pending intent: (%v, %d)", m.reconnectPending, m.reconnectGeneration)
	}

	// A failed dial will install its own backoff timer. The event loop's
	// post-handler promotion must not bypass that delay with a duplicate start.
	m.promotePendingReconnect()
	select {
	case got := <-m.eventCh:
		t.Fatalf("duplicate reconnect bypassed backoff: %+v", got)
	default:
	}
}

func TestStaleDequeuedStartDoesNotConsumeCurrentPendingIntent(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	m.coreGeneration.Store(2)
	m.desiredConnection = true
	m.reconnectPending = true
	m.reconnectGeneration = 2

	m.consumeQueuedReconnect(internalEventEnvelope{kind: eventStart, generation: 1})
	if !m.reconnectPending || m.reconnectGeneration != 2 {
		t.Fatalf("stale start consumed current pending intent: (%v, %d)", m.reconnectPending, m.reconnectGeneration)
	}
}
