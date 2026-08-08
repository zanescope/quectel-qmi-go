package manager

import (
	"context"
	"sync"
	"time"
)

// CorePhase describes the lifecycle of the QMI core independently from the
// data connection State. In particular, StateConnecting may describe either
// core startup or a data dial, while CorePhase remains unambiguous.
type CorePhase string

const (
	CorePhaseIdle       CorePhase = "idle"
	CorePhaseStarting   CorePhase = "starting"
	CorePhaseReady      CorePhase = "ready"
	CorePhaseRecovering CorePhase = "recovering"
	CorePhaseDegraded   CorePhase = "degraded"
	CorePhaseStopping   CorePhase = "stopping"
	CorePhaseStopped    CorePhase = "stopped"
	CorePhaseTerminal   CorePhase = "terminal"
)

// CoreStatus is the durable latest snapshot of the manager core lifecycle.
// Sequence is local to this status stream and advances independently from
// Generation. LastError is a string so snapshots never retain mutable errors.
type CoreStatus struct {
	Sequence     uint64
	Generation   uint64
	Phase        CorePhase
	State        State
	ControlReady bool
	CoreReady    bool
	Recovering   bool
	Terminal     bool
	Stage        string
	Reason       string
	LastError    string
	UpdatedAt    time.Time
}

// coreStatusHub is a non-blocking, latest-value broadcaster. It has no
// dispatcher goroutine and its zero value is ready for use. Every subscriber
// owns a capacity-one mailbox; a slow subscriber loses intermediate snapshots
// but always retains the newest one.
type coreStatusHub struct {
	mu          sync.Mutex
	current     CoreStatus
	initialized bool
	nextID      uint64
	subscribers map[uint64]chan CoreStatus
}

func (h *coreStatusHub) currentOr(initial CoreStatus) CoreStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.initialized {
		h.current = initial
		h.initialized = true
	}
	return h.current
}

func (h *coreStatusHub) publish(next CoreStatus) CoreStatus {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.initialized {
		next.Sequence = h.current.Sequence + 1
	} else {
		next.Sequence = 1
		h.initialized = true
	}
	next.UpdatedAt = time.Now()
	h.current = next

	for _, ch := range h.subscribers {
		select {
		case <-ch:
		default:
		}
		// The channel has capacity one, all publishers hold h.mu, and an
		// unsubscribe cannot close it while this lock is held.
		ch <- next
	}
	return next
}

func (h *coreStatusHub) subscribe(ctx context.Context, initial CoreStatus) <-chan CoreStatus {
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan CoreStatus, 1)
	if err := ctx.Err(); err != nil {
		close(ch)
		return ch
	}

	h.mu.Lock()
	if !h.initialized {
		h.current = initial
		h.initialized = true
	}
	h.nextID++
	id := h.nextID
	if h.subscribers == nil {
		h.subscribers = make(map[uint64]chan CoreStatus)
	}
	h.subscribers[id] = ch
	ch <- h.current
	h.mu.Unlock()

	context.AfterFunc(ctx, func() {
		h.mu.Lock()
		if registered, ok := h.subscribers[id]; ok && registered == ch {
			delete(h.subscribers, id)
			close(ch)
		}
		h.mu.Unlock()
	})
	return ch
}

// CurrentCoreStatus returns the latest durable core lifecycle snapshot.
func (m *Manager) CurrentCoreStatus() CoreStatus {
	if m == nil {
		return CoreStatus{Phase: CorePhaseIdle, State: StateDisconnected}
	}
	m.mu.RLock()
	initial := m.coreStatusSnapshotLocked()
	status := m.coreStatusHub.currentOr(initial)
	m.mu.RUnlock()
	return status
}

// SubscribeCoreStatus subscribes to the durable latest core lifecycle state.
// The returned channel has capacity one. Publication never waits for a slow
// consumer; an unread value is replaced by the newest snapshot. Stop publishes
// CorePhaseStopped but deliberately leaves subscriptions open until ctx ends.
func (m *Manager) SubscribeCoreStatus(ctx context.Context) <-chan CoreStatus {
	if m == nil {
		ch := make(chan CoreStatus)
		close(ch)
		return ch
	}
	m.mu.RLock()
	initial := m.coreStatusSnapshotLocked()
	ch := m.coreStatusHub.subscribe(ctx, initial)
	m.mu.RUnlock()
	return ch
}

// coreStatusSnapshotLocked requires m.mu to be held. It must not acquire any
// manager lock; hub.mu is always the final lock in the lifecycle lock order.
func (m *Manager) coreStatusSnapshotLocked() CoreStatus {
	phase := m.corePhase
	if phase == "" {
		switch {
		case m.stopped:
			phase = CorePhaseStopped
		case m.coreReady:
			phase = CorePhaseReady
		case m.lifetimeActive:
			phase = CorePhaseStarting
		default:
			phase = CorePhaseIdle
		}
	}
	return CoreStatus{
		Generation:   m.coreGeneration.Load(),
		Phase:        phase,
		State:        m.state,
		ControlReady: m.controlReady,
		CoreReady:    m.coreReady,
		Recovering:   phase == CorePhaseRecovering,
		Terminal:     m.coreStatusTerminal || phase == CorePhaseTerminal,
		Stage:        m.coreStatusStage,
		Reason:       m.coreStatusReason,
		LastError:    m.coreStatusLastErr,
	}
}

// publishCoreStatusLocked requires m.mu. It only copies immutable state into
// the hub and never invokes callbacks or performs I/O.
func (m *Manager) publishCoreStatusLocked() CoreStatus {
	return m.coreStatusHub.publish(m.coreStatusSnapshotLocked())
}

// setCorePhaseLocked updates lifecycle metadata without publishing. Callers
// compose all fields in their atomic transaction, then publish exactly once.
func (m *Manager) setCorePhaseLocked(phase CorePhase, stage, reason string, err error) {
	if phase != "" {
		m.corePhase = phase
		m.coreStatusTerminal = phase == CorePhaseTerminal
	}
	if stage != "" {
		m.coreStatusStage = stage
	}
	if reason != "" || phase == CorePhaseIdle || phase == CorePhaseStarting || phase == CorePhaseStopping || phase == CorePhaseStopped {
		m.coreStatusReason = reason
	}
	if err != nil {
		m.coreStatusLastErr = err.Error()
	} else {
		switch phase {
		case CorePhaseIdle, CorePhaseStarting, CorePhaseReady, CorePhaseStopping, CorePhaseStopped:
			m.coreStatusLastErr = ""
		}
	}
}
