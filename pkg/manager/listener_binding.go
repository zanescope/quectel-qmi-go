package manager

import (
	"context"
	"errors"
	"sync"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

var errQMIClientEventStreamClosed = errors.New("QMI client event stream closed")

const listenerIndicationQueueSize = 256

type listenerIndication struct {
	binding *listenerBinding
	event   qmi.Event
}

// listenerBinding owns the indication stream for one exact core transport.
// Retirement always happens before the manager closes or replaces the client.
type listenerBinding struct {
	id             uint64
	coreGeneration uint64
	client         *qmi.Client
	runCtx         context.Context
	events         <-chan qmi.Event
	done           <-chan struct{}
	terminalErr    func() error
	retired        chan struct{}
	retireOnce     sync.Once
	terminalMu     sync.Mutex
	terminalQueued bool
}

func newListenerBinding(id uint64, generation uint64, client *qmi.Client, runCtx context.Context) *listenerBinding {
	if id == 0 || generation == 0 || client == nil || runCtx == nil {
		return nil
	}
	return &listenerBinding{
		id:             id,
		coreGeneration: generation,
		client:         client,
		runCtx:         runCtx,
		events:         client.Events(),
		done:           client.Done(),
		terminalErr:    client.Err,
		retired:        make(chan struct{}),
	}
}

func (b *listenerBinding) retire() {
	if b == nil {
		return
	}
	b.retireOnce.Do(func() {
		if b.retired != nil {
			close(b.retired)
		}
	})
}

func (b *listenerBinding) err() error {
	if b == nil || b.terminalErr == nil {
		return nil
	}
	return b.terminalErr()
}

func (b *listenerBinding) terminal() bool {
	if b == nil || b.done == nil {
		return false
	}
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

func (b *listenerBinding) terminalRecoveryQueued() bool {
	if b == nil {
		return false
	}
	b.terminalMu.Lock()
	queued := b.terminalQueued
	b.terminalMu.Unlock()
	return queued
}

// ensureListenerChangedLocked requires m.mu.
func (m *Manager) ensureListenerChangedLocked() chan struct{} {
	if m.listenerChanged == nil {
		m.listenerChanged = make(chan struct{}, 1)
	}
	return m.listenerChanged
}

// signalListenerChangedLocked requires m.mu and never blocks lifecycle work.
func (m *Manager) signalListenerChangedLocked() {
	changed := m.ensureListenerChangedLocked()
	select {
	case changed <- struct{}{}:
	default:
	}
}

// replaceListenerBindingLocked atomically retires the previous indication
// owner and publishes next. It requires m.mu. next may be nil.
func (m *Manager) replaceListenerBindingLocked(next *listenerBinding) {
	if current := m.listenerBinding; current != nil && current != next {
		current.retire()
	}
	m.listenerBinding = next
	m.signalListenerChangedLocked()
}

// publishListenerBindingLocked publishes the exact current client/generation
// tuple. It requires m.mu and must share the transaction publishing m.client.
func (m *Manager) publishListenerBindingLocked(client *qmi.Client, generation uint64, runCtx context.Context) *listenerBinding {
	if client == nil || generation == 0 || runCtx == nil {
		m.replaceListenerBindingLocked(nil)
		return nil
	}
	m.nextListenerBindingID++
	next := newListenerBinding(m.nextListenerBindingID, generation, client, runCtx)
	m.replaceListenerBindingLocked(next)
	return next
}

// retireListenerBindingLocked retires the current binding if expectedClient
// is nil or owns it. It requires m.mu and must run before transport Close.
func (m *Manager) retireListenerBindingLocked(expectedClient *qmi.Client) {
	current := m.listenerBinding
	if current == nil || (expectedClient != nil && current.client != expectedClient) {
		return
	}
	m.replaceListenerBindingLocked(nil)
}

// listenerBindingOwnedLocked requires m.mu. It validates transport ownership
// independently from readiness so a terminal client cannot be hidden while
// startup or recovery is still converging.
func (m *Manager) listenerBindingOwnedLocked(binding *listenerBinding) bool {
	return binding != nil &&
		binding.id != 0 &&
		binding.coreGeneration != 0 &&
		binding.client != nil &&
		binding.runCtx != nil &&
		binding.runCtx.Err() == nil &&
		m.listenerBinding == binding &&
		m.client == binding.client &&
		m.ctx == binding.runCtx &&
		m.coreGeneration.Load() == binding.coreGeneration &&
		m.lifetimeActive &&
		!m.stopped &&
		m.state != StateStopping
}

// listenerBindingUsableLocked requires m.mu and gates indication side effects.
func (m *Manager) listenerBindingUsableLocked(binding *listenerBinding) bool {
	return m.listenerBindingOwnedLocked(binding) && m.coreReady
}

// currentListenerTerminalLocked requires m.mu. A nil binding is permitted for
// tests that replace the open hook; production opens always publish one.
func (m *Manager) currentListenerTerminalLocked(generation uint64) bool {
	binding := m.listenerBinding
	return binding != nil &&
		binding.coreGeneration == generation &&
		m.listenerBindingOwnedLocked(binding) &&
		binding.terminal()
}

func (m *Manager) listenerBindingSnapshot() (*listenerBinding, <-chan struct{}) {
	if m == nil {
		return nil, nil
	}
	m.mu.Lock()
	changed := m.ensureListenerChangedLocked()
	binding := m.listenerBinding
	m.mu.Unlock()
	return binding, changed
}

func waitForListenerChange(runCtx context.Context, binding *listenerBinding, changed <-chan struct{}) bool {
	if runCtx == nil {
		return false
	}
	if binding == nil {
		select {
		case <-runCtx.Done():
			return false
		case <-changed:
			return true
		}
	}
	select {
	case <-runCtx.Done():
		return false
	case <-binding.runCtx.Done():
		return false
	case <-binding.retired:
		return true
	case <-changed:
		return true
	}
}

func (m *Manager) queueListenerIndication(runCtx context.Context, binding *listenerBinding, event qmi.Event) {
	if m == nil || runCtx == nil || binding == nil || m.listenerIndicationCh == nil {
		return
	}
	m.mu.RLock()
	usable := m.listenerBindingUsableLocked(binding)
	m.mu.RUnlock()
	if !usable {
		return
	}
	select {
	case <-runCtx.Done():
	case m.listenerIndicationCh <- listenerIndication{binding: binding, event: event}:
	default:
		dropped := m.listenerIndicationsDropped.Add(1)
		// Log only at powers of two. The counter remains exact while a burst
		// cannot create a second log storm.
		if m.log != nil && dropped&(dropped-1) == 0 {
			m.log.Warnf("Dropped ordinary QMI indication because the bounded dispatch queue is full (total=%d)", dropped)
		}
	}
}

// listenerIndicationHandler serializes ordinary indication side effects. The
// transport receive loop remains independent so modem resets can always reach
// the recovery quiet-window gate even while lifecycle work is in progress.
func (m *Manager) listenerIndicationHandler(runCtx context.Context) {
	defer m.wg.Done()
	for {
		if runCtx == nil || runCtx.Err() != nil {
			return
		}
		select {
		case <-runCtx.Done():
			return
		case indication := <-m.listenerIndicationCh:
			if runCtx.Err() != nil {
				return
			}
			m.handleIndicationForBinding(indication.binding, indication.event)
		}
	}
}

// reportListenerTerminal submits recovery once for the exact current binding.
// lifecycleMu closes the validation/enqueue race with transport replacement.
func (m *Manager) reportListenerTerminal(binding *listenerBinding) bool {
	if m == nil || binding == nil {
		return false
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	binding.terminalMu.Lock()
	defer binding.terminalMu.Unlock()
	if binding.terminalQueued {
		return true
	}

	terminalErr := binding.err()
	if terminalErr == nil {
		terminalErr = errQMIClientEventStreamClosed
	}

	// Withdraw readiness before queueing recovery so the durable snapshot can
	// never remain Ready after this exact transport is known to be terminal.
	m.mu.Lock()
	if !m.listenerBindingOwnedLocked(binding) {
		m.mu.Unlock()
		return false
	}
	m.markControlNotReadyLocked("listener_transport_terminal")
	m.markCoreNotReadyLocked("listener_transport_terminal", terminalErr)
	m.setCorePhaseLocked(CorePhaseRecovering, "listener_transport_terminal", string(recoveryReasonTransportDown), terminalErr)
	m.publishCoreStatusLocked()
	m.mu.Unlock()

	request := recoveryRequest{
		kind:   recoveryKindSoftware,
		reason: recoveryReasonTransportDown,
		detail: "listener_terminal",
	}
	if !m.enqueueCoreRecoveryEventWithOptions(request, true, true) {
		return false
	}
	binding.terminalQueued = true
	m.coreRecoveryLogger().WithError(terminalErr).Warn("QMI client transport terminated; scheduling core recovery")
	return true
}
