package manager

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func newListenerBindingTestManager(t *testing.T) (*Manager, context.Context) {
	t.Helper()
	m := New(Config{}, NewNopLogger())
	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.ctx = runCtx
	m.cancel = cancel
	m.lifetimeActive = true
	m.coreGeneration.Store(1)
	m.state = StateDisconnected
	m.markCoreReadyLocked("listener_test_ready")
	m.setCorePhaseLocked(CorePhaseReady, "listener_test_ready", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	t.Cleanup(func() {
		m.mu.Lock()
		m.retireListenerBindingLocked(nil)
		m.mu.Unlock()
		cancel()
		if m.events != nil {
			m.events.Close()
		}
	})
	return m, runCtx
}

func installListenerTestBinding(
	t *testing.T,
	m *Manager,
	events <-chan qmi.Event,
	done <-chan struct{},
	terminalErr error,
) (*listenerBinding, *qmi.Client) {
	t.Helper()
	client := &qmi.Client{}
	m.mu.Lock()
	m.client = client
	m.nextListenerBindingID++
	binding := &listenerBinding{
		id:             m.nextListenerBindingID,
		coreGeneration: m.coreGeneration.Load(),
		client:         client,
		runCtx:         m.ctx,
		events:         events,
		done:           done,
		terminalErr:    func() error { return terminalErr },
		retired:        make(chan struct{}),
	}
	m.replaceListenerBindingLocked(binding)
	m.mu.Unlock()
	return binding, client
}

func waitListenerTestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for listener condition")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitListenerTestGroup(t *testing.T, m *Manager) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener goroutine did not stop")
	}
}

func TestListenerTerminalQueuesOneRecoveryWhileCoreNotReady(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	done := make(chan struct{})
	close(done)
	binding, _ := installListenerTestBinding(t, m, events, done, errors.New("transport EOF"))

	m.mu.Lock()
	m.markControlNotReadyLocked("listener_test_not_ready")
	m.markCoreNotReadyLocked("listener_test_not_ready", nil)
	m.mu.Unlock()

	if !m.reportListenerTerminal(binding) {
		t.Fatal("current terminal binding did not queue recovery while core was not ready")
	}
	if !m.reportListenerTerminal(binding) {
		t.Fatal("duplicate report did not preserve the accepted terminal result")
	}
	if got := m.coreRecoveryRequests.Load(); got != 1 {
		t.Fatalf("core recovery requests = %d, want exactly 1", got)
	}

	m.modemResetMu.Lock()
	queued := m.coreRecoveryEnqueued
	request := m.coreRecoveryRequest
	m.modemResetMu.Unlock()
	if !queued || request.reason != recoveryReasonTransportDown || request.generation != 1 {
		t.Fatalf("queued recovery = %v request=%+v", queued, request)
	}
}

func TestRetiredListenerCannotQueueRecoveryOrRemainUsable(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	eventsA := make(chan qmi.Event)
	doneA := make(chan struct{})
	close(doneA)
	bindingA, _ := installListenerTestBinding(t, m, eventsA, doneA, errors.New("retired EOF"))

	eventsB := make(chan qmi.Event)
	doneB := make(chan struct{})
	bindingB, _ := installListenerTestBinding(t, m, eventsB, doneB, nil)

	select {
	case <-bindingA.retired:
	default:
		t.Fatal("replacement did not retire the old listener")
	}
	if m.reportListenerTerminal(bindingA) {
		t.Fatal("retired listener queued a recovery")
	}
	if got := m.coreRecoveryRequests.Load(); got != 0 {
		t.Fatalf("retired listener advanced recovery requests to %d", got)
	}

	m.mu.RLock()
	oldUsable := m.listenerBindingUsableLocked(bindingA)
	newUsable := m.listenerBindingUsableLocked(bindingB)
	m.mu.RUnlock()
	if oldUsable || !newUsable {
		t.Fatalf("listener usability old=%v new=%v, want false/true", oldUsable, newUsable)
	}
}

func TestIndicationHandlerWakesOnClosedEventStreamWithoutPolling(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	close(events)
	done := make(chan struct{})
	binding, _ := installListenerTestBinding(t, m, events, done, nil)

	m.wg.Add(1)
	started := time.Now()
	go m.indicationHandler(runCtx)
	waitListenerTestCondition(t, func() bool {
		return m.coreRecoveryRequests.Load() == 1
	})
	if elapsed := time.Since(started); elapsed >= 150*time.Millisecond {
		t.Fatalf("closed event stream recovery took %v; handler still appears to poll", elapsed)
	}
	if !binding.terminalRecoveryQueued() {
		t.Fatal("closed event stream did not persist terminal recovery ownership")
	}

	m.mu.Lock()
	m.retireListenerBindingLocked(binding.client)
	m.mu.Unlock()
	m.cancel()
	waitListenerTestGroup(t, m)
}

func TestStopRetiresListenerBeforeClosingClient(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	done := make(chan struct{})
	binding, client := installListenerTestBinding(t, m, events, done, nil)

	closeObserved := make(chan struct{}, 1)
	m.closeQMIClientHook = func(got *qmi.Client) error {
		if got != client {
			t.Errorf("closed client %p, want %p", got, client)
		}
		select {
		case <-binding.retired:
		default:
			t.Error("client Close ran before listener retirement")
		}
		closeObserved <- struct{}{}
		return nil
	}

	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	select {
	case <-closeObserved:
	default:
		t.Fatal("Stop did not close the QMI client")
	}
	m.mu.RLock()
	current := m.listenerBinding
	m.mu.RUnlock()
	if current != nil {
		t.Fatalf("listener after Stop = %+v, want nil", current)
	}
}

func TestStartRejectsTerminalListenerBeforeReadyCommit(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	defer m.events.Close()
	client := &qmi.Client{}
	events := make(chan qmi.Event)
	done := make(chan struct{})
	close(done)
	var binding *listenerBinding

	m.openClientAndAllocateServicesHook = func(context.Context) error {
		m.mu.Lock()
		m.client = client
		binding = m.publishListenerBindingLocked(client, m.coreGeneration.Load(), m.ctx)
		binding.events = events
		binding.done = done
		binding.terminalErr = func() error { return errors.New("startup EOF") }
		m.mu.Unlock()
		return nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }
	m.checkSIMContextHook = func(context.Context) error { return nil }

	err := m.StartCoreContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "terminated before startup commit") {
		t.Fatalf("StartCoreContext() error = %v, want terminal transport rejection", err)
	}
	status := m.CurrentCoreStatus()
	if status.Phase != CorePhaseDegraded || status.CoreReady || status.ControlReady ||
		status.Stage != "start_transport_terminal" {
		t.Fatalf("status after terminal startup = %+v", status)
	}
	select {
	case <-binding.retired:
	default:
		t.Fatal("failed startup closed resources without retiring its listener")
	}
	m.mu.RLock()
	currentBinding := m.listenerBinding
	currentClient := m.client
	m.mu.RUnlock()
	if currentBinding != nil || currentClient != nil {
		t.Fatalf("failed startup retained binding=%+v client=%p", currentBinding, currentClient)
	}
}

func TestGenerationAdvanceRetiresListenerBeforePublishingNewGeneration(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	done := make(chan struct{})
	binding, client := installListenerTestBinding(t, m, events, done, nil)

	m.mu.Lock()
	oldGeneration := m.coreGeneration.Load()
	m.retireListenerBindingLocked(client)
	newGeneration := m.coreGeneration.Add(1)
	current := m.listenerBinding
	m.mu.Unlock()

	if oldGeneration != 1 || newGeneration != 2 {
		t.Fatalf("generation transition = %d -> %d, want 1 -> 2", oldGeneration, newGeneration)
	}
	if current != nil {
		t.Fatalf("new generation retained old listener: %+v", current)
	}
	select {
	case <-binding.retired:
	default:
		t.Fatal("generation advanced before old listener was retired")
	}
	if m.reportListenerTerminal(binding) {
		t.Fatal("retired prior-generation listener queued recovery")
	}
}
func TestModemResetBypassesBlockedOrdinaryIndicationDuringRecovery(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	events := make(chan qmi.Event, 2)
	done := make(chan struct{})
	binding, _ := installListenerTestBinding(t, m, events, done, nil)

	m.modemResetMu.Lock()
	m.modemResetRecovering = true
	m.recoveryGeneration = binding.coreGeneration
	m.currentRecoveryRequest = explicitRecoveryRequest("listener_reset_test")
	m.currentRecoveryRequest.generation = binding.coreGeneration
	m.modemResetMu.Unlock()

	m.lifecycleMu.Lock()
	defer func() {
		m.lifecycleMu.Unlock()
		m.mu.Lock()
		m.retireListenerBindingLocked(binding.client)
		m.mu.Unlock()
		m.cancel()
		waitListenerTestGroup(t, m)
	}()

	m.wg.Add(1)
	go m.indicationHandler(runCtx)
	events <- qmi.Event{Type: qmi.EventNASEventReport}
	events <- qmi.Event{Type: qmi.EventModemReset}

	waitListenerTestCondition(t, func() bool {
		m.modemResetMu.Lock()
		pending := m.modemResetPending
		m.modemResetMu.Unlock()
		return pending
	})
	if got := len(m.listenerIndicationCh); got != 1 {
		t.Fatalf("ordinary dispatch queue length = %d, want 1 blocked item", got)
	}
}

func TestSameGenerationReplacementRejectsRetiredReset(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	eventsA := make(chan qmi.Event)
	doneA := make(chan struct{})
	bindingA, _ := installListenerTestBinding(t, m, eventsA, doneA, nil)

	eventsB := make(chan qmi.Event)
	doneB := make(chan struct{})
	bindingB, _ := installListenerTestBinding(t, m, eventsB, doneB, nil)

	m.handleModemResetForBinding(bindingA, qmi.Event{Type: qmi.EventModemReset})
	m.modemResetMu.Lock()
	staleQueued := m.modemResetEnqueued || m.modemResetPending
	m.modemResetMu.Unlock()
	if staleQueued {
		t.Fatal("retired same-generation binding queued a modem reset")
	}

	m.handleModemResetForBinding(bindingB, qmi.Event{Type: qmi.EventModemReset})
	m.modemResetMu.Lock()
	queued := m.modemResetEnqueued
	request := m.modemResetRequest
	m.modemResetMu.Unlock()
	if !queued || request.generation != bindingB.coreGeneration {
		t.Fatalf("current binding reset queued=%v request=%+v", queued, request)
	}
}

func TestListenerTerminalWithdrawsDurableReadinessBeforeRecoveryRuns(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	done := make(chan struct{})
	close(done)
	terminalErr := errors.New("durable transport EOF")
	binding, _ := installListenerTestBinding(t, m, events, done, terminalErr)

	if !m.reportListenerTerminal(binding) {
		t.Fatal("terminal binding did not queue recovery")
	}
	status := m.CurrentCoreStatus()
	if status.Phase != CorePhaseRecovering || status.Stage != "listener_transport_terminal" ||
		status.CoreReady || status.ControlReady || !strings.Contains(status.LastError, terminalErr.Error()) {
		t.Fatalf("durable status after terminal listener = %+v", status)
	}
	select {
	case envelope := <-m.eventCh:
		if envelope.kind != eventCoreRecovery || envelope.generation != binding.coreGeneration {
			t.Fatalf("recovery envelope = %+v", envelope)
		}
	default:
		t.Fatal("terminal recovery was not queued")
	}
}

func TestRecoveryTerminalCandidateRetiresBindingBeforeBackoff(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	defer m.stopScheduledTimers()
	oldEvents := make(chan qmi.Event)
	oldDone := make(chan struct{})
	_, _ = installListenerTestBinding(t, m, oldEvents, oldDone, nil)
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }
	m.checkSIMContextHook = func(context.Context) error { return nil }
	m.modemResetQuietWindow = time.Millisecond
	m.getICCIDStrictHook = func(context.Context) (string, error) { return "test-iccid", nil }

	terminalErr := errors.New("recovered transport EOF")
	var terminalBinding *listenerBinding
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		client := &qmi.Client{}
		events := make(chan qmi.Event)
		done := make(chan struct{})
		close(done)
		m.mu.Lock()
		m.client = client
		m.nextListenerBindingID++
		terminalBinding = &listenerBinding{
			id:             m.nextListenerBindingID,
			coreGeneration: m.coreGeneration.Load(),
			client:         client,
			runCtx:         m.ctx,
			events:         events,
			done:           done,
			terminalErr:    func() error { return terminalErr },
			retired:        make(chan struct{}),
		}
		m.replaceListenerBindingLocked(terminalBinding)
		m.mu.Unlock()
		return nil
	}

	request := explicitRecoveryRequest("terminal_candidate")
	request.generation = 1
	m.modemResetMu.Lock()
	m.modemResetRecovering = true
	m.recoveryGeneration = 1
	m.currentRecoveryRequest = request
	m.modemResetMu.Unlock()

	if m.doRecoverCore(request) {
		t.Fatal("terminal recovered transport committed Ready")
	}
	if terminalBinding == nil {
		t.Fatal("recovery did not publish the terminal candidate")
	}
	select {
	case <-terminalBinding.retired:
	default:
		t.Fatal("terminal candidate was not retired before scheduling backoff")
	}
	m.mu.RLock()
	current := m.listenerBinding
	m.mu.RUnlock()
	if current != nil {
		t.Fatalf("terminal candidate remained current during backoff: %+v", current)
	}

	m.finishRecovery()
	before := m.coreRecoveryRequests.Load()
	if m.reportListenerTerminal(terminalBinding) {
		t.Fatal("retired terminal candidate bypassed recovery backoff")
	}
	if got := m.coreRecoveryRequests.Load(); got != before {
		t.Fatalf("retired terminal candidate added recovery request: before=%d after=%d", before, got)
	}
	status := m.CurrentCoreStatus()
	if status.Stage != "recovery_backoff" || status.CoreReady || status.ControlReady {
		t.Fatalf("status after terminal candidate backoff = %+v", status)
	}
}
func TestModemResetExternalEventIsQueuedBeforeRecoveryWake(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	m.events.Close()
	m.events = &EventEmitter{
		queue: make(chan Event, 1),
		done:  make(chan struct{}),
	}
	events := make(chan qmi.Event)
	done := make(chan struct{})
	binding, _ := installListenerTestBinding(t, m, events, done, nil)

	hookCalled := false
	m.recoveryEventValidatedHook = func() {
		hookCalled = true
		select {
		case event := <-m.events.queue:
			if event.Type != EventModemReset || event.Generation != binding.coreGeneration {
				t.Errorf("external reset event before recovery wake = %+v", event)
			}
		default:
			t.Error("internal recovery was signaled before the external modem reset event was queued")
		}
	}

	m.handleModemResetForBinding(binding, qmi.Event{Type: qmi.EventModemReset})
	if !hookCalled {
		t.Fatal("recovery wake validation hook was not called")
	}
}
func TestOrdinaryIndicationQueueDropsNonReadyAndCountsOverflow(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	events := make(chan qmi.Event)
	done := make(chan struct{})
	binding, _ := installListenerTestBinding(t, m, events, done, nil)
	m.listenerIndicationCh = make(chan listenerIndication, 1)

	m.mu.Lock()
	m.markCoreNotReadyLocked("listener_queue_test", nil)
	m.mu.Unlock()
	m.queueListenerIndication(runCtx, binding, qmi.Event{Type: qmi.EventNASEventReport})
	if got := len(m.listenerIndicationCh); got != 0 {
		t.Fatalf("non-ready binding queued %d ordinary indications", got)
	}

	m.mu.Lock()
	m.markCoreReadyLocked("listener_queue_test_ready")
	m.mu.Unlock()
	m.queueListenerIndication(runCtx, binding, qmi.Event{Type: qmi.EventNASEventReport})
	m.queueListenerIndication(runCtx, binding, qmi.Event{Type: qmi.EventNASSignalInfoChanged})
	if got := len(m.listenerIndicationCh); got != 1 {
		t.Fatalf("bounded listener queue length = %d, want 1", got)
	}
	if got := m.Stats().ListenerIndicationsDropped; got != 1 {
		t.Fatalf("listener indication drops = %d, want 1", got)
	}
}
