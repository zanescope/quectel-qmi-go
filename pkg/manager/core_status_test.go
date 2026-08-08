package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func receiveCoreStatus(t *testing.T, ch <-chan CoreStatus) CoreStatus {
	t.Helper()
	select {
	case status, ok := <-ch:
		if !ok {
			t.Fatal("core status subscription closed unexpectedly")
		}
		return status
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for core status")
		return CoreStatus{}
	}
}

func TestCoreStatusZeroValueAndSlowConsumerKeepsLatest(t *testing.T) {
	var m Manager
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := m.SubscribeCoreStatus(ctx)
	if cap(updates) != 1 {
		t.Fatalf("subscription capacity = %d, want 1", cap(updates))
	}
	initial := receiveCoreStatus(t, updates)
	if initial.Sequence != 0 || initial.Generation != 0 || initial.Phase != CorePhaseIdle || initial.State != StateDisconnected {
		t.Fatalf("zero-value status = %+v, want idle sequence/generation zero", initial)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			m.mu.Lock()
			m.coreGeneration.Store(9)
			m.state = State(i % 3)
			m.setCorePhaseLocked(CorePhaseStarting, fmt.Sprintf("publish_%03d", i), "", nil)
			m.publishCoreStatusLocked()
			m.mu.Unlock()
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publisher blocked behind slow subscriber")
	}

	want := m.CurrentCoreStatus()
	got := receiveCoreStatus(t, updates)
	if got != want {
		t.Fatalf("slow subscriber got %+v, want latest %+v", got, want)
	}
	if got.Sequence != 100 || got.Generation != 9 || got.Stage != "publish_099" {
		t.Fatalf("latest status = %+v, want sequence=100 generation=9 final stage", got)
	}
}

func TestCoreStatusInitialSnapshotAndConcurrentPublishAreLinearized(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	defer m.events.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := make(chan struct{})
	var updates <-chan CoreStatus
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		updates = m.SubscribeCoreStatus(ctx)
	}()
	go func() {
		defer wg.Done()
		<-start
		m.mu.Lock()
		m.coreGeneration.Store(4)
		m.state = StateConnecting
		m.setCorePhaseLocked(CorePhaseStarting, "concurrent_publish", "", nil)
		m.publishCoreStatusLocked()
		m.mu.Unlock()
	}()
	close(start)
	wg.Wait()

	want := m.CurrentCoreStatus()
	got := receiveCoreStatus(t, updates)
	if got != want {
		t.Fatalf("linearized subscription got %+v, want current %+v", got, want)
	}
}

func TestCoreStatusSequenceIndependentFromGenerationAndStaleRetry(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	defer m.cancel()

	m.mu.Lock()
	m.coreGeneration.Store(5)
	m.setCorePhaseLocked(CorePhaseReady, "ready", "", nil)
	first := m.publishCoreStatusLocked()
	m.state = StateConnecting
	second := m.publishCoreStatusLocked()
	m.mu.Unlock()
	if second.Generation != first.Generation || second.Generation != 5 || second.Sequence != first.Sequence+1 {
		t.Fatalf("sequence/generation statuses = first=%+v second=%+v", first, second)
	}

	before := m.CurrentCoreStatus()
	result := m.scheduleRecoverRetryFor(recoveryRequest{
		reason: recoveryReasonExplicitRequest, generation: 4,
	}, "stale_retry")
	after := m.CurrentCoreStatus()
	if result.generation != 5 {
		t.Fatalf("stale retry current generation = %d, want 5", result.generation)
	}
	if after.Sequence != before.Sequence || after != before {
		t.Fatalf("stale retry published status: before=%+v after=%+v", before, after)
	}
}

func TestCoreStatusStopPublishesStoppedWithoutClosingHub(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	ctx, cancel := context.WithCancel(context.Background())
	updates := m.SubscribeCoreStatus(ctx)
	_ = receiveCoreStatus(t, updates)

	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := receiveCoreStatus(t, updates)
	if stopped.Phase != CorePhaseStopped || stopped.State != StateDisconnected || stopped.CoreReady || stopped.ControlReady {
		t.Fatalf("final stop status = %+v", stopped)
	}
	if current := m.CurrentCoreStatus(); current != stopped {
		t.Fatalf("current after Stop = %+v, subscription got %+v", current, stopped)
	}

	select {
	case _, ok := <-updates:
		if !ok {
			t.Fatal("Stop closed the core status hub")
		}
		t.Fatal("unexpected status after final Stopped snapshot")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("subscription produced a value after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not close after context cancellation")
	}
}

func TestCoreStatusFailedStartCanRetryToReady(t *testing.T) {
	openErr := errors.New("first open failed")
	m := New(Config{}, NewNopLogger())
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	var attempts int
	var attemptsMu sync.Mutex
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		attemptsMu.Lock()
		attempts++
		attempt := attempts
		attemptsMu.Unlock()
		if attempt == 1 {
			close(openStarted)
			<-releaseOpen
			return openErr
		}
		return nil
	}
	m.checkSIMContextHook = func(context.Context) error { return nil }

	firstDone := make(chan error, 1)
	go func() { firstDone <- m.StartCoreContext(context.Background()) }()
	select {
	case <-openStarted:
	case <-time.After(time.Second):
		t.Fatal("first startup did not reach transport open")
	}
	starting := m.CurrentCoreStatus()
	if starting.Phase != CorePhaseStarting || starting.Generation != 1 ||
		starting.State != StateConnecting || starting.ControlReady || starting.CoreReady {
		t.Fatalf("in-flight startup status = %+v", starting)
	}
	close(releaseOpen)
	select {
	case err := <-firstDone:
		if !errors.Is(err, openErr) {
			t.Fatalf("first StartCoreContext() error = %v, want %v", err, openErr)
		}
	case <-time.After(time.Second):
		t.Fatal("failed startup did not return")
	}
	degraded := m.CurrentCoreStatus()
	if degraded.Phase != CorePhaseDegraded || degraded.Generation != 1 ||
		degraded.State != StateDisconnected || degraded.CoreReady || degraded.ControlReady ||
		degraded.Stage != "start_open_failed" || !strings.Contains(degraded.LastError, openErr.Error()) {
		t.Fatalf("failed startup status = %+v", degraded)
	}
	if degraded.Sequence <= starting.Sequence {
		t.Fatalf("failed startup sequence = %d, want > starting sequence %d", degraded.Sequence, starting.Sequence)
	}

	if err := m.StartCoreContext(context.Background()); err != nil {
		t.Fatalf("retry StartCoreContext() error = %v", err)
	}
	ready := m.CurrentCoreStatus()
	if ready.Phase != CorePhaseReady || ready.Generation != 2 ||
		ready.State != StateDisconnected || !ready.CoreReady || !ready.ControlReady ||
		ready.Stage != "start_core_ready" || ready.LastError != "" {
		t.Fatalf("successful retry status = %+v", ready)
	}
	if ready.Sequence <= degraded.Sequence {
		t.Fatalf("ready sequence = %d, want > degraded sequence %d", ready.Sequence, degraded.Sequence)
	}
	attemptsMu.Lock()
	gotAttempts := attempts
	attemptsMu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("open attempts = %d, want 2", gotAttempts)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func prepareCoreStatusRecoveryTestManager(t *testing.T, cfg Config, openErr error) *Manager {
	t.Helper()
	m := newRecoveryTestManager()
	m.cfg = normalizeConfig(cfg)
	m.openClientAndAllocateServicesHook = func(context.Context) error { return openErr }
	m.mu.Lock()
	m.lifetimeActive = true
	m.controlReady = true
	m.coreReady = true
	m.state = StateDisconnected
	m.corePhase = CorePhaseReady
	m.coreReadyLastErr = "stale readiness error"
	m.coreStatusLastErr = "stale status error"
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	return m
}

func runOneFailedRecovery(t *testing.T, m *Manager) {
	t.Helper()
	if !m.enqueueCoreRecoveryEvent(explicitRecoveryRequest("core_status_test")) {
		t.Fatal("failed to enqueue core recovery")
	}
	select {
	case envelope := <-m.eventCh:
		m.handleRecoveryEventForGeneration(envelope.kind, envelope.generation)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recovery event")
	}
}

func TestCoreStatusRecoveryGenerationBackoffAndStaleError(t *testing.T) {
	openErr := errors.New("reopen failed")
	m := prepareCoreStatusRecoveryTestManager(t, Config{}, openErr)
	var beforeReopen CoreStatus
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		beforeReopen = m.CurrentCoreStatus()
		return openErr
	}
	defer func() { _ = m.Stop() }()

	runOneFailedRecovery(t, m)
	if beforeReopen.Generation != 2 || beforeReopen.LastError != "" {
		t.Fatalf("new generation carried stale error before reopen: %+v", beforeReopen)
	}
	status := m.CurrentCoreStatus()
	if status.Generation != 2 {
		t.Fatalf("recovery generation = %d, want exact coreGeneration 2", status.Generation)
	}
	if status.Phase != CorePhaseDegraded || status.Stage != "recovery_backoff" || status.Recovering {
		t.Fatalf("durable backoff status = %+v", status)
	}
	if status.CoreReady || status.ControlReady {
		t.Fatalf("failed recovery advertised readiness: %+v", status)
	}
	if !strings.Contains(status.LastError, openErr.Error()) || strings.Contains(status.LastError, "stale") {
		t.Fatalf("recovery LastError = %q, want current reopen error only", status.LastError)
	}

	before := status
	_ = m.scheduleRecoverRetryFor(recoveryRequest{
		reason: recoveryReasonExplicitRequest, generation: 1,
	}, "retired_generation")
	after := m.CurrentCoreStatus()
	if after.Sequence != before.Sequence || after != before {
		t.Fatalf("retired retry advanced status: before=%+v after=%+v", before, after)
	}
}

func TestCoreStatusTerminalSurvivesClosedEventEmitter(t *testing.T) {
	openErr := errors.New("terminal reopen failed")
	m := prepareCoreStatusRecoveryTestManager(t, Config{
		RecoveryPolicy: RecoveryPolicy{MaxRecoverAttempts: 1},
	}, openErr)
	defer func() { _ = m.Stop() }()
	m.mu.Lock()
	m.recoverCount = 1
	m.mu.Unlock()
	m.events.Close()

	runOneFailedRecovery(t, m)
	status := m.CurrentCoreStatus()
	if status.Phase != CorePhaseTerminal || !status.Terminal || status.Recovering {
		t.Fatalf("terminal status = %+v", status)
	}
	if status.Generation != 2 || status.Reason != "recovery_exhausted" || status.Stage != "recovery_terminal" {
		t.Fatalf("terminal generation/metadata = %+v", status)
	}
	if !strings.Contains(status.LastError, openErr.Error()) {
		t.Fatalf("terminal LastError = %q, want %q", status.LastError, openErr)
	}
	if m.events.Dropped() == 0 {
		t.Fatal("closed EventEmitter did not record dropped recovery events")
	}
}

func TestCoreStatusDirectRecoveryCommitsTerminalBeforeDroppedEvent(t *testing.T) {
	openErr := errors.New("direct recovery device removed")
	missingControl := t.TempDir() + "/missing-control"
	m := prepareCoreStatusRecoveryTestManager(t, Config{
		Device: ModemDevice{ControlPath: missingControl},
	}, openErr)
	m.events.Close()
	defer func() { _ = m.Stop() }()

	if recovered := m.doRecoverFromModemReset(); recovered {
		t.Fatal("direct recovery unexpectedly succeeded")
	}
	status := m.CurrentCoreStatus()
	if status.Phase != CorePhaseTerminal || !status.Terminal ||
		status.Generation != 2 || status.Reason != "device_removed" ||
		!strings.Contains(status.LastError, openErr.Error()) {
		t.Fatalf("direct recovery terminal status = %+v", status)
	}
	if m.events.Dropped() == 0 {
		t.Fatal("closed EventEmitter did not drop direct terminal event")
	}
}

func TestCoreStatusLegacyRetryExhaustionCommitsTerminal(t *testing.T) {
	m := newRecoveryTestManager()
	m.cfg = normalizeConfig(Config{
		RecoveryPolicy: RecoveryPolicy{MaxRecoverAttempts: 1},
	})
	m.mu.Lock()
	m.lifetimeActive = true
	m.controlReady = true
	m.coreReady = true
	m.corePhase = CorePhaseReady
	m.recoverCount = 1
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.events.Close()
	defer func() { _ = m.Stop() }()

	m.scheduleRecoverRetry("legacy_exhaustion")
	status := m.CurrentCoreStatus()
	if status.Phase != CorePhaseTerminal || !status.Terminal ||
		status.Generation != 1 || status.Reason != "recovery_exhausted" ||
		status.Stage != "recovery_terminal" {
		t.Fatalf("legacy exhaustion terminal status = %+v", status)
	}
	m.modemResetMu.Lock()
	recoveryCleared := !m.modemResetRecovering && !m.modemResetPending &&
		!m.modemResetEnqueued && !m.coreRecoveryEnqueued && m.recoveryGeneration == 0
	m.modemResetMu.Unlock()
	if !recoveryCleared {
		t.Fatal("legacy terminal status published before recovery state was cleared")
	}
}

func TestCoreStatusEventEmitterFullAndConcurrentCancellation(t *testing.T) {
	emitter := NewEventEmitterWithQueueSize(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	emitter.On(func(Event) {
		enteredOnce.Do(func() { close(entered) })
		<-release
	})
	if !emitter.Emit(Event{Type: EventConnected}) {
		t.Fatal("failed to start blocking EventEmitter handler")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("EventEmitter handler did not start")
	}
	if !emitter.Emit(Event{Type: EventDisconnected}) {
		t.Fatal("failed to fill EventEmitter queue")
	}
	if emitter.Emit(Event{Type: EventDialFailed}) {
		t.Fatal("Emit succeeded with a full EventEmitter queue")
	}

	var m Manager
	m.events = emitter
	const subscribers = 16
	cancels := make([]context.CancelFunc, 0, subscribers)
	var readers sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		updates := m.SubscribeCoreStatus(ctx)
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range updates {
			}
		}()
	}

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 0; i < 200; i++ {
			m.mu.Lock()
			m.coreGeneration.Store(3)
			m.state = State(i % 3)
			m.setCorePhaseLocked(CorePhaseRecovering, "event_emitter_full", "test", nil)
			m.publishCoreStatusLocked()
			m.mu.Unlock()
			_ = emitter.Emit(Event{Type: EventCoreRecoveryFailed})
		}
	}()
	for _, cancel := range cancels {
		cancel()
	}
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("status publication blocked while EventEmitter was full")
	}
	readers.Wait()
	if status := m.CurrentCoreStatus(); status.Sequence != 200 || status.Stage != "event_emitter_full" {
		t.Fatalf("final concurrent status = %+v", status)
	}
	if emitter.Dropped() == 0 {
		t.Fatal("full EventEmitter did not drop any events")
	}
	close(release)
	emitter.Close()
}

func TestWaitIdentityReadyDoesNotTrustCoreReadyAlone(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	defer m.cancel()
	m.mu.Lock()
	m.markCoreReadyLocked("recover_converged")
	m.setCorePhaseLocked(CorePhaseReady, "recover_converged", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.getICCIDStrictHook = func(context.Context) (string, error) {
		return "", errors.New("ICCID unavailable")
	}
	m.getIMSIStrictHook = func(context.Context) (string, error) {
		return "", errors.New("IMSI unavailable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := m.WaitIdentityReady(ctx)
	if err == nil || !strings.Contains(err.Error(), "identity not readable") {
		t.Fatalf("WaitIdentityReady() error = %v, want unreadable live identity", err)
	}
	if status := m.CurrentCoreStatus(); !status.CoreReady || status.Phase != CorePhaseReady {
		t.Fatalf("identity test changed core readiness: %+v", status)
	}
}

func TestWaitIdentityReadyAcceptsIMSIWhileICCIDProbeBlocks(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()
	defer m.cancel()
	m.mu.Lock()
	m.markCoreReadyLocked("recover_converged")
	m.setCorePhaseLocked(CorePhaseReady, "recover_converged", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.getICCIDStrictHook = func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	m.getIMSIStrictHook = func(context.Context) (string, error) {
		return "460011234567890", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := m.WaitIdentityReady(ctx); err != nil {
		t.Fatalf("WaitIdentityReady() error = %v, want readable IMSI to win", err)
	}
	if elapsed := time.Since(started); elapsed > 450*time.Millisecond {
		t.Fatalf("WaitIdentityReady() took %v; ICCID consumed IMSI's deadline budget", elapsed)
	}
}
