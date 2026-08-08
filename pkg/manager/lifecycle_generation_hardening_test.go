package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func TestFinishRecoveryAndStopCannotRepublishPendingWork(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.state = StateDisconnected
	m.coreReady = true
	m.coreGeneration.Store(7)
	m.mu.Unlock()

	enteredFinish := make(chan struct{})
	m.finishRecoveryLockedHook = func() {
		close(enteredFinish)
	}

	m.modemResetMu.Lock()
	m.modemResetRecovering = true
	m.modemResetPending = true
	m.recoveryGeneration = 7

	finishDone := make(chan struct{})
	go func() {
		m.finishRecovery()
		close(finishDone)
	}()

	select {
	case <-enteredFinish:
	case <-time.After(time.Second):
		m.modemResetMu.Unlock()
		t.Fatal("finishRecovery did not reach the recovery-state boundary")
	}

	stopStarted := make(chan struct{})
	stopDone := make(chan error, 1)
	go func() {
		close(stopStarted)
		stopDone <- m.Stop()
	}()
	<-stopStarted
	m.modemResetMu.Unlock()

	select {
	case <-finishDone:
	case <-time.After(time.Second):
		t.Fatal("finishRecovery did not complete")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}

	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	if m.modemResetRecovering ||
		m.modemResetPending ||
		m.modemResetEnqueued ||
		m.coreRecoveryEnqueued ||
		m.modemResetDeferred ||
		m.recoveryGeneration != 0 {
		t.Fatalf(
			"recovery state survived Stop: recovering=%v pending=%v reset_enqueued=%v core_enqueued=%v deferred=%v generation=%d",
			m.modemResetRecovering,
			m.modemResetPending,
			m.modemResetEnqueued,
			m.coreRecoveryEnqueued,
			m.modemResetDeferred,
			m.recoveryGeneration,
		)
	}
	select {
	case event := <-m.eventCh:
		t.Fatalf("retired recovery event remained queued after Stop: %v", event)
	default:
	}
	if m.IsCoreReady() {
		t.Fatal("core readiness survived Stop")
	}
	if got := m.State(); got != StateDisconnected {
		t.Fatalf("state = %s, want disconnected", got)
	}
}

func TestStopDrainsRecoveryEventValidatedBeforeShutdown(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.state = StateDisconnected
	m.coreReady = true
	m.coreGeneration.Store(9)
	m.mu.Unlock()

	m.modemResetMu.Lock()
	m.coreRecoveryEnqueued = true
	m.coreRecoveryRequest = explicitRecoveryRequest("validated-before-stop")
	m.recoveryGeneration = 9
	m.modemResetMu.Unlock()

	validated := make(chan struct{})
	releaseValidation := make(chan struct{})
	m.recoveryEventValidatedHook = func() {
		close(validated)
		<-releaseValidation
	}

	signalDone := make(chan struct{})
	go func() {
		m.signalRecoveryEvent(eventCoreRecovery, 9)
		close(signalDone)
	}()
	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("recovery event did not reach its validated enqueue boundary")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()
	select {
	case err := <-stopDone:
		close(releaseValidation)
		t.Fatalf("Stop completed before validated enqueue was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseValidation)

	select {
	case <-signalDone:
	case <-time.After(time.Second):
		t.Fatal("recovery event enqueue did not complete")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}

	select {
	case event := <-m.eventCh:
		t.Fatalf("validated recovery event remained queued after Stop: %v", event)
	default:
	}
}

func TestStaleRecoveryGenerationIsIgnored(t *testing.T) {
	m := newRecoveryTestManager()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	defer m.cancel()
	defer m.events.Close()
	m.state = StateDisconnected
	m.coreReady = true
	m.coreGeneration.Store(2)
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		t.Fatal("stale recovery must not reach transport open")
		return nil
	}

	m.modemResetMu.Lock()
	m.coreRecoveryEnqueued = true
	m.coreRecoveryRequest = explicitRecoveryRequest("retired")
	m.recoveryGeneration = 1
	m.modemResetMu.Unlock()

	m.handleRecoveryEvent(eventCoreRecovery)

	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	if m.modemResetRecovering ||
		m.modemResetEnqueued ||
		m.coreRecoveryEnqueued ||
		m.modemResetDeferred ||
		m.recoveryGeneration != 0 {
		t.Fatalf(
			"stale recovery state was not cleared: recovering=%v reset_enqueued=%v core_enqueued=%v deferred=%v generation=%d",
			m.modemResetRecovering,
			m.modemResetEnqueued,
			m.coreRecoveryEnqueued,
			m.modemResetDeferred,
			m.recoveryGeneration,
		)
	}
	if got := m.recoverAttempts.Load(); got != 0 {
		t.Fatalf("recovery attempts = %d, want 0", got)
	}
}

func TestPendingResetBeforeRecoveryGenerationBumpIsPreserved(t *testing.T) {
	m := newRecoveryTestManager()
	m.cfg = normalizeConfig(Config{})
	m.ctx, m.cancel = context.WithCancel(context.Background())
	defer m.cancel()
	defer m.events.Close()
	m.state = StateDisconnected
	m.coreReady = true
	m.coreGeneration.Store(11)

	if !m.enqueueCoreRecoveryEvent(explicitRecoveryRequest("pending-before-bump")) {
		t.Fatal("failed to enqueue initial core recovery")
	}
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventCoreRecovery {
		t.Fatalf("initial event = %v, want core recovery", event)
	}
	request, ok := m.beginRecovery(eventCoreRecovery)
	if !ok {
		t.Fatal("failed to begin initial core recovery")
	}
	if !m.enqueueModemResetEvent("pending-before-bump") {
		t.Fatal("failed to preserve modem reset during active recovery")
	}

	m.openClientAndAllocateServicesHook = func(context.Context) error { return nil }
	m.checkSIMHook = func() error { return nil }
	m.modemResetQuietWindow = time.Millisecond

	if recovered := m.doRecoverCore(request); recovered {
		t.Fatal("recovery unexpectedly converged while a reset was pending")
	}
	if got := m.coreGeneration.Load(); got != 12 {
		t.Fatalf("core generation = %d, want 12", got)
	}
	m.modemResetMu.Lock()
	if !m.modemResetRecovering || !m.modemResetPending || m.recoveryGeneration != 12 {
		m.modemResetMu.Unlock()
		t.Fatalf(
			"pending reset was not migrated: recovering=%v pending=%v generation=%d",
			m.modemResetRecovering,
			m.modemResetPending,
			m.recoveryGeneration,
		)
	}
	m.modemResetMu.Unlock()

	m.finishRecovery()

	m.modemResetMu.Lock()
	if m.modemResetRecovering ||
		m.modemResetPending ||
		!m.modemResetEnqueued ||
		m.recoveryGeneration != 12 {
		m.modemResetMu.Unlock()
		t.Fatalf(
			"pending reset was not re-enqueued: recovering=%v pending=%v enqueued=%v generation=%d",
			m.modemResetRecovering,
			m.modemResetPending,
			m.modemResetEnqueued,
			m.recoveryGeneration,
		)
	}
	m.modemResetMu.Unlock()
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventModemReset {
		t.Fatalf("follow-up event = %v, want modem reset", event)
	}
}

func TestStopWinsBeforeRecoveryFinalCommit(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.state = StateDisconnected
	m.coreReady = true
	m.mu.Unlock()
	m.coreGeneration.Store(1)
	request := recoveryRequest{kind: recoveryKindSoftware, reason: recoveryReasonExplicitRequest, detail: "stop-before-commit", generation: 1}
	configureSuccessfulCoreRecovery(m)

	enteredCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	m.beforeRecoveryCommitHook = func() {
		close(enteredCommit)
		<-releaseCommit
	}

	recoveryDone := make(chan bool, 1)
	go func() {
		recoveryDone <- m.doRecoverCore(request)
	}()

	select {
	case <-enteredCommit:
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach the final commit boundary")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	deadline := time.Now().Add(time.Second)
	for m.State() != StateStopping && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.State(); got != StateStopping {
		close(releaseCommit)
		t.Fatalf("state = %s, want stopping before releasing recovery", got)
	}
	close(releaseCommit)

	select {
	case recovered := <-recoveryDone:
		if recovered {
			t.Fatal("recovery published success after Stop won the final commit")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not return")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}

	if m.IsCoreReady() {
		t.Fatal("core readiness resurrected after Stop")
	}
	if got := m.recoverSuccess.Load(); got != 0 {
		t.Fatalf("recovery success count = %d, want 0", got)
	}
}

func TestResetExistingDataConnectionCancellationDoesNotBlockStop(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.state = StateDisconnected
	m.coreReady = true
	m.client = &qmi.Client{}
	m.coreGeneration.Store(3)
	m.mu.Unlock()

	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		return &qmi.WDSService{}, nil
	}
	m.closeWDSService = func(*qmi.WDSService) error { return nil }
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }

	enteredQuery := make(chan struct{})
	m.queryExistingPacketServiceState = func(ctx context.Context, _ *qmi.WDSService) (qmi.ConnectionStatus, error) {
		close(enteredQuery)
		<-ctx.Done()
		return qmi.StatusUnknown, ctx.Err()
	}

	resetDone := make(chan error, 1)
	go func() {
		_, err := m.ResetExistingDataConnection(context.Background())
		resetDone <- err
	}()

	select {
	case <-enteredQuery:
	case <-time.After(time.Second):
		t.Fatal("reset did not reach blocked QMI query")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()

	select {
	case err := <-resetDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ResetExistingDataConnection() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reset did not observe manager lifetime cancellation")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop remained blocked behind canceled QMI I/O")
	}
}

func TestScheduledTimerFromRetiredEpochIsIgnored(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.events.Close()

	var wrapped func()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer {
		wrapped = fn
		return timer
	}

	var calls atomic.Int32
	m.scheduleAfter(time.Second, func() {
		calls.Add(1)
	})
	if wrapped == nil {
		t.Fatal("scheduleAfter did not install a callback")
	}

	m.stopScheduledTimers()
	wrapped()

	if got := calls.Load(); got != 0 {
		t.Fatalf("retired timer epoch calls = %d, want 0", got)
	}
	if got := m.staleTimerIgnored.Load(); got != 1 {
		t.Fatalf("stale timers ignored = %d, want 1", got)
	}
}

func TestClaimedTimerCompletesBeforeStartAdvancesGeneration(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	defer func() { _ = m.Stop() }()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.coreGeneration.Store(1)

	var wrapped func()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	m.afterFunc = func(_ time.Duration, fn func()) *time.Timer {
		wrapped = fn
		return timer
	}

	claimed := make(chan struct{})
	releaseClaim := make(chan struct{})
	m.scheduledTimerClaimedHook = func() {
		close(claimed)
		<-releaseClaim
	}
	var calls atomic.Int32
	m.scheduleAfter(time.Second, func() {
		calls.Add(1)
	})

	callbackDone := make(chan struct{})
	go func() {
		wrapped()
		close(callbackDone)
	}()
	select {
	case <-claimed:
	case <-time.After(time.Second):
		t.Fatal("timer callback did not claim its generation")
	}

	openStarted := make(chan struct{})
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		close(openStarted)
		return nil
	}
	m.checkSIMContextHook = func(context.Context) error { return nil }
	startDone := make(chan error, 1)
	go func() {
		startDone <- m.StartCoreContext(context.Background())
	}()

	select {
	case err := <-startDone:
		close(releaseClaim)
		t.Fatalf("StartCoreContext completed before claimed callback: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := m.coreGeneration.Load(); got != 1 {
		close(releaseClaim)
		t.Fatalf("core generation advanced while prior callback was claimed: %d", got)
	}
	select {
	case <-openStarted:
		close(releaseClaim)
		t.Fatal("transport open began before prior callback completed")
	default:
	}
	close(releaseClaim)

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("claimed timer callback did not complete")
	}
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("StartCoreContext() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartCoreContext did not advance after timer callback completed")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("timer callback calls = %d, want 1", got)
	}
	if got := m.coreGeneration.Load(); got != 2 {
		t.Fatalf("core generation = %d, want 2", got)
	}
}

func TestOpenGuardPostRejectClosesOpenedClient(t *testing.T) {
	guardErr := errors.New("ownership changed after open")
	client := &qmi.Client{}
	var phases []OpenPhase
	var closeCalls atomic.Int32
	m := New(Config{
		OpenGuard: func(_ context.Context, attempt OpenAttempt) error {
			phases = append(phases, attempt.Phase)
			if attempt.Phase == OpenPhaseAfter {
				return guardErr
			}
			return nil
		},
	}, NewNopLogger())
	defer m.events.Close()
	m.openQMIClientHook = func(context.Context, string, qmi.ClientOptions) (*qmi.Client, error) {
		return client, nil
	}
	m.closeQMIClientHook = func(got *qmi.Client) error {
		if got != client {
			t.Fatalf("closed client = %p, want %p", got, client)
		}
		closeCalls.Add(1)
		return nil
	}

	err := m.openClientAndAllocateServices(context.Background(), OpenReasonRecovery)
	if !errors.Is(err, guardErr) {
		t.Fatalf("open error = %v, want %v", err, guardErr)
	}
	if len(phases) != 2 || phases[0] != OpenPhaseBefore || phases[1] != OpenPhaseAfter {
		t.Fatalf("guard phases = %v, want [before after]", phases)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("client close calls = %d, want 1", got)
	}
	if m.client != nil {
		t.Fatal("rejected client was published on manager")
	}
}

func TestOpenGuardPostCancellationClosesOpenedClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &qmi.Client{}
	var closeCalls atomic.Int32
	m := New(Config{
		OpenGuard: func(_ context.Context, attempt OpenAttempt) error {
			if attempt.Phase == OpenPhaseAfter {
				cancel()
			}
			return nil
		},
	}, NewNopLogger())
	defer m.events.Close()
	m.openQMIClientHook = func(context.Context, string, qmi.ClientOptions) (*qmi.Client, error) {
		return client, nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error {
		closeCalls.Add(1)
		return nil
	}

	err := m.openClientAndAllocateServices(ctx, OpenReasonInitial)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("open error = %v, want context.Canceled", err)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("client close calls = %d, want 1", got)
	}
	if m.client != nil {
		t.Fatal("canceled client was published on manager")
	}
}
