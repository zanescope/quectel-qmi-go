package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func activateManagerForLifecycleTest(m *Manager, generation uint64) context.Context {
	runCtx, runCancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.ctx = runCtx
	m.cancel = runCancel
	m.state = StateDisconnected
	m.coreReady = true
	m.coreGeneration.Store(generation)
	m.mu.Unlock()
	return runCtx
}

func TestRequestDataConnectionLinearizesWithStop(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	activateManagerForLifecycleTest(m, 1)

	validated := make(chan struct{})
	releaseValidation := make(chan struct{})
	m.internalEventValidatedHook = func(event internalEvent, generation uint64) {
		if event != eventStart || generation != 1 {
			return
		}
		close(validated)
		<-releaseValidation
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- m.requestDataConnection()
	}()
	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("start request did not reach its validated enqueue boundary")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- m.Stop()
	}()
	select {
	case err := <-stopDone:
		close(releaseValidation)
		t.Fatalf("Stop completed before the validated start enqueue was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseValidation)

	if err := <-startDone; err != nil {
		t.Fatalf("requestDataConnection() error = %v", err)
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
		t.Fatalf("start event survived Stop drain: %v", event)
	default:
	}
}

func TestStopIsSingleFlightAndManagerIsTerminal(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	m.client = &qmi.Client{}

	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var closeCalls atomic.Int32
	m.closeQMIClientHook = func(*qmi.Client) error {
		closeCalls.Add(1)
		close(closeEntered)
		<-releaseClose
		return nil
	}

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- m.Stop() }()
	select {
	case <-closeEntered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not reach resource cleanup")
	}
	go func() { secondDone <- m.Stop() }()
	select {
	case err := <-secondDone:
		close(releaseClose)
		t.Fatalf("concurrent Stop returned before the single flight completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseClose)

	for index, done := range []<-chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Stop call %d error = %v", index+1, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Stop call %d did not complete", index+1)
		}
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("client close calls = %d, want 1", got)
	}

	if err := m.StartCore(); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("StartCore() error = %v, want ErrManagerStopped", err)
	}
	if err := m.Start(); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("Start() error = %v, want ErrManagerStopped", err)
	}
	if err := m.Connect(); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("Connect() error = %v, want ErrManagerStopped", err)
	}
}

func TestFailedStartupCanRetryBeforeStop(t *testing.T) {
	openErr := errors.New("first open failed")
	m := New(Config{}, NewNopLogger())
	var attempts atomic.Int32
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		if attempts.Add(1) == 1 {
			return openErr
		}
		return nil
	}
	m.checkSIMContextHook = func(context.Context) error { return nil }

	if err := m.StartCore(); !errors.Is(err, openErr) {
		t.Fatalf("first StartCore() error = %v, want %v", err, openErr)
	}
	if err := m.StartCore(); err != nil {
		t.Fatalf("second StartCore() error = %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("open attempts = %d, want 2", got)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestConnectedCommitCannotPublishAfterStop(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	runCtx := activateManagerForLifecycleTest(m, 7)
	m.mu.Lock()
	m.state = StateConnecting
	m.desiredConnection = true
	m.mu.Unlock()

	connected := make(chan Event, 1)
	m.OnEvent(func(event Event) {
		if event.Type == EventConnected {
			connected <- event
		}
	})

	enteredCommit := make(chan struct{})
	releaseCommit := make(chan struct{})
	m.beforeConnectCommitHook = func() {
		close(enteredCommit)
		<-releaseCommit
	}

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- m.commitConnected(
			runCtx,
			7,
			&qmi.RuntimeSettings{},
			1,
			true,
		)
	}()
	select {
	case <-enteredCommit:
	case <-time.After(time.Second):
		t.Fatal("connect did not reach its final commit boundary")
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	close(releaseCommit)

	select {
	case err := <-commitDone:
		if !errors.Is(err, ErrManagerStopped) {
			t.Fatalf("connected commit error = %v, want ErrManagerStopped", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connected commit did not return")
	}
	select {
	case event := <-connected:
		t.Fatalf("Connected event published after Stop: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
	if got := m.State(); got != StateDisconnected {
		t.Fatalf("state = %s, want disconnected", got)
	}
}

func TestFullInternalEventQueueDoesNotBlockStop(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	activateManagerForLifecycleTest(m, 1)
	m.eventCh = make(chan internalEventEnvelope, 1)
	m.eventCh <- internalEventEnvelope{kind: eventCheckTargeted, generation: 1}

	started := time.Now()
	if got := m.enqueueInternalEvent(eventCheckFull, 1); got != internalEventQueueFull {
		t.Fatalf("enqueue result = %v, want queue full", got)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("full-queue enqueue blocked for %v", elapsed)
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop blocked behind a full internal event queue")
	}
}

func TestStartCoreSIMCheckHonorsCallerCancellation(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	t.Cleanup(m.events.Close)
	m.openClientAndAllocateServicesHook = func(context.Context) error { return nil }

	enteredSIMCheck := make(chan struct{})
	m.checkSIMContextHook = func(ctx context.Context) error {
		close(enteredSIMCheck)
		<-ctx.Done()
		return ctx.Err()
	}

	callerCtx, callerCancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() {
		startDone <- m.StartCoreContext(callerCtx)
	}()
	select {
	case <-enteredSIMCheck:
	case <-time.After(time.Second):
		t.Fatal("startup did not reach SIM check")
	}
	callerCancel()

	select {
	case err := <-startDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StartCoreContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SIM check did not observe caller cancellation")
	}
	if m.stopped {
		t.Fatal("caller cancellation incorrectly made the manager terminal")
	}
}

func TestStopCancelsTemporaryWDSRelease(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	activateManagerForLifecycleTest(m, 3)
	m.client = &qmi.Client{}
	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		return &qmi.WDSService{}, nil
	}
	m.queryExistingPacketServiceState = func(context.Context, *qmi.WDSService) (qmi.ConnectionStatus, error) {
		return qmi.StatusDisconnected, nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }

	releaseEntered := make(chan struct{})
	var releaseCanceled atomic.Bool
	m.closeWDSServiceWithContext = func(ctx context.Context, _ *qmi.WDSService) error {
		close(releaseEntered)
		<-ctx.Done()
		releaseCanceled.Store(errors.Is(ctx.Err(), context.Canceled))
		return ctx.Err()
	}

	resetDone := make(chan error, 1)
	go func() {
		_, err := m.ResetExistingDataConnection(context.Background())
		resetDone <- err
	}()
	select {
	case <-releaseEntered:
	case <-time.After(time.Second):
		t.Fatal("temporary WDS release did not start")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()
	select {
	case err := <-resetDone:
		if err != nil {
			t.Fatalf("ResetExistingDataConnection() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("temporary WDS release did not observe Stop cancellation")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop remained blocked behind temporary WDS release")
	}
	if !releaseCanceled.Load() {
		t.Fatal("temporary WDS release did not receive a canceled context")
	}
}

func TestLatePendingResetPreventsRecoveryReadyCommit(t *testing.T) {
	m := newRecoveryTestManager()
	defer m.cancel()
	defer m.events.Close()
	m.cfg = normalizeConfig(Config{})
	m.state = StateDisconnected
	m.coreReady = true

	if !m.enqueueCoreRecoveryEvent(explicitRecoveryRequest("late-reset")) {
		t.Fatal("failed to enqueue core recovery")
	}
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventCoreRecovery {
		t.Fatalf("event = %v, want core recovery", event)
	}
	request, ok := m.beginRecovery(eventCoreRecovery)
	if !ok {
		t.Fatal("failed to begin core recovery")
	}

	configureSuccessfulCoreRecovery(m)
	m.beforeRecoveryCommitHook = func() {
		if !m.enqueueModemResetEvent("late-final-commit") {
			t.Fatal("failed to preserve late modem reset")
		}
	}
	if recovered := m.doRecoverCore(request); recovered {
		t.Fatal("recovery converged despite a late pending modem reset")
	}
	if m.IsCoreReady() {
		t.Fatal("coreReady published despite a late pending modem reset")
	}
	if got := m.coreRecoverySuccess.Load(); got != 0 {
		t.Fatalf("core recovery success count = %d, want 0", got)
	}
	if got := m.recoverSuccess.Load(); got != 0 {
		t.Fatalf("recover success count = %d, want 0", got)
	}

	m.finishRecovery()
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventModemReset {
		t.Fatalf("follow-up event = %v, want modem reset", event)
	}
}

func collectRecoveryEvents(t *testing.T, events <-chan Event, count int) []Event {
	t.Helper()
	result := make([]Event, 0, count)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(result) < count {
		select {
		case event := <-events:
			switch event.Type {
			case EventCoreRecoveryRequested, EventCoreRecoverySucceeded, EventCoreRecoveryFailed:
				result = append(result, event)
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d recovery events; got %+v", count, result)
		}
	}
	return result
}

func TestRecoveryEventsCarryTheirCoreGeneration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		m := newRecoveryTestManager()
		defer m.cancel()
		defer m.events.Close()
		m.cfg = normalizeConfig(Config{})
		m.state = StateDisconnected
		m.coreReady = true
		configureSuccessfulCoreRecovery(m)

		external := make(chan Event, 8)
		m.OnEvent(func(event Event) { external <- event })
		if !m.RequestCoreRecovery("generation-success") {
			t.Fatal("core recovery request was rejected")
		}
		event := waitInternalRecoveryEvent(t, m.eventCh, time.Second)
		m.handleRecoveryEvent(event)

		got := collectRecoveryEvents(t, external, 2)
		if got[0].Type != EventCoreRecoveryRequested || got[0].Generation != 1 {
			t.Fatalf("requested event = %+v, want generation 1", got[0])
		}
		if got[1].Type != EventCoreRecoverySucceeded || got[1].Generation != 2 {
			t.Fatalf("succeeded event = %+v, want generation 2", got[1])
		}
	})

	t.Run("failure", func(t *testing.T) {
		openErr := errors.New("reopen failed")
		m := newRecoveryTestManager()
		defer m.cancel()
		defer m.events.Close()
		m.cfg = normalizeConfig(Config{Device: ModemDevice{ControlPath: "."}})
		m.state = StateDisconnected
		m.coreReady = true
		m.openClientAndAllocateServicesHook = func(context.Context) error { return openErr }
		m.afterFunc = func(time.Duration, func()) *time.Timer {
			return time.NewTimer(time.Hour)
		}

		external := make(chan Event, 8)
		m.OnEvent(func(event Event) { external <- event })
		if !m.RequestCoreRecovery("generation-failure") {
			t.Fatal("core recovery request was rejected")
		}
		event := waitInternalRecoveryEvent(t, m.eventCh, time.Second)
		m.handleRecoveryEvent(event)

		got := collectRecoveryEvents(t, external, 2)
		if got[0].Type != EventCoreRecoveryRequested || got[0].Generation != 1 {
			t.Fatalf("requested event = %+v, want generation 1", got[0])
		}
		if got[1].Type != EventCoreRecoveryFailed || got[1].Generation != 2 {
			t.Fatalf("failed event = %+v, want generation 2", got[1])
		}
	})
}

func TestOpenGuardAfterPhaseReportsActualTransportMode(t *testing.T) {
	allocErr := errors.New("stop after open guard")
	var attemptsMu sync.Mutex
	var attempts []OpenAttempt
	m := New(Config{
		ClientOptions: qmi.ClientOptions{UseProxy: true},
		OpenGuard: func(_ context.Context, attempt OpenAttempt) error {
			attemptsMu.Lock()
			attempts = append(attempts, attempt)
			attemptsMu.Unlock()
			return nil
		},
	}, NewNopLogger())
	defer m.events.Close()
	m.openQMIClientHook = func(context.Context, string, qmi.ClientOptions) (*qmi.Client, error) {
		return &qmi.Client{}, nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }
	m.newNASService = func(context.Context, *qmi.Client) (*qmi.NASService, error) {
		return nil, allocErr
	}
	m.newDMSService = func(context.Context, *qmi.Client) (*qmi.DMSService, error) {
		return nil, allocErr
	}
	m.newUIMService = func(context.Context, *qmi.Client) (*qmi.UIMService, error) {
		return nil, allocErr
	}

	if err := m.openClientAndAllocateServices(context.Background(), OpenReasonInitial); err == nil {
		t.Fatal("openClientAndAllocateServices() error = nil, want injected allocation error")
	}
	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	if len(attempts) != 2 {
		t.Fatalf("open guard calls = %d, want 2", len(attempts))
	}
	if attempts[0].Phase != OpenPhaseBefore || !attempts[0].UseProxy {
		t.Fatalf("before attempt = %+v, want requested proxy mode", attempts[0])
	}
	if attempts[1].Phase != OpenPhaseAfter || attempts[1].UseProxy {
		t.Fatalf("after attempt = %+v, want actual raw mode", attempts[1])
	}
}
