package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func newActiveDataLifecycleTestManager(t *testing.T, cfg Config) (*Manager, coreSessionToken) {
	t.Helper()
	if cfg.Timeouts.Dial <= 0 {
		cfg.Timeouts.Dial = time.Second
	}
	if cfg.Timeouts.Stop <= 0 {
		cfg.Timeouts.Stop = time.Second
	}
	if cfg.Timeouts.StatusCheck <= 0 {
		cfg.Timeouts.StatusCheck = time.Second
	}
	client := &qmi.Client{}
	runCtx, runCancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:               cfg,
		log:               NewNopLogger(),
		client:            client,
		ctx:               runCtx,
		cancel:            runCancel,
		lifetimeActive:    true,
		coreReady:         true,
		desiredConnection: true,
		state:             StateConnected,
		eventCh:           make(chan internalEventEnvelope, 8),
		events:            NewEventEmitterWithQueueSize(32),
		scheduledTimers:   make(map[*time.Timer]struct{}),
		retryDelays:       []time.Duration{time.Millisecond},
	}
	m.coreGeneration.Store(1)
	t.Cleanup(func() {
		runCancel()
		m.events.Close()
	})
	return m, coreSessionToken{generation: 1, client: client, runCtx: runCtx}
}

func installQuietDialQueries(m *Manager) {
	m.querySignalStrength = func(context.Context) (*qmi.SignalStrength, error) {
		return nil, errors.New("signal unavailable in test")
	}
	m.queryServingSystem = func(context.Context) (*qmi.ServingSystem, error) {
		return nil, errors.New("serving system unavailable in test")
	}
	m.queryNASRegistered = func(context.Context) (bool, error) { return true, nil }
}

func waitForLifecycleEvent(t *testing.T, ch <-chan Event, want EventType) Event {
	t.Helper()
	select {
	case event := <-ch:
		if event.Type != want {
			t.Fatalf("event = %s, want %s", event.Type, want)
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", want)
		return Event{}
	}
}

func TestRotateV4FailurePreservesV6AndRepairsOnlyMissingLeg(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{
		EnableIPv4:    true,
		EnableIPv6:    true,
		AutoReconnect: true,
	})
	wdsV4, wdsV6 := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds, m.wdsV6 = wdsV4, wdsV6
	m.handleV4, m.handleV6 = 41, 61
	m.settings = &qmi.RuntimeSettings{
		IPv4Address: net.ParseIP("10.0.0.1"),
		IPv6Address: net.ParseIP("2001:db8::1"),
		IPv6Prefix:  64,
	}

	var startV4, startV6 atomic.Int32
	m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
		switch family {
		case qmi.IpFamilyV4:
			if gotWDS != wdsV4 {
				t.Errorf("IPv4 start used unexpected WDS: %p", gotWDS)
			}
			return uint32(101 + startV4.Add(1)), nil
		case qmi.IpFamilyV6:
			startV6.Add(1)
			return 201, nil
		default:
			return 0, errors.New("unexpected family")
		}
	}
	var stoppedMu sync.Mutex
	var stopped []uint32
	m.stopDataCallHook = func(_ context.Context, _ *qmi.WDSService, handle uint32) error {
		stoppedMu.Lock()
		stopped = append(stopped, handle)
		stoppedMu.Unlock()
		return nil
	}
	m.flushRotationAddressesHook = func(string) error { return nil }
	m.getRotationRuntimeSettingsHook = func(context.Context, *qmi.WDSService) (*qmi.RuntimeSettings, error) {
		return &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.2")}, nil
	}
	configureErr := errors.New("configure failed")
	var configureCalls atomic.Int32
	m.configureNetworkHook = func() error {
		if configureCalls.Add(1) == 1 {
			return configureErr
		}
		return nil
	}
	installQuietDialQueries(m)

	gotEvents := make(chan Event, 4)
	m.OnEvent(func(event Event) {
		if event.Type == EventDisconnected || event.Type == EventReconnecting {
			gotEvents <- event
		}
	})

	if err := m.RotateIP(); !errors.Is(err, configureErr) {
		t.Fatalf("RotateIP error = %v, want %v", err, configureErr)
	}
	if m.State() != StateDisconnected {
		t.Fatalf("state = %s, want Disconnected", m.State())
	}
	m.mu.RLock()
	if m.handleV4 != 0 || m.handleV6 != 61 || m.settings != nil || !m.desiredConnection {
		t.Fatalf("failed rotation state: h4=%d h6=%d settings=%v desired=%v", m.handleV4, m.handleV6, m.settings, m.desiredConnection)
	}
	m.mu.RUnlock()
	stoppedMu.Lock()
	if len(stopped) != 2 || stopped[0] != 41 || stopped[1] != 102 {
		t.Fatalf("stopped handles = %v, want [41 102]", stopped)
	}
	stoppedMu.Unlock()
	if startV6.Load() != 0 {
		t.Fatalf("IPv6 starts during direct rotation = %d, want 0", startV6.Load())
	}
	waitForLifecycleEvent(t, gotEvents, EventDisconnected)
	waitForLifecycleEvent(t, gotEvents, EventReconnecting)

	var envelope internalEventEnvelope
	select {
	case envelope = <-m.eventCh:
		if envelope.kind != eventStart || envelope.generation != 1 {
			t.Fatalf("repair envelope = %+v, want eventStart generation 1", envelope)
		}
	default:
		t.Fatal("missing durable reconnect event")
	}
	select {
	case extra := <-m.eventCh:
		t.Fatalf("unexpected duplicate reconnect event: %+v", extra)
	default:
	}

	m.handleEventForGeneration(envelope.kind, envelope.generation)
	m.mu.RLock()
	gotState, gotV4, gotV6 := m.state, m.handleV4, m.handleV6
	m.mu.RUnlock()
	if gotState != StateConnected || gotV4 != 103 || gotV6 != 61 {
		t.Fatalf("repair result: state=%s h4=%d h6=%d, want Connected/103/61", gotState, gotV4, gotV6)
	}
	if startV4.Load() != 2 || startV6.Load() != 0 {
		t.Fatalf("start counts after repair: v4=%d v6=%d, want 2/0", startV4.Load(), startV6.Load())
	}
}

func TestRadioResetInvalidatesBothLegsAndRepairDialsBoth(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{
		EnableIPv4:    true,
		EnableIPv6:    true,
		AutoReconnect: true,
	})
	wdsV4, wdsV6 := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds, m.wdsV6 = wdsV4, wdsV6
	m.handleV4, m.handleV6 = 41, 61
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1"), IPv6Address: net.ParseIP("2001:db8::1")}

	var offCalls, onCalls, flushCalls atomic.Int32
	m.radioPowerCommandHook = func(_ context.Context, _ coreSessionToken, on bool, _ string) error {
		if !on {
			offCalls.Add(1)
			return nil
		}
		onCalls.Add(1)
		m.mu.RLock()
		defer m.mu.RUnlock()
		if m.handleV4 != 0 || m.handleV6 != 0 || m.settings != nil || m.state == StateConnected {
			t.Errorf("radio-on observed stale data plane: state=%s h4=%d h6=%d settings=%v", m.state, m.handleV4, m.handleV6, m.settings)
		}
		return nil
	}
	m.flushRadioDataPlaneHook = func(string) error {
		flushCalls.Add(1)
		return nil
	}
	var startV4, startV6 atomic.Int32
	m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
		switch family {
		case qmi.IpFamilyV4:
			startV4.Add(1)
			if gotWDS != wdsV4 {
				t.Errorf("IPv4 repair used wrong WDS")
			}
			return 71, nil
		case qmi.IpFamilyV6:
			startV6.Add(1)
			if gotWDS != wdsV6 {
				t.Errorf("IPv6 repair used wrong WDS")
			}
			return 81, nil
		default:
			return 0, errors.New("unexpected family")
		}
	}
	m.configureNetworkHook = func() error { return nil }
	installQuietDialQueries(m)

	if err := m.RadioReset(); err != nil {
		t.Fatalf("RadioReset failed: %v", err)
	}
	if offCalls.Load() != 1 || onCalls.Load() != 1 || flushCalls.Load() != 1 {
		t.Fatalf("radio calls off/on/flush = %d/%d/%d, want 1/1/1", offCalls.Load(), onCalls.Load(), flushCalls.Load())
	}
	m.mu.RLock()
	if m.state != StateDisconnected || m.handleV4 != 0 || m.handleV6 != 0 || m.settings != nil {
		t.Fatalf("post-reset state=%s h4=%d h6=%d settings=%v", m.state, m.handleV4, m.handleV6, m.settings)
	}
	m.mu.RUnlock()

	var envelope internalEventEnvelope
	select {
	case envelope = <-m.eventCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for radio repair event")
	}
	if envelope.kind != eventStart || envelope.generation != 1 {
		t.Fatalf("repair envelope = %+v, want eventStart generation 1", envelope)
	}
	m.handleEventForGeneration(envelope.kind, envelope.generation)
	m.mu.RLock()
	gotState, gotV4, gotV6 := m.state, m.handleV4, m.handleV6
	m.mu.RUnlock()
	if gotState != StateConnected || gotV4 != 71 || gotV6 != 81 {
		t.Fatalf("radio repair result: state=%s h4=%d h6=%d", gotState, gotV4, gotV6)
	}
	if startV4.Load() != 1 || startV6.Load() != 1 {
		t.Fatalf("repair start counts = %d/%d, want 1/1", startV4.Load(), startV6.Load())
	}

	m.handleEventForGeneration(eventStart, 1)
	if startV4.Load() != 1 || startV6.Load() != 1 {
		t.Fatalf("full-handle eventStart redialed: v4=%d v6=%d", startV4.Load(), startV6.Load())
	}
}

func TestRadioPowerOnFailureKeepsBothLegsInvalidated(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true, EnableIPv6: true})
	m.wds, m.wdsV6 = &qmi.WDSService{}, &qmi.WDSService{}
	m.handleV4, m.handleV6 = 41, 61
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
	m.flushRadioDataPlaneHook = func(string) error { return nil }
	powerOnErr := errors.New("radio on failed")
	m.radioPowerCommandHook = func(_ context.Context, _ coreSessionToken, on bool, _ string) error {
		if on {
			return powerOnErr
		}
		return nil
	}

	if err := m.RadioReset(); !errors.Is(err, powerOnErr) {
		t.Fatalf("RadioReset error = %v, want %v", err, powerOnErr)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != StateDisconnected || m.handleV4 != 0 || m.handleV6 != 0 || m.settings != nil {
		t.Fatalf("power-on failure restored stale data: state=%s h4=%d h6=%d settings=%v", m.state, m.handleV4, m.handleV6, m.settings)
	}
}

func TestRadioOffCancellationStillAttemptsFreshPowerOn(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true, EnableIPv6: true})
	m.wds, m.wdsV6 = &qmi.WDSService{}, &qmi.WDSService{}
	m.handleV4, m.handleV6 = 41, 61
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
	m.flushRadioDataPlaneHook = func(string) error { return nil }

	operationCtx, cancelOperation := context.WithCancel(token.runCtx)
	defer cancelOperation()
	var onCalls atomic.Int32
	m.radioPowerCommandHook = func(ctx context.Context, _ coreSessionToken, on bool, _ string) error {
		if !on {
			cancelOperation()
			return nil
		}
		onCalls.Add(1)
		if ctx.Err() != nil {
			t.Errorf("fresh power-on context was already canceled: %v", ctx.Err())
		}
		return nil
	}

	result, err := m.radioPowerCycleForSession(operationCtx, token, "test.cancelAfterOff")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("radioPowerCycle error = %v, want context.Canceled", err)
	}
	if !result.dataInvalidated || !result.hadData {
		t.Fatalf("radio result = %+v, want invalidated data", result)
	}
	if onCalls.Load() != 1 {
		t.Fatalf("fresh power-on calls = %d, want 1", onCalls.Load())
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != StateDisconnected || m.handleV4 != 0 || m.handleV6 != 0 || m.settings != nil {
		t.Fatalf("canceled cycle retained stale data: state=%s h4=%d h6=%d settings=%v", m.state, m.handleV4, m.handleV6, m.settings)
	}
}

func TestStopWinsRotateCandidateCommitRollsBackWithoutEvents(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{
		EnableIPv4:    true,
		EnableIPv6:    true,
		AutoReconnect: true,
	})
	wdsV4, wdsV6 := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds, m.wdsV6 = wdsV4, wdsV6
	m.handleV6 = 61
	m.settings = &qmi.RuntimeSettings{IPv6Address: net.ParseIP("2001:db8::1")}
	m.flushRotationAddressesHook = func(string) error { return nil }

	startEntered := make(chan struct{})
	var startOnce sync.Once
	m.startDataCallHook = func(ctx context.Context, _ *qmi.WDSService, family uint8) (uint32, error) {
		if family != qmi.IpFamilyV4 {
			return 0, errors.New("unexpected family")
		}
		startOnce.Do(func() { close(startEntered) })
		<-ctx.Done()
		// Simulate a late QMI success after cancellation. Exact-session commit
		// must reject it and the caller must own candidate rollback.
		return 77, nil
	}
	candidateRolledBack := make(chan struct{})
	var rollbackOnce sync.Once
	m.stopDataCallHook = func(_ context.Context, _ *qmi.WDSService, handle uint32) error {
		if handle == 77 {
			rollbackOnce.Do(func() { close(candidateRolledBack) })
		}
		return nil
	}
	m.stopWDSForCleanup = func(context.Context, *qmi.WDSService, uint32) error { return nil }
	m.closeWDSService = func(*qmi.WDSService) error { return nil }
	var closeBeforeRollback atomic.Bool
	m.closeQMIClientHook = func(*qmi.Client) error {
		select {
		case <-candidateRolledBack:
		default:
			closeBeforeRollback.Store(true)
		}
		return nil
	}
	var forbiddenEvents atomic.Int32
	m.OnEvent(func(event Event) {
		switch event.Type {
		case EventIPChanged, EventDisconnected, EventReconnecting, EventConnected:
			forbiddenEvents.Add(1)
		}
	})

	rotateDone := make(chan error, 1)
	go func() { rotateDone <- m.RotateIP() }()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("RotateIP did not reach candidate start")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()

	select {
	case err := <-rotateDone:
		if err == nil {
			t.Fatal("RotateIP unexpectedly succeeded after Stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RotateIP did not return after Stop cancellation")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not join RotateIP")
	}
	select {
	case <-candidateRolledBack:
	default:
		t.Fatal("late rotation candidate was not rolled back")
	}
	if closeBeforeRollback.Load() {
		t.Fatal("transport closed before late candidate rollback completed")
	}
	if forbiddenEvents.Load() != 0 {
		t.Fatalf("observed %d lifecycle events after Stop won", forbiddenEvents.Load())
	}
	if m.State() != StateDisconnected {
		t.Fatalf("final state = %s, want Disconnected", m.State())
	}
}

func TestIPv6OnlyConnectedWithoutHandleDoesNotBypassDial(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{EnableIPv6: true})
	m.wds = nil
	m.wdsV6 = &qmi.WDSService{}
	m.handleV4, m.handleV6 = 0, 0
	installQuietDialQueries(m)
	dialErr := errors.New("IPv6 unavailable")
	var starts atomic.Int32
	m.startDataCallHook = func(context.Context, *qmi.WDSService, uint8) (uint32, error) {
		starts.Add(1)
		return 0, dialErr
	}
	m.configureNetworkHook = func() error {
		t.Fatal("configureNetwork called without a live IPv6 handle")
		return nil
	}

	if err := m.doConnectForGeneration(1); !errors.Is(err, dialErr) {
		t.Fatalf("doConnect error = %v, want %v", err, dialErr)
	}
	if starts.Load() != 1 {
		t.Fatalf("IPv6 starts = %d, want 1", starts.Load())
	}
	if m.State() == StateConnected {
		t.Fatal("IPv6-only dial failure left manager Connected")
	}
}

func TestRotateIPRejectsIPv4DisabledWithoutSideEffects(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{EnableIPv6: true, AutoReconnect: true})
	staleWDSV4, wdsV6 := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds, m.wdsV6 = staleWDSV4, wdsV6
	m.handleV4, m.handleV6 = 41, 61
	settings := &qmi.RuntimeSettings{IPv6Address: net.ParseIP("2001:db8::1")}
	m.settings = settings
	var sideEffects atomic.Int32
	m.startDataCallHook = func(context.Context, *qmi.WDSService, uint8) (uint32, error) {
		sideEffects.Add(1)
		return 0, nil
	}
	m.stopDataCallHook = func(context.Context, *qmi.WDSService, uint32) error {
		sideEffects.Add(1)
		return nil
	}
	m.flushRotationAddressesHook = func(string) error {
		sideEffects.Add(1)
		return nil
	}

	if err := m.RotateIP(); err == nil {
		t.Fatal("RotateIP succeeded with IPv4 disabled")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != StateConnected || m.wds != staleWDSV4 || m.handleV4 != 41 || m.handleV6 != 61 || m.settings != settings || m.isRotating {
		t.Fatalf("IPv4-disabled RotateIP mutated state: state=%s h4=%d h6=%d settingsSame=%v rotating=%v", m.state, m.handleV4, m.handleV6, m.settings == settings, m.isRotating)
	}
	if sideEffects.Load() != 0 {
		t.Fatalf("IPv4-disabled RotateIP performed %d side effects", sideEffects.Load())
	}
}

func TestDisconnectDuringRotationFailureFlushSuppressesReconnect(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true, AutoReconnect: true})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.handleV4 = 0
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
	m.isRotating = true

	flushEntered := make(chan struct{})
	releaseFlush := make(chan struct{})
	var flushOnce sync.Once
	m.flushRotationAddressesHook = func(string) error {
		flushOnce.Do(func() { close(flushEntered) })
		<-releaseFlush
		return nil
	}
	reconnecting := make(chan Event, 1)
	m.OnEvent(func(event Event) {
		if event.Type == EventReconnecting {
			reconnecting <- event
		}
	})

	finishDone := make(chan struct{})
	go func() {
		m.finishIPRotation(token, wds, true, errors.New("rotation failed"))
		close(finishDone)
	}()
	select {
	case <-flushEntered:
	case <-time.After(time.Second):
		t.Fatal("rotation failure did not reach host flush")
	}

	if err := m.Disconnect(); err != nil {
		t.Fatalf("Disconnect failed during host flush: %v", err)
	}
	close(releaseFlush)
	select {
	case <-finishDone:
	case <-time.After(time.Second):
		t.Fatal("rotation finalizer did not finish")
	}
	m.mu.RLock()
	desired := m.desiredConnection
	m.mu.RUnlock()
	if desired {
		t.Fatal("Disconnect did not clear desiredConnection")
	}
	select {
	case event := <-reconnecting:
		t.Fatalf("unexpected reconnect notification after Disconnect: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case envelope := <-m.eventCh:
		t.Fatalf("unexpected reconnect event after Disconnect: %+v", envelope)
	default:
	}
}

func TestRotationFailureWithoutReconnectPolicyDoesNotQueue(t *testing.T) {
	for _, tc := range []struct {
		name    string
		auto    bool
		desired bool
	}{
		{name: "auto_reconnect_disabled", desired: true},
		{name: "connection_not_desired", auto: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true, EnableIPv6: true, AutoReconnect: tc.auto})
			wds := &qmi.WDSService{}
			m.wds, m.wdsV6 = wds, &qmi.WDSService{}
			m.handleV6 = 61
			m.desiredConnection = tc.desired
			m.isRotating = true
			m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
			m.flushRotationAddressesHook = func(string) error { return nil }

			m.finishIPRotation(token, wds, true, errors.New("rotation failed"))
			if m.State() != StateDisconnected {
				t.Fatalf("state = %s, want Disconnected", m.State())
			}
			m.mu.RLock()
			if m.handleV6 != 61 || m.settings != nil || m.desiredConnection != tc.desired {
				t.Fatalf("fail-close state: h6=%d settings=%v desired=%v", m.handleV6, m.settings, m.desiredConnection)
			}
			m.mu.RUnlock()
			select {
			case envelope := <-m.eventCh:
				t.Fatalf("unexpected reconnect event: %+v", envelope)
			default:
			}
		})
	}
}

func TestDisconnectStopFailureMovesHandleToPendingAndRetries(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{
		EnableIPv4: true,
		Device:     ModemDevice{NetInterface: "wwan0"},
	})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.handleV4 = 41
	m.muxIface = "qmimux7"
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}

	stopErr := errors.New("modem stop failed")
	var stopMu sync.Mutex
	var stopped []uint32
	m.stopDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, handle uint32) error {
		if gotWDS != wds {
			t.Errorf("stop used WDS %p, want %p", gotWDS, wds)
		}
		stopMu.Lock()
		defer stopMu.Unlock()
		stopped = append(stopped, handle)
		if len(stopped) == 1 {
			return stopErr
		}
		return nil
	}
	var cleanedIfaces []string
	m.disconnectHostCleanupHook = func(ifname string) error {
		cleanedIfaces = append(cleanedIfaces, ifname)
		return nil
	}

	if err := m.Disconnect(); !errors.Is(err, stopErr) {
		t.Fatalf("first Disconnect error = %v, want %v", err, stopErr)
	}
	m.mu.RLock()
	if m.handleV4 != 0 || len(m.pendingDataCalls) != 1 || m.pendingDataCalls[0].handle != 41 || m.state != StateDisconnected || m.desiredConnection {
		t.Fatalf("first Disconnect state: h4=%d pending=%+v state=%s desired=%v", m.handleV4, m.pendingDataCalls, m.state, m.desiredConnection)
	}
	m.mu.RUnlock()

	if err := m.Disconnect(); err != nil {
		t.Fatalf("second Disconnect failed: %v", err)
	}
	m.mu.RLock()
	if m.handleV4 != 0 || len(m.pendingDataCalls) != 0 || m.state != StateDisconnected {
		t.Fatalf("second Disconnect state: h4=%d pending=%+v state=%s", m.handleV4, m.pendingDataCalls, m.state)
	}
	m.mu.RUnlock()
	stopMu.Lock()
	if len(stopped) != 2 || stopped[0] != 41 || stopped[1] != 41 {
		t.Fatalf("stopped handles = %v, want [41 41]", stopped)
	}
	stopMu.Unlock()
	if len(cleanedIfaces) != 2 || cleanedIfaces[0] != "qmimux7" || cleanedIfaces[1] != "qmimux7" {
		t.Fatalf("host cleanup interfaces = %v, want [qmimux7 qmimux7]", cleanedIfaces)
	}
}

func TestDialMissingDataFamiliesDrainsPendingFromReboundWDS(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
	oldWDS, reboundWDS := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds = reboundWDS
	m.handleV4 = 0
	m.pendingDataCalls = []pendingDataCall{{generation: token.generation, wds: oldWDS, family: qmi.IpFamilyV4, handle: 77}}

	var pendingStopped atomic.Bool
	m.stopDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, handle uint32) error {
		if gotWDS != oldWDS || handle != 77 {
			t.Fatalf("pending stop = (%p, %d), want (%p, 77)", gotWDS, handle, oldWDS)
		}
		pendingStopped.Store(true)
		return nil
	}
	m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
		if !pendingStopped.Load() {
			t.Fatal("new dial started before rebound WDS pending ownership was released")
		}
		if gotWDS != reboundWDS || family != qmi.IpFamilyV4 {
			t.Fatalf("new dial = (%p, %d), want rebound WDS/IPv4", gotWDS, family)
		}
		return 88, nil
	}

	if err := m.dialMissingDataFamilies(token.runCtx, token); err != nil {
		t.Fatalf("dialMissingDataFamilies failed: %v", err)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.handleV4 != 88 || len(m.pendingDataCalls) != 0 {
		t.Fatalf("post-dial state: h4=%d pending=%+v", m.handleV4, m.pendingDataCalls)
	}
}

func TestPendingRollbackBlocksRedialAndCleanupStopsBeforeClose(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.handleV4 = 0
	stopErr := errors.New("candidate rollback failed")
	m.stopDataCallHook = func(context.Context, *qmi.WDSService, uint32) error { return stopErr }

	m.rollbackDataCallCandidate(token, wds, qmi.IpFamilyV4, 77)
	m.mu.RLock()
	if len(m.pendingDataCalls) != 1 || m.pendingDataCalls[0].handle != 77 {
		t.Fatalf("rollback pending = %+v, want handle 77", m.pendingDataCalls)
	}
	m.mu.RUnlock()
	var starts atomic.Int32
	m.startDataCallHook = func(context.Context, *qmi.WDSService, uint8) (uint32, error) {
		starts.Add(1)
		return 88, nil
	}
	if err := m.dialMissingDataFamilies(token.runCtx, token); !errors.Is(err, stopErr) {
		t.Fatalf("redial error = %v, want pending stop error %v", err, stopErr)
	}
	if starts.Load() != 0 {
		t.Fatalf("new starts while old ownership pending = %d, want 0", starts.Load())
	}

	var pendingStopped atomic.Bool
	m.stopWDSForCleanup = func(_ context.Context, gotWDS *qmi.WDSService, handle uint32) error {
		if gotWDS != wds || handle != 77 {
			t.Errorf("cleanup stop = (%p, %d), want (%p, 77)", gotWDS, handle, wds)
		}
		pendingStopped.Store(true)
		return nil
	}
	m.closeWDSService = func(*qmi.WDSService) error {
		if !pendingStopped.Load() {
			t.Error("WDS closed before pending data call cleanup")
		}
		return nil
	}
	m.closeQMIClientHook = func(*qmi.Client) error {
		if !pendingStopped.Load() {
			t.Error("QMI client closed before pending data call cleanup")
		}
		return nil
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	m.cleanupLocked(cleanupCtx)
	cleanupCancel()
	if !pendingStopped.Load() {
		t.Fatal("cleanup did not retry pending candidate")
	}
}

func TestRadioOffTerminatesPendingBeforeRepairDial(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
	oldWDS, currentWDS := &qmi.WDSService{}, &qmi.WDSService{}
	m.wds = currentWDS
	m.handleV4 = 0
	m.pendingDataCalls = []pendingDataCall{{generation: token.generation, wds: oldWDS, family: qmi.IpFamilyV4, handle: 77}}
	m.flushRadioDataPlaneHook = func(string) error { return nil }
	m.radioPowerCommandHook = func(context.Context, coreSessionToken, bool, string) error { return nil }
	m.stopDataCallHook = func(context.Context, *qmi.WDSService, uint32) error {
		t.Fatal("radio-off terminated pending call but repair attempted StopNetworkInterface")
		return nil
	}
	m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
		if gotWDS != currentWDS || family != qmi.IpFamilyV4 {
			t.Fatalf("repair dial = (%p, %d), want current WDS/IPv4", gotWDS, family)
		}
		return 88, nil
	}

	result, err := m.radioPowerCycleForSession(token.runCtx, token, "test.pending")
	if err != nil {
		t.Fatalf("radio power cycle failed: %v", err)
	}
	if !result.dataInvalidated || !result.hadData {
		t.Fatalf("radio result = %+v, want pending data invalidated", result)
	}
	m.mu.RLock()
	if len(m.pendingDataCalls) != 0 {
		t.Fatalf("pending calls survived radio off: %+v", m.pendingDataCalls)
	}
	m.mu.RUnlock()
	if err := m.dialMissingDataFamilies(token.runCtx, token); err != nil {
		t.Fatalf("repair dial failed: %v", err)
	}
}

func TestClosedEventEmitterDoesNotDropReconnectIntent(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true, AutoReconnect: true})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.handleV4 = 0
	m.isRotating = true
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
	m.flushRotationAddressesHook = func(string) error { return nil }
	m.events.Close()

	m.finishIPRotation(token, wds, true, errors.New("rotation failed"))
	select {
	case envelope := <-m.eventCh:
		if envelope.kind != eventStart || envelope.generation != token.generation {
			t.Fatalf("reconnect envelope = %+v, want eventStart generation %d", envelope, token.generation)
		}
	case <-time.After(time.Second):
		t.Fatal("closed external emitter suppressed durable reconnect intent")
	}
}

func TestStopDuringDataCallStopSuppressesDisconnectedEvent(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.handleV4 = 41
	stopEntered := make(chan struct{})
	var enterOnce sync.Once
	m.stopDataCallHook = func(ctx context.Context, _ *qmi.WDSService, handle uint32) error {
		if handle != 41 {
			t.Errorf("active stop handle = %d, want 41", handle)
		}
		enterOnce.Do(func() { close(stopEntered) })
		<-ctx.Done()
		return ctx.Err()
	}
	m.stopWDSForCleanup = func(context.Context, *qmi.WDSService, uint32) error { return nil }
	m.closeWDSService = func(*qmi.WDSService) error { return nil }
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }
	disconnectedEvents := make(chan Event, 1)
	m.OnEvent(func(event Event) {
		if event.Type == EventDisconnected {
			disconnectedEvents <- event
		}
	})

	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- m.Disconnect() }()
	select {
	case <-stopEntered:
	case <-time.After(time.Second):
		t.Fatal("Disconnect did not enter data call stop")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()

	select {
	case err := <-disconnectDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Disconnect error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect did not return after Stop canceled the lifetime")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not finish after joining Disconnect")
	}
	select {
	case event := <-disconnectedEvents:
		t.Fatalf("post-Stop disconnected event = %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDisconnectTreatsAlreadyStoppedQMIResultsAsSuccess(t *testing.T) {
	for _, code := range []uint16{qmi.QMIErrOutOfCall, qmi.QMIErrNoEffect} {
		t.Run(fmt.Sprintf("code_%04x", code), func(t *testing.T) {
			m, _ := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
			m.wds = &qmi.WDSService{}
			m.handleV4 = 41
			m.disconnectHostCleanupHook = func(string) error { return nil }
			m.stopDataCallHook = func(context.Context, *qmi.WDSService, uint32) error {
				return fmt.Errorf("wrapped stop response: %w", &qmi.QMIError{ErrorCode: code})
			}

			if err := m.Disconnect(); err != nil {
				t.Fatalf("Disconnect returned definitive no-call result as error: %v", err)
			}
			m.mu.RLock()
			defer m.mu.RUnlock()
			if m.handleV4 != 0 || len(m.pendingDataCalls) != 0 || m.state != StateDisconnected {
				t.Fatalf("post-Disconnect state: h4=%d pending=%+v state=%s", m.handleV4, m.pendingDataCalls, m.state)
			}
		})
	}
}

func TestPendingNoCallResultClearsOwnershipAndAllowsRedial(t *testing.T) {
	for _, code := range []uint16{qmi.QMIErrOutOfCall, qmi.QMIErrNoEffect} {
		t.Run(fmt.Sprintf("code_%04x", code), func(t *testing.T) {
			m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
			oldWDS, currentWDS := &qmi.WDSService{}, &qmi.WDSService{}
			m.wds = currentWDS
			m.handleV4 = 0
			m.pendingDataCalls = []pendingDataCall{{
				generation: token.generation,
				wds:        oldWDS,
				family:     qmi.IpFamilyV4,
				handle:     77,
			}}
			var stopCalls, startCalls atomic.Int32
			m.stopDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, handle uint32) error {
				stopCalls.Add(1)
				if gotWDS != oldWDS || handle != 77 {
					t.Fatalf("pending stop = (%p, %d), want (%p, 77)", gotWDS, handle, oldWDS)
				}
				return fmt.Errorf("wrapped pending response: %w", &qmi.QMIError{ErrorCode: code})
			}
			m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
				startCalls.Add(1)
				if gotWDS != currentWDS || family != qmi.IpFamilyV4 {
					t.Fatalf("redial = (%p, %d), want current WDS/IPv4", gotWDS, family)
				}
				return 88, nil
			}

			if err := m.dialMissingDataFamilies(token.runCtx, token); err != nil {
				t.Fatalf("dialMissingDataFamilies failed: %v", err)
			}
			if stopCalls.Load() != 1 || startCalls.Load() != 1 {
				t.Fatalf("stop/start calls = %d/%d, want 1/1", stopCalls.Load(), startCalls.Load())
			}
			m.mu.RLock()
			defer m.mu.RUnlock()
			if len(m.pendingDataCalls) != 0 || m.handleV4 != 88 {
				t.Fatalf("post-redial state: pending=%+v h4=%d", m.pendingDataCalls, m.handleV4)
			}
		})
	}
}

func TestDisconnectWaitsForRotationAndDrainsLateRollbackPending(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{EnableIPv4: true})
	wds := &qmi.WDSService{}
	m.wds = wds
	m.state = StateDisconnected
	m.handleV4 = 0
	m.settings = nil
	m.isRotating = true

	rollbackErr := errors.New("late candidate rollback failed")
	var stopCalls atomic.Int32
	m.stopDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, handle uint32) error {
		if gotWDS != wds || handle != 77 {
			t.Fatalf("stop = (%p, %d), want (%p, 77)", gotWDS, handle, wds)
		}
		if stopCalls.Add(1) == 1 {
			return rollbackErr
		}
		return nil
	}

	m.lifecycleMu.Lock()
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			m.lifecycleMu.Unlock()
		}
	}()
	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- m.Disconnect() }()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.RLock()
		desired := m.desiredConnection
		m.mu.RUnlock()
		if !desired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Disconnect did not clear desiredConnection before waiting for rotation")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-disconnectDone:
		t.Fatalf("Disconnect returned before rotation released lifecycle ownership: %v", err)
	default:
	}

	m.rollbackDataCallCandidate(token, wds, qmi.IpFamilyV4, 77)
	m.mu.Lock()
	if len(m.pendingDataCalls) != 1 || m.pendingDataCalls[0].handle != 77 {
		m.mu.Unlock()
		t.Fatalf("late rollback pending = %+v, want handle 77", m.pendingDataCalls)
	}
	m.isRotating = false
	m.mu.Unlock()
	m.lifecycleMu.Unlock()
	lifecycleLocked = false

	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatalf("Disconnect failed to clean late pending call: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect did not finish after rotation released lifecycle ownership")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.pendingDataCalls) != 0 || stopCalls.Load() != 2 {
		t.Fatalf("post-Disconnect pending=%+v stopCalls=%d, want empty/2", m.pendingDataCalls, stopCalls.Load())
	}
}

func TestStatusDisconnectUsesCurrentDesiredAndMuxInterface(t *testing.T) {
	m, _ := newActiveDataLifecycleTestManager(t, Config{
		Device:        ModemDevice{NetInterface: "wwan0"},
		EnableIPv4:    true,
		AutoReconnect: true,
	})
	m.wds = &qmi.WDSService{}
	m.handleV4 = 41
	m.muxIface = "qmimux7"
	m.settings = &qmi.RuntimeSettings{IPv4Address: net.ParseIP("10.0.0.1")}
	queryEntered := make(chan struct{})
	releaseQuery := make(chan struct{})
	var queryOnce sync.Once
	m.queryPacketServiceState = func(context.Context) (qmi.ConnectionStatus, error) {
		queryOnce.Do(func() { close(queryEntered) })
		<-releaseQuery
		return qmi.StatusDisconnected, nil
	}
	flushedIface := make(chan string, 1)
	m.flushRotationAddressesHook = func(ifname string) error {
		flushedIface <- ifname
		return nil
	}
	m.disconnectHostCleanupHook = func(string) error { return nil }
	reconnecting := make(chan Event, 1)
	m.OnEvent(func(event Event) {
		if event.Type == EventReconnecting {
			reconnecting <- event
		}
	})

	statusDone := make(chan struct{})
	go func() {
		m.doStatusCheck(false)
		close(statusDone)
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("status check did not reach packet service query")
	}
	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- m.Disconnect() }()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.RLock()
		desired := m.desiredConnection
		m.mu.RUnlock()
		if !desired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Disconnect did not clear desiredConnection while status query was blocked")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseQuery)
	select {
	case <-statusDone:
	case <-time.After(2 * time.Second):
		t.Fatal("status check did not finish")
	}
	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatalf("Disconnect failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect did not finish after status check")
	}
	select {
	case ifname := <-flushedIface:
		if ifname != "qmimux7" {
			t.Fatalf("status cleanup interface = %q, want qmimux7", ifname)
		}
	case <-time.After(time.Second):
		t.Fatal("status disconnect did not flush host data plane")
	}
	select {
	case event := <-reconnecting:
		t.Fatalf("stale desired snapshot emitted reconnecting: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case envelope := <-m.eventCh:
		t.Fatalf("stale desired snapshot queued reconnect: %+v", envelope)
	default:
	}
}

func TestPacketServiceStatusTargetSelectsPrimaryEnabledFamily(t *testing.T) {
	wdsV4, wdsV6 := &qmi.WDSService{}, &qmi.WDSService{}
	for _, tc := range []struct {
		name   string
		cfg    Config
		want   *qmi.WDSService
		family uint8
	}{
		{name: "dual_stack_prefers_ipv4", cfg: Config{EnableIPv4: true, EnableIPv6: true}, want: wdsV4, family: qmi.IpFamilyV4},
		{name: "ipv6_only", cfg: Config{EnableIPv6: true}, want: wdsV6, family: qmi.IpFamilyV6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Manager{cfg: tc.cfg, wds: wdsV4, wdsV6: wdsV6}
			got, family := m.packetServiceStatusTarget()
			if got != tc.want || family != tc.family {
				t.Fatalf("target = (%p, %d), want (%p, %d)", got, family, tc.want, tc.family)
			}
		})
	}
}

func TestIPv6OnlyStatusDisconnectClearsHandleAndRepairRedials(t *testing.T) {
	m, token := newActiveDataLifecycleTestManager(t, Config{
		EnableIPv6:    true,
		AutoReconnect: true,
	})
	wdsV6 := &qmi.WDSService{}
	m.wds = nil
	m.wdsV6 = wdsV6
	m.handleV6 = 61
	m.settings = &qmi.RuntimeSettings{IPv6Address: net.ParseIP("2001:db8::1"), IPv6Prefix: 64}
	m.muxIface = "qmimux6"
	m.queryPacketServiceState = func(context.Context) (qmi.ConnectionStatus, error) {
		return qmi.StatusDisconnected, nil
	}
	m.flushRotationAddressesHook = func(ifname string) error {
		if ifname != "qmimux6" {
			t.Errorf("IPv6 status cleanup interface = %q, want qmimux6", ifname)
		}
		return nil
	}
	var starts atomic.Int32
	m.startDataCallHook = func(_ context.Context, gotWDS *qmi.WDSService, family uint8) (uint32, error) {
		starts.Add(1)
		if gotWDS != wdsV6 || family != qmi.IpFamilyV6 {
			t.Fatalf("IPv6 repair dial = (%p, %d), want (%p, IPv6)", gotWDS, family, wdsV6)
		}
		return 81, nil
	}
	m.configureNetworkHook = func() error {
		m.mu.Lock()
		m.settings = &qmi.RuntimeSettings{IPv6Address: net.ParseIP("2001:db8::2"), IPv6Prefix: 64}
		m.mu.Unlock()
		return nil
	}
	installQuietDialQueries(m)

	m.doStatusCheck(false)
	m.mu.RLock()
	if m.state != StateDisconnected || m.handleV6 != 0 || m.settings != nil {
		t.Fatalf("post-status state=%s h6=%d settings=%v", m.state, m.handleV6, m.settings)
	}
	m.mu.RUnlock()
	var envelope internalEventEnvelope
	select {
	case envelope = <-m.eventCh:
		if envelope.kind != eventStart || envelope.generation != token.generation {
			t.Fatalf("IPv6 reconnect envelope = %+v", envelope)
		}
	case <-time.After(time.Second):
		t.Fatal("IPv6-only disconnect did not queue repair")
	}
	m.handleEventForGeneration(envelope.kind, envelope.generation)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != StateConnected || m.handleV6 != 81 || starts.Load() != 1 {
		t.Fatalf("IPv6 repair state=%s h6=%d starts=%d", m.state, m.handleV6, starts.Load())
	}
}
