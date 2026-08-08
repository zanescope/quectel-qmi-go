package manager

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func newClassificationTestManager(t *testing.T, threshold int) *Manager {
	t.Helper()
	m := newRecoveryTestManager()
	m.cfg = normalizeConfig(Config{
		RecoveryPolicy: RecoveryPolicy{
			ServiceTimeoutThreshold: threshold,
			ServiceTimeoutWindow:    time.Minute,
		},
	})
	m.coreReady = true
	m.state = StateDisconnected
	t.Cleanup(m.events.Close)
	return m
}

func assertNoInternalRecoveryEvent(t *testing.T, m *Manager) {
	t.Helper()
	select {
	case event := <-m.eventCh:
		t.Fatalf("unexpected internal recovery event: %v", event)
	default:
	}
}

func configureSuccessfulCoreRecovery(m *Manager) {
	m.openClientAndAllocateServicesHook = func(context.Context) error { return nil }
	m.checkSIMHook = func() error { return nil }
	m.modemResetQuietWindow = time.Millisecond
	m.getICCIDStrictHook = func(context.Context) (string, error) { return "test-iccid", nil }
}

func TestCrossServiceTimeoutsBelowThresholdDoNotRecover(t *testing.T) {
	m := newClassificationTestManager(t, 3)

	if m.recordServiceTimeoutFailure("DMS", "GetDeviceSerialNumbers", context.DeadlineExceeded) {
		t.Fatal("DMS timeout below threshold unexpectedly enabled service recovery")
	}
	if m.recordServiceTimeoutFailure("NAS", "GetServingSystem", context.DeadlineExceeded) {
		t.Fatal("NAS timeout below threshold unexpectedly enabled service recovery")
	}

	assertNoInternalRecoveryEvent(t, m)
	stats := m.Stats()
	if stats.ResetEvents != 0 || stats.CoreRecoveryRequests != 0 {
		t.Fatalf("unexpected recovery metrics: %+v", stats)
	}
}

func TestTimeoutStormRequiresQualifiedDistinctServices(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	logger, hook := logrustest.NewNullLogger()
	m.log = NewLogrusLogger(logger)

	for _, service := range []string{"DMS", "NAS"} {
		for attempt := 0; attempt < 2; attempt++ {
			m.recordServiceTimeoutFailure(service, "TimeoutOp", context.DeadlineExceeded)
		}
	}

	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventCoreRecovery {
		t.Fatalf("event = %v, want eventCoreRecovery", event)
	}
	m.modemResetMu.Lock()
	request := m.coreRecoveryRequest
	m.modemResetMu.Unlock()
	if request.reason != recoveryReasonServiceTimeoutStorm {
		t.Fatalf("reason = %q, want %q", request.reason, recoveryReasonServiceTimeoutStorm)
	}
	if request.detail != "DMS,NAS" {
		t.Fatalf("detail = %q, want DMS,NAS", request.detail)
	}

	var found bool
	for _, entry := range hook.AllEntries() {
		if entry.Message != "Timeout storm detected; triggering core recovery" {
			continue
		}
		found = true
		if got := entry.Data["services_affected"]; got != 2 {
			t.Fatalf("services_affected = %#v, want 2", got)
		}
		if got := entry.Data["services"]; !reflect.DeepEqual(got, []string{"DMS", "NAS"}) {
			t.Fatalf("services = %#v, want [DMS NAS]", got)
		}
	}
	if !found {
		t.Fatal("timeout storm diagnostic log not found")
	}

	stats := m.Stats()
	if stats.CoreRecoveryRequests != 1 || stats.ResetEvents != 0 {
		t.Fatalf("unexpected recovery metrics: %+v", stats)
	}
}

func TestSameServiceAboveThresholdDoesNotCreateStorm(t *testing.T) {
	m := newClassificationTestManager(t, 2)

	for _, op := range []string{"GetSignalStrength", "GetServingSystem"} {
		for attempt := 0; attempt < 5; attempt++ {
			m.recordServiceTimeoutFailure("NAS", op, context.DeadlineExceeded)
		}
	}

	assertNoInternalRecoveryEvent(t, m)
	m.globalTimeoutMu.Lock()
	services := len(m.globalTimeoutServices)
	m.globalTimeoutMu.Unlock()
	if services != 1 {
		t.Fatalf("qualified services = %d, want 1", services)
	}
}

func TestTimeoutStormIsGatedByCoreLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Manager)
	}{
		{
			name: "core not ready",
			configure: func(m *Manager) {
				m.coreReady = false
			},
		},
		{
			name: "recovery running",
			configure: func(m *Manager) {
				m.modemResetRecovering = true
			},
		},
		{
			name: "software recovery pending",
			configure: func(m *Manager) {
				m.coreRecoveryEnqueued = true
			},
		},
		{
			name: "stopping",
			configure: func(m *Manager) {
				m.state = StateStopping
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := newClassificationTestManager(t, 1)
			test.configure(m)

			m.detectTimeoutStorm("DMS")
			m.detectTimeoutStorm("NAS")

			assertNoInternalRecoveryEvent(t, m)
			m.globalTimeoutMu.Lock()
			services := len(m.globalTimeoutServices)
			m.globalTimeoutMu.Unlock()
			if services != 0 {
				t.Fatalf("storm candidates retained while gated: %d", services)
			}
		})
	}
}

func TestRequestCoreRecoveryDoesNotEmitModemReset(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	external := make(chan Event, 4)
	m.OnEvent(func(event Event) { external <- event })

	if !m.RequestCoreRecovery("post_switch_service_stalled") {
		t.Fatal("RequestCoreRecovery() rejected an eligible request")
	}
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventCoreRecovery {
		t.Fatalf("event = %v, want eventCoreRecovery", event)
	}

	select {
	case event := <-external:
		if event.Type != EventCoreRecoveryRequested {
			t.Fatalf("external event = %s, want CoreRecoveryRequested", event.Type)
		}
		if event.Reason != string(recoveryReasonPostSwitch) {
			t.Fatalf("reason = %q, want %q", event.Reason, recoveryReasonPostSwitch)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CoreRecoveryRequested")
	}

	stats := m.Stats()
	if stats.CoreRecoveryRequests != 1 || stats.ResetEvents != 0 || stats.ResetCoalesced != 0 {
		t.Fatalf("software recovery polluted reset metrics: %+v", stats)
	}
}

func TestRequestCoreRecoveryCountsSuppressedLifecycleRequest(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	m.coreReady = false

	if m.RequestCoreRecovery("manual_recovery") {
		t.Fatal("RequestCoreRecovery() accepted request while core was not ready")
	}
	assertNoInternalRecoveryEvent(t, m)
	stats := m.Stats()
	if stats.CoreRecoveryRequests != 1 || stats.CoreRecoverySuppressed != 1 || stats.ResetEvents != 0 {
		t.Fatalf("unexpected suppressed recovery metrics: %v", stats)
	}
}

func TestRealModemResetEmitsResetAndRunsFullRecovery(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	configureSuccessfulCoreRecovery(m)
	external := make(chan Event, 8)
	m.OnEvent(func(event Event) { external <- event })

	m.snapshot.UpdateIdentities(DeviceIdentities{ICCID: "old", IMSI: "old"})
	m.handleIndication(qmi.Event{Type: qmi.EventModemReset})
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventModemReset {
		t.Fatalf("event = %v, want eventModemReset", event)
	} else {
		m.handleEvent(event)
	}

	var sawReset bool
	deadline := time.After(time.Second)
	for !sawReset {
		select {
		case event := <-external:
			if event.Type == EventModemReset {
				sawReset = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for EventModemReset")
		}
	}
	stats := m.Stats()
	if stats.ResetEvents != 1 || stats.CoreRecoveryRequests != 0 || stats.RecoverAttempts != 1 || stats.CoreRecoverySuccess != 1 {
		t.Fatalf("unexpected real reset metrics: %+v", stats)
	}
	identities, _ := m.snapshot.Identities()
	if identities.ICCID != "" || identities.IMSI != "" {
		t.Fatalf("snapshot identities were not reset: %+v", identities)
	}
}

func TestRealResetPreemptsQueuedSoftwareRecovery(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	configureSuccessfulCoreRecovery(m)

	if !m.RequestCoreRecovery("manual_recovery") {
		t.Fatal("software recovery request was rejected")
	}
	softwareEvent := waitInternalRecoveryEvent(t, m.eventCh, time.Second)
	m.handleIndication(qmi.Event{Type: qmi.EventModemReset})
	if resetEvent := waitInternalRecoveryEvent(t, m.eventCh, time.Second); resetEvent != eventModemReset {
		t.Fatalf("reset event = %v, want eventModemReset", resetEvent)
	}

	m.handleEvent(softwareEvent)
	if got := m.Stats().RecoverAttempts; got != 0 {
		t.Fatalf("superseded software wake triggered recovery: %d", got)
	}

	m.handleEvent(eventModemReset)
	stats := m.Stats()
	if stats.RecoverAttempts != 1 || stats.ResetEvents != 1 || stats.CoreRecoveryCoalesced != 1 {
		t.Fatalf("real reset did not preempt software recovery: %+v", stats)
	}

	m.handleEvent(eventCoreRecovery)
	if got := m.Stats().RecoverAttempts; got != 1 {
		t.Fatalf("stale software wake triggered an extra recovery: %d", got)
	}
}

func TestRealResetDuringSoftwareRecoveryIsPreserved(t *testing.T) {
	m := newClassificationTestManager(t, 2)
	m.modemResetQuietWindow = time.Millisecond
	m.checkSIMHook = func() error { return nil }
	m.getICCIDStrictHook = func(context.Context) (string, error) { return "test-iccid", nil }

	entered := make(chan struct{})
	release := make(chan struct{})
	var opens atomic.Int32
	m.openClientAndAllocateServicesHook = func(context.Context) error {
		if opens.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}

	if !m.RequestCoreRecovery("manual_recovery") {
		t.Fatal("software recovery request was rejected")
	}
	softwareEvent := waitInternalRecoveryEvent(t, m.eventCh, time.Second)
	done := make(chan struct{})
	go func() {
		m.handleEvent(softwareEvent)
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("software recovery did not enter executor")
	}
	m.handleIndication(qmi.Event{Type: qmi.EventModemReset})
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("software recovery did not finish after reset arrived")
	}

	resetEvent := waitInternalRecoveryEvent(t, m.eventCh, time.Second)
	if resetEvent != eventModemReset {
		t.Fatalf("preserved event = %v, want eventModemReset", resetEvent)
	}
	m.handleEvent(resetEvent)

	m.modemResetMu.Lock()
	running := m.modemResetRecovering
	pending := m.modemResetPending
	enqueued := m.modemResetEnqueued || m.coreRecoveryEnqueued
	m.modemResetMu.Unlock()
	if running || pending || enqueued {
		t.Fatalf("recovery state did not converge: running=%v pending=%v enqueued=%v", running, pending, enqueued)
	}
	stats := m.Stats()
	if stats.ResetEvents != 1 || stats.RecoverAttempts != 2 || stats.CoreRecoveryFailures != 1 || stats.CoreRecoverySuccess != 1 {
		t.Fatalf("unexpected concurrent recovery metrics: %+v", stats)
	}
}
