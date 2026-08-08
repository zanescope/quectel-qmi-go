package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func publishDMSOwnerForTest(t *testing.T, m *Manager, service *qmi.DMSService, clientID uint8) *serviceOwner {
	t.Helper()
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	m.dms = service
	owner, err := m.publishServiceOwnerLocked(serviceSlotDMS, clientID, service)
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	if err != nil {
		t.Fatalf("publish DMS owner client=0x%02x: %v", clientID, err)
	}
	if owner == nil {
		t.Fatalf("publish DMS owner client=0x%02x returned nil", clientID)
	}
	return owner
}

func replaceDMSOwnerForTest(t *testing.T, m *Manager, service *qmi.DMSService, clientID uint8) *serviceOwner {
	t.Helper()
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	m.revokeServiceOwnerLocked(serviceSlotDMS)
	m.dms = service
	owner, err := m.publishServiceOwnerLocked(serviceSlotDMS, clientID, service)
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	if err != nil {
		t.Fatalf("replace DMS owner client=0x%02x: %v", clientID, err)
	}
	return owner
}

func TestStaleTimeoutCannotRebindReplacementOwner(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 1
	m.cfg.RecoveryPolicy.ServiceTimeoutWindow = time.Minute

	oldService := &qmi.DMSService{}
	oldOwner := publishDMSOwnerForTest(t, m, oldService, 0x71)
	entered := make(chan struct{})
	release := make(chan struct{})
	resultCh := make(chan error, 1)
	var rebindCalls atomic.Int32
	m.rebindDMSServiceHook = func(string) (*qmi.DMSService, error) {
		rebindCalls.Add(1)
		return nil, errors.New("unexpected stale rebind")
	}

	go func() {
		resultCh <- m.withDMSRecovery("DMS.StaleTimeout", func(dms *qmi.DMSService) error {
			if dms != oldService {
				return errors.New("operation received unexpected DMS owner")
			}
			close(entered)
			<-release
			return context.DeadlineExceeded
		})
	}()
	<-entered

	newService := &qmi.DMSService{}
	newOwner := replaceDMSOwnerForTest(t, m, newService, 0x72)
	close(release)
	if err := <-resultCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale operation error=%v, want deadline exceeded", err)
	}
	if got := rebindCalls.Load(); got != 0 {
		t.Fatalf("stale timeout rebind calls=%d, want 0", got)
	}
	if m.serviceOwnerCurrent(oldOwner) || !m.serviceOwnerCurrent(newOwner) {
		t.Fatal("stale timeout changed replacement owner state")
	}
	if m.dms != newService {
		t.Fatal("stale timeout replaced the current DMS pointer")
	}
	select {
	case event := <-m.eventCh:
		t.Fatalf("stale timeout queued core event %+v", event)
	default:
	}
}

func TestOldSuccessCannotClearReplacementOwnerTimeoutWindow(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 2
	m.cfg.RecoveryPolicy.ServiceTimeoutWindow = time.Minute

	oldService := &qmi.DMSService{}
	publishDMSOwnerForTest(t, m, oldService, 0x73)
	entered := make(chan struct{})
	release := make(chan struct{})
	oldResultCh := make(chan error, 1)
	go func() {
		oldResultCh <- m.withDMSRecovery("DMS.OwnerWindow", func(dms *qmi.DMSService) error {
			if dms != oldService {
				return errors.New("operation received unexpected old DMS owner")
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	newService := &qmi.DMSService{}
	newOwner := replaceDMSOwnerForTest(t, m, newService, 0x74)
	if err := m.withDMSRecovery("DMS.OwnerWindow", func(dms *qmi.DMSService) error {
		if dms != newService {
			return errors.New("operation received unexpected replacement DMS owner")
		}
		return context.DeadlineExceeded
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first replacement timeout error=%v, want deadline exceeded", err)
	}

	close(release)
	if err := <-oldResultCh; !errors.Is(err, errServiceOwnerStale) {
		t.Fatalf("delayed old success error=%v, want errServiceOwnerStale", err)
	}

	var rebindCalls atomic.Int32
	m.rebindDMSServiceHook = func(string) (*qmi.DMSService, error) {
		rebindCalls.Add(1)
		return nil, errors.New("stop after proving threshold")
	}
	_ = m.withDMSRecovery("DMS.OwnerWindow", func(dms *qmi.DMSService) error {
		if dms != newService {
			return errors.New("operation received unexpected replacement DMS owner")
		}
		return context.DeadlineExceeded
	})
	if got := rebindCalls.Load(); got != 1 {
		t.Fatalf("replacement rebind calls=%d, want 1 after second timeout", got)
	}
	if !m.serviceOwnerCurrent(newOwner) || m.dms != newService {
		t.Fatal("proof hook unexpectedly changed replacement owner")
	}
}

func TestDetachManagedServiceRejectsStaleExpectedOwner(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)
	oldService := &qmi.DMSService{}
	oldOwner := publishDMSOwnerForTest(t, m, oldService, 0x75)
	oldToken := serviceOperationOwner{owner: oldOwner, required: true}
	newService := &qmi.DMSService{}
	newOwner := replaceDMSOwnerForTest(t, m, newService, 0x76)

	previous, client, detached := detachManagedServiceIfCurrent(m, serviceSlotDMS, &m.dms, oldToken)
	if detached || previous != nil || client != nil {
		t.Fatalf("stale detach=(previous=%p client=%p detached=%v), want nil/nil/false", previous, client, detached)
	}
	if m.dms != newService || !m.serviceOwnerCurrent(newOwner) {
		t.Fatal("stale detach modified replacement owner")
	}
}

func TestServiceTimeoutWindowsAreScopedByOwnerEpoch(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)
	m.cfg.RecoveryPolicy.ServiceTimeoutThreshold = 3
	m.cfg.RecoveryPolicy.ServiceTimeoutWindow = time.Minute

	oldService := &qmi.DMSService{}
	oldOwner := publishDMSOwnerForTest(t, m, oldService, 0x77)
	oldToken := serviceOperationOwner{owner: oldOwner, required: true}
	if m.recordServiceTimeoutFailureForOwner(oldToken, "DMS", "DMS.EpochWindow", context.DeadlineExceeded) {
		t.Fatal("old owner reached timeout threshold too early")
	}

	newService := &qmi.DMSService{}
	newOwner := replaceDMSOwnerForTest(t, m, newService, 0x78)
	newToken := serviceOperationOwner{owner: newOwner, required: true}
	if m.recordServiceTimeoutFailureForOwner(newToken, "DMS", "DMS.EpochWindow", context.DeadlineExceeded) {
		t.Fatal("replacement owner inherited old timeout count")
	}
	m.noteServiceOperationSuccessForOwner(oldToken, "DMS", "DMS.EpochWindow")

	m.serviceTimeoutMu.Lock()
	oldState := m.serviceTimeoutFailures[serviceTimeoutKey{
		service: "DMS", op: "DMS.EpochWindow", ownerEpoch: oldOwner.epoch,
	}]
	newState := m.serviceTimeoutFailures[serviceTimeoutKey{
		service: "DMS", op: "DMS.EpochWindow", ownerEpoch: newOwner.epoch,
	}]
	m.serviceTimeoutMu.Unlock()
	if oldState.count != 1 || newState.count != 1 {
		t.Fatalf("timeout windows old=%+v new=%+v, want independent count=1", oldState, newState)
	}
}
