package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

type fakeManagedService struct {
	clientID uint8
	closed   atomic.Int32
	onClose  func()
}

func (s *fakeManagedService) ClientID() uint8 {
	if s == nil {
		return 0
	}
	return s.clientID
}

func (s *fakeManagedService) Close() error {
	if s != nil {
		s.closed.Add(1)
		if s.onClose != nil {
			s.onClose()
		}
	}
	return nil
}

func prepareServiceOwnerTestBinding(t *testing.T, m *Manager) (*listenerBinding, *qmi.Client) {
	t.Helper()
	events := make(chan qmi.Event, 8)
	done := make(chan struct{})
	binding, client := installListenerTestBinding(t, m, events, done, nil)
	m.mu.Lock()
	m.resetServiceOwnerRegistryForBindingLocked(binding)
	m.mu.Unlock()
	return binding, client
}

func publishServiceOwnerForTest(
	t *testing.T,
	m *Manager,
	slot serviceSlot,
	clientID uint8,
	instance any,
) *serviceOwner {
	t.Helper()
	m.mu.Lock()
	owner, err := m.publishServiceOwnerLocked(slot, clientID, instance)
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("publish owner slot=%s client=0x%02x: %v", slot, clientID, err)
	}
	if owner == nil {
		t.Fatalf("publish owner slot=%s returned nil", slot)
	}
	return owner
}

func TestServiceOwnerRoutesDualWDSByRawClientID(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)

	v4 := publishServiceOwnerForTest(t, m, serviceSlotWDSv4, 0x11, new(int))
	v6 := publishServiceOwnerForTest(t, m, serviceSlotWDSv6, 0x22, new(int))

	m.mu.RLock()
	gotV4, requiredV4 := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDS, ClientID: 0x11,
	})
	gotV6, requiredV6 := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDS, ClientID: 0x22,
	})
	pseudoV6, pseudoRequired := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDSIPv6, ClientID: 0x22,
	})
	m.mu.RUnlock()

	if !requiredV4 || gotV4 != v4 || !requiredV6 || gotV6 != v6 {
		t.Fatalf("dual WDS routing v4=(%v,%p) v6=(%v,%p), want exact owners %p/%p",
			requiredV4, gotV4, requiredV6, gotV6, v4, v6)
	}
	if !pseudoRequired || pseudoV6 != nil {
		t.Fatalf("ServiceWDSIPv6 pseudo identity routed to owner %p required=%v; want fail closed", pseudoV6, pseudoRequired)
	}
}

func TestServiceOwnerRejectsActiveCollisionAndRetiredTupleReuse(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)

	owner := publishServiceOwnerForTest(t, m, serviceSlotWDSv4, 0x31, new(int))
	m.mu.Lock()
	_, collisionErr := m.publishServiceOwnerLocked(serviceSlotWDSv6, 0x31, new(int))
	retired := m.revokeServiceOwnerLocked(serviceSlotWDSv4)
	_, reusedErr := m.publishServiceOwnerLocked(serviceSlotWDSv4, 0x31, new(int))
	m.mu.Unlock()

	if retired != owner || owner.phase != serviceOwnerRevoked {
		t.Fatalf("retired owner=%p phase=%d, want owner=%p revoked", retired, owner.phase, owner)
	}
	if !errors.Is(collisionErr, errServiceOwnerIdentityCollision) {
		t.Fatalf("collision error=%v, want errServiceOwnerIdentityCollision", collisionErr)
	}
	if !errors.Is(reusedErr, errServiceOwnerIdentityReused) {
		t.Fatalf("reuse error=%v, want errServiceOwnerIdentityReused", reusedErr)
	}
}

func TestServiceOwnerAllowsNumericTupleOnReplacementTransport(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, _ = prepareServiceOwnerTestBinding(t, m)
	oldOwner := publishServiceOwnerForTest(t, m, serviceSlotNAS, 0x2a, new(int))

	m.mu.Lock()
	m.revokeServiceOwnerLocked(serviceSlotNAS)
	m.mu.Unlock()

	newBinding, _ := prepareServiceOwnerTestBinding(t, m)
	newOwner := publishServiceOwnerForTest(t, m, serviceSlotNAS, 0x2a, new(int))

	if newOwner.epoch <= oldOwner.epoch {
		t.Fatalf("replacement epoch=%d, want > old epoch=%d", newOwner.epoch, oldOwner.epoch)
	}
	if newOwner.listenerBindingID != newBinding.id || newOwner.client != newBinding.client {
		t.Fatalf("replacement owner binding/client mismatch: %+v binding=%+v", newOwner, newBinding)
	}
	if m.serviceOwnerCurrent(oldOwner) {
		t.Fatal("old transport owner remained current after replacement")
	}
	if !m.serviceOwnerCurrent(newOwner) {
		t.Fatal("replacement transport owner is not current")
	}
}

func TestServiceOwnerRejectsTupleReuseWhenListenerChangesOnSameTransport(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	_, client := prepareServiceOwnerTestBinding(t, m)
	oldOwner := publishServiceOwnerForTest(t, m, serviceSlotNAS, 0x2a, new(int))

	m.mu.Lock()
	newGeneration := m.coreGeneration.Add(1)
	replacement := m.publishListenerBindingLocked(client, newGeneration, runCtx)
	m.mu.Unlock()
	if replacement == nil {
		t.Fatal("same-transport replacement listener was not published")
	}

	m.mu.Lock()
	_, err := m.publishServiceOwnerLocked(serviceSlotNAS, 0x2a, new(int))
	m.mu.Unlock()
	if !errors.Is(err, errServiceOwnerIdentityReused) {
		t.Fatalf("same-transport tuple reuse error=%v, want errServiceOwnerIdentityReused", err)
	}
	if oldOwner.phase != serviceOwnerRevoked || m.serviceOwnerCurrent(oldOwner) {
		t.Fatal("old owner remained active after same-transport listener replacement")
	}
}

func TestQueuedOldOwnerIndicationCannotAffectReplacement(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldOwner := publishServiceOwnerForTest(t, m, serviceSlotWDSv4, 0x41, new(int))
	oldEvent := qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDS, ClientID: 0x41,
	}
	m.queueListenerIndication(runCtx, binding, oldEvent)

	var queuedOld listenerIndication
	select {
	case queuedOld = <-m.listenerIndicationCh:
	default:
		t.Fatal("old owner indication was not admitted")
	}
	if queuedOld.owner != oldOwner || !queuedOld.ownerRequired {
		t.Fatalf("queued owner=%p required=%v, want old owner %p", queuedOld.owner, queuedOld.ownerRequired, oldOwner)
	}

	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	m.revokeServiceOwnerLocked(serviceSlotWDSv4)
	newOwner, err := m.publishServiceOwnerLocked(serviceSlotWDSv4, 0x42, new(int))
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	if err != nil {
		t.Fatalf("publish replacement owner: %v", err)
	}

	m.handleIndicationForBinding(queuedOld.binding, queuedOld.owner, queuedOld.ownerRequired, queuedOld.event)
	select {
	case event := <-m.eventCh:
		t.Fatalf("stale queued indication produced internal event %+v", event)
	default:
	}

	newEvent := qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDS, ClientID: 0x42,
	}
	m.queueListenerIndication(runCtx, binding, newEvent)
	queuedNew := <-m.listenerIndicationCh
	if queuedNew.owner != newOwner {
		t.Fatalf("new queued owner=%p, want %p", queuedNew.owner, newOwner)
	}
	m.handleIndicationForBinding(queuedNew.binding, queuedNew.owner, queuedNew.ownerRequired, queuedNew.event)
	select {
	case event := <-m.eventCh:
		if event.kind != eventPacketStatusChanged {
			t.Fatalf("new indication internal event=%v, want packet status", event.kind)
		}
	default:
		t.Fatal("current owner indication produced no internal event")
	}
}

func TestRetiredServiceIdentityIsRejectedAtQueueAdmission(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	publishServiceOwnerForTest(t, m, serviceSlotNAS, 0x51, new(int))
	m.mu.Lock()
	m.revokeServiceOwnerLocked(serviceSlotNAS)
	m.mu.Unlock()

	m.queueListenerIndication(runCtx, binding, qmi.Event{
		Type: qmi.EventNASEventReport, ServiceID: qmi.ServiceNAS, ClientID: 0x51,
	})
	if got := len(m.listenerIndicationCh); got != 0 {
		t.Fatalf("retired identity queue length=%d, want 0", got)
	}
}

func TestServiceOwnerAcceptsClientIDZeroWhenWireIdentityIsExact(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	owner := publishServiceOwnerForTest(t, m, serviceSlotNAS, 0, new(int))

	m.mu.RLock()
	got, required := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventNASEventReport, ServiceID: qmi.ServiceNAS, ClientID: 0,
	})
	m.mu.RUnlock()
	if !required || got != owner {
		t.Fatalf("zero client ID routed owner=%p required=%v, want %p/true", got, required, owner)
	}
}

func TestInstallManagedServiceRejectsStopAndClosesCandidateOutsideLocks(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	_, client := prepareServiceOwnerTestBinding(t, m)
	var field *fakeManagedService
	candidate := &fakeManagedService{clientID: 0x61}
	candidate.onClose = func() {
		if !m.mu.TryLock() {
			t.Error("candidate Close ran while m.mu was held")
		} else {
			m.mu.Unlock()
		}
		if !m.indicationDispatchMu.TryLock() {
			t.Error("candidate Close ran while indicationDispatchMu was held")
		} else {
			m.indicationDispatchMu.Unlock()
		}
	}

	m.mu.Lock()
	m.state = StateStopping
	m.mu.Unlock()

	err := installManagedService(m, serviceSlotNAS, client, &field, candidate)
	if err == nil {
		t.Fatal("install after stopping succeeded")
	}
	if field != nil {
		t.Fatal("install after stopping published candidate")
	}
	if got := candidate.closed.Load(); got != 1 {
		t.Fatalf("candidate Close calls=%d, want 1", got)
	}
}

func TestInstallManagedServicePublishesFieldAndExactOwner(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, client := prepareServiceOwnerTestBinding(t, m)
	var field *fakeManagedService
	candidate := &fakeManagedService{clientID: 0x62}

	if err := installManagedService(m, serviceSlotNAS, client, &field, candidate); err != nil {
		t.Fatalf("install managed service: %v", err)
	}
	if field != candidate || candidate.closed.Load() != 0 {
		t.Fatalf("installed field=%p closed=%d, want candidate=%p open", field, candidate.closed.Load(), candidate)
	}
	m.mu.RLock()
	owner, required := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventNASEventReport, ServiceID: qmi.ServiceNAS, ClientID: candidate.clientID,
	})
	m.mu.RUnlock()
	if !required || owner == nil || owner.instance != candidate || owner.slot != serviceSlotNAS {
		t.Fatalf("installed owner=%+v required=%v, want exact NAS candidate", owner, required)
	}
}

func TestServiceReleaseOutcomeClassification(t *testing.T) {
	if !serviceReleaseAllowsReallocate(nil) {
		t.Fatal("successful release did not allow replacement")
	}
	if !serviceReleaseAllowsReallocate(&qmi.QMIError{ErrorCode: qmi.QMIErrInvalidID}) {
		t.Fatal("InvalidID release did not allow replacement")
	}
	if serviceReleaseAllowsReallocate(context.DeadlineExceeded) {
		t.Fatal("uncertain timeout release allowed replacement")
	}
	if !errors.Is(uncertainServiceReleaseError(serviceSlotNAS, context.DeadlineExceeded), errServiceOwnerReleaseUncertain) {
		t.Fatal("uncertain release did not preserve owner-release sentinel")
	}
}
