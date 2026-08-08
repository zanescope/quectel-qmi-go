package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func ctlRevokeEvent(serviceID, clientID uint8) qmi.Event {
	return qmi.Event{
		Type:      qmi.EventModemReset,
		ServiceID: qmi.ServiceControl,
		MessageID: qmi.CTLRevokeClientIDInd,
		RevokedClient: &qmi.ClientIdentity{
			ServiceID: serviceID,
			ClientID:  clientID,
		},
	}
}

func publishWDSOwnersForRevocationTest(
	t *testing.T,
	m *Manager,
	v4 *qmi.WDSService,
	v4ClientID uint8,
	v6 *qmi.WDSService,
	v6ClientID uint8,
) (*serviceOwner, *serviceOwner) {
	t.Helper()
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	m.wds = v4
	v4Owner, v4Err := m.publishServiceOwnerLocked(serviceSlotWDSv4, v4ClientID, v4)
	var v6Owner *serviceOwner
	var v6Err error
	if v6 != nil {
		m.wdsV6 = v6
		v6Owner, v6Err = m.publishServiceOwnerLocked(serviceSlotWDSv6, v6ClientID, v6)
	}
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	if v4Err != nil || v6Err != nil {
		t.Fatalf("publish dual WDS owners v4=%v v6=%v", v4Err, v6Err)
	}
	return v4Owner, v6Owner
}

func currentCoreTokenForTest(m *Manager) coreSessionToken {
	m.mu.RLock()
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	m.mu.RUnlock()
	return token
}

func waitBackgroundTasksForTest(m *Manager) {
	m.backgroundTaskMu.Lock()
	m.backgroundTaskMu.Unlock()
	m.backgroundTaskWG.Wait()
}

func TestCTLRevokeRoutesDualWDSByExactClientID(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	v4 := &qmi.WDSService{}
	v6 := &qmi.WDSService{}
	v4Owner, v6Owner := publishWDSOwnersForRevocationTest(t, m, v4, 0x11, v6, 0x22)

	owner, disposition, reason := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x11))
	if disposition != serviceRevocationTargeted || owner != v4Owner || reason != "exact_target" {
		t.Fatalf("v4 revoke=(owner=%p disposition=%d reason=%q), want %p/targeted/exact", owner, disposition, reason, v4Owner)
	}
	if v4Owner.phase != serviceOwnerRevoking || !m.serviceOwnerCurrent(v6Owner) {
		t.Fatal("v4 revoke changed the v6 owner or failed to reserve v4")
	}

	duplicateOwner, duplicateDisposition, _ := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x11))
	if duplicateDisposition != serviceRevocationDuplicate || duplicateOwner != v4Owner {
		t.Fatalf("duplicate revoke=(owner=%p disposition=%d), want %p/duplicate", duplicateOwner, duplicateDisposition, v4Owner)
	}

	m.mu.RLock()
	ordinaryOwner, required := m.serviceOwnerForEventLocked(binding, qmi.Event{
		Type: qmi.EventPacketServiceStatusChanged, ServiceID: qmi.ServiceWDS, ClientID: 0x11,
	})
	m.mu.RUnlock()
	if !required || ordinaryOwner != nil {
		t.Fatalf("revoking WDS ordinary route=(owner=%p required=%v), want nil/true", ordinaryOwner, required)
	}
}

func TestCTLRevokeAmbiguousIdentityFallsBackToCore(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	v4 := publishServiceOwnerForTest(t, m, serviceSlotWDSv4, 0x23, &qmi.WDSService{})

	m.mu.Lock()
	duplicate := *v4
	duplicate.slot = serviceSlotWDSv6
	m.serviceOwnersBySlot[serviceSlotWDSv6] = &duplicate
	m.mu.Unlock()

	owner, disposition, reason := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x23))
	if owner != nil || disposition != serviceRevocationFallback || reason != "ambiguous_target" {
		t.Fatalf("ambiguous revoke=(owner=%p disposition=%d reason=%q), want nil/fallback/ambiguous", owner, disposition, reason)
	}
}

func TestUnknownCTLRevokeFallsBackToCoreRecovery(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	m.handleModemResetForBinding(binding, ctlRevokeEvent(qmi.ServiceDMS, 0x7f))
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventModemReset {
		t.Fatalf("unknown revoke event=%v, want eventModemReset recovery input", event)
	}
}

func TestRetiredCTLRevokeDoesNotEscalateToCore(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldService := &qmi.DMSService{}
	publishDMSOwnerForTest(t, m, oldService, 0x26)
	newService := &qmi.DMSService{}
	newOwner := replaceDMSOwnerForTest(t, m, newService, 0x27)

	m.handleModemResetForBinding(binding, ctlRevokeEvent(qmi.ServiceDMS, 0x26))
	select {
	case event := <-m.eventCh:
		t.Fatalf("retired revoke queued core event %+v", event)
	default:
	}
	if m.dms != newService || !m.serviceOwnerCurrent(newOwner) {
		t.Fatal("retired revoke modified replacement DMS owner")
	}
}

func TestTargetedWDSRevokePreservesOtherFamily(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldV4 := &qmi.WDSService{}
	v6 := &qmi.WDSService{}
	v4Owner, v6Owner := publishWDSOwnersForRevocationTest(t, m, oldV4, 0x24, v6, 0x25)
	owner, disposition, _ := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x24))
	if disposition != serviceRevocationTargeted || owner != v4Owner {
		t.Fatal("failed to reserve exact WDSv4 revoke")
	}

	replacement := &qmi.WDSService{}
	var allocations atomic.Int32
	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		allocations.Add(1)
		if m.lifecycleMu.TryLock() {
			m.lifecycleMu.Unlock()
			t.Error("targeted allocation did not hold lifecycleMu")
		}
		return replacement, nil
	}
	m.lifecycleMu.Lock()
	err := m.recoverRevokedService(context.Background(), currentCoreTokenForTest(m), owner)
	m.lifecycleMu.Unlock()
	if err != nil {
		t.Fatalf("recover revoked WDSv4: %v", err)
	}
	if allocations.Load() != 1 || m.wds != replacement {
		t.Fatalf("replacement allocation calls=%d field=%p, want 1/%p", allocations.Load(), m.wds, replacement)
	}
	if m.wdsV6 != v6 || !m.serviceOwnerCurrent(v6Owner) {
		t.Fatal("WDSv4 revoke modified WDSv6 owner or pointer")
	}
	if m.serviceOwnerCurrent(v4Owner) {
		t.Fatal("revoked WDSv4 owner remained current")
	}
}

func TestTargetedWDSRevokeRedialsMissingFamily(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	m.cfg.EnableIPv4 = true
	m.cfg.EnableIPv6 = false
	oldWDS := &qmi.WDSService{}
	publishWDSOwnersForRevocationTest(t, m, oldWDS, 0x28, nil, 0)
	m.mu.Lock()
	m.desiredConnection = true
	m.state = StateConnected
	m.handleV4 = 0x100
	m.mu.Unlock()
	owner, disposition, _ := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x28))
	if disposition != serviceRevocationTargeted {
		t.Fatal("failed to reserve WDSv4 revoke for redial")
	}

	replacement := &qmi.WDSService{}
	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		return replacement, nil
	}
	var starts atomic.Int32
	m.startDataCallHook = func(_ context.Context, service *qmi.WDSService, family uint8) (uint32, error) {
		starts.Add(1)
		if service != replacement || family != qmi.IpFamilyV4 {
			t.Errorf("redial service=%p family=%d, want replacement/%d", service, family, qmi.IpFamilyV4)
		}
		return 0x200, nil
	}
	var networkConfigures atomic.Int32
	m.configureNetworkHook = func() error {
		networkConfigures.Add(1)
		return nil
	}

	m.lifecycleMu.Lock()
	err := m.recoverRevokedService(context.Background(), currentCoreTokenForTest(m), owner)
	m.lifecycleMu.Unlock()
	if err != nil {
		t.Fatalf("recover and redial WDSv4: %v", err)
	}
	m.mu.RLock()
	handle := m.handleV4
	m.mu.RUnlock()
	if starts.Load() != 1 || handle != 0x200 || networkConfigures.Load() != 1 {
		t.Fatalf("redial starts=%d handle=0x%x network_configures=%d, want 1/0x200/1",
			starts.Load(), handle, networkConfigures.Load())
	}
}

func TestDuplicateCTLRevokeAllocatesReplacementOnce(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldService := &qmi.DMSService{}
	publishDMSOwnerForTest(t, m, oldService, 0x31)
	replacement := &qmi.DMSService{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var allocations atomic.Int32
	m.newDMSService = func(context.Context, *qmi.Client) (*qmi.DMSService, error) {
		if m.dmsRecoveryMu.TryLock() {
			m.dmsRecoveryMu.Unlock()
			t.Error("targeted DMS allocation did not hold dmsRecoveryMu")
		}
		if m.lifecycleMu.TryLock() {
			m.lifecycleMu.Unlock()
			t.Error("targeted DMS allocation did not hold lifecycleMu")
		}
		if allocations.Add(1) == 1 {
			close(entered)
		}
		<-release
		return replacement, nil
	}

	event := ctlRevokeEvent(qmi.ServiceDMS, 0x31)
	m.handleModemResetForBinding(binding, event)
	<-entered
	m.handleModemResetForBinding(binding, event)
	close(release)
	waitBackgroundTasksForTest(m)

	if got := allocations.Load(); got != 1 {
		t.Fatalf("duplicate revoke allocations=%d, want 1", got)
	}
	if m.dms != replacement {
		t.Fatalf("DMS replacement=%p, want %p", m.dms, replacement)
	}
	select {
	case queued := <-m.eventCh:
		t.Fatalf("exact duplicate revoke queued core event %+v", queued)
	default:
	}
}

func TestTargetedRevokeFailureAlwaysQueuesCoreRecovery(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldService := &qmi.DMSService{}
	publishDMSOwnerForTest(t, m, oldService, 0x35)
	m.uimLastRecoverSignal = time.Now()
	m.newDMSService = func(context.Context, *qmi.Client) (*qmi.DMSService, error) {
		return nil, errors.New("targeted allocation failed")
	}

	m.handleModemResetForBinding(binding, ctlRevokeEvent(qmi.ServiceDMS, 0x35))
	waitBackgroundTasksForTest(m)
	if event := waitInternalRecoveryEvent(t, m.eventCh, time.Second); event != eventCoreRecovery {
		t.Fatalf("targeted failure event=%v, want eventCoreRecovery", event)
	}
}

func TestCapturedRevokeCannotRecoverAfterTransportReplacement(t *testing.T) {
	m, _ := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldService := &qmi.DMSService{}
	publishDMSOwnerForTest(t, m, oldService, 0x32)
	owner, disposition, _ := m.beginServiceOwnerRevocation(binding, ctlRevokeEvent(qmi.ServiceDMS, 0x32))
	if disposition != serviceRevocationTargeted {
		t.Fatal("failed to reserve old DMS revoke")
	}
	oldToken := currentCoreTokenForTest(m)

	_, _ = prepareServiceOwnerTestBinding(t, m)
	newService := &qmi.DMSService{}
	newOwner := publishDMSOwnerForTest(t, m, newService, 0x32)
	var allocations atomic.Int32
	m.newDMSService = func(context.Context, *qmi.Client) (*qmi.DMSService, error) {
		allocations.Add(1)
		return &qmi.DMSService{}, nil
	}

	if err := m.recoverRevokedService(context.Background(), oldToken, owner); !errors.Is(err, errServiceOwnerStale) {
		t.Fatalf("old transport recovery error=%v, want stale owner", err)
	}
	if allocations.Load() != 0 || m.dms != newService || !m.serviceOwnerCurrent(newOwner) {
		t.Fatal("old revoke worker modified replacement transport owner")
	}
}

func TestStopWinsQueuedTargetedRevokeWorker(t *testing.T) {
	m, runCtx := newListenerBindingTestManager(t)
	binding, _ := prepareServiceOwnerTestBinding(t, m)
	oldWDS := &qmi.WDSService{}
	publishWDSOwnersForRevocationTest(t, m, oldWDS, 0x33, nil, 0x34)
	var allocations atomic.Int32
	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		allocations.Add(1)
		return &qmi.WDSService{}, nil
	}
	m.closeWDSService = func(*qmi.WDSService) error { return nil }
	m.closeQMIClientHook = func(*qmi.Client) error { return nil }

	m.lifecycleMu.Lock()
	m.handleModemResetForBinding(binding, ctlRevokeEvent(qmi.ServiceWDS, 0x33))
	stopDone := make(chan error, 1)
	go func() { stopDone <- m.Stop() }()
	<-runCtx.Done()
	m.lifecycleMu.Unlock()
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop after queued revoke: %v", err)
	}
	if allocations.Load() != 0 {
		t.Fatalf("queued revoke allocated %d replacements after Stop", allocations.Load())
	}
	if m.wds != nil {
		t.Fatal("queued revoke republished WDS after Stop")
	}
}
