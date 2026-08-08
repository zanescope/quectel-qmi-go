package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

var (
	errServiceOwnerIdentityCollision = errors.New("QMI service identity is already active")
	errServiceOwnerIdentityReused    = errors.New("QMI service identity was reused on the same transport")
	errServiceOwnerReleaseUncertain  = errors.New("QMI service client release outcome is uncertain")
)

type serviceSlot uint8

const (
	serviceSlotWDSv4 serviceSlot = iota + 1
	serviceSlotWDSv6
	serviceSlotNAS
	serviceSlotDMS
	serviceSlotUIM
	serviceSlotWDA
	serviceSlotWMS
	serviceSlotIMS
	serviceSlotIMSA
	serviceSlotIMSP
	serviceSlotVOICE
)

func (s serviceSlot) String() string {
	switch s {
	case serviceSlotWDSv4:
		return "WDSv4"
	case serviceSlotWDSv6:
		return "WDSv6"
	case serviceSlotNAS:
		return "NAS"
	case serviceSlotDMS:
		return "DMS"
	case serviceSlotUIM:
		return "UIM"
	case serviceSlotWDA:
		return "WDA"
	case serviceSlotWMS:
		return "WMS"
	case serviceSlotIMS:
		return "IMS"
	case serviceSlotIMSA:
		return "IMSA"
	case serviceSlotIMSP:
		return "IMSP"
	case serviceSlotVOICE:
		return "VOICE"
	default:
		return fmt.Sprintf("service-slot-%d", uint8(s))
	}
}

func serviceIDForSlot(slot serviceSlot) uint8 {
	switch slot {
	case serviceSlotWDSv4, serviceSlotWDSv6:
		return qmi.ServiceWDS
	case serviceSlotNAS:
		return qmi.ServiceNAS
	case serviceSlotDMS:
		return qmi.ServiceDMS
	case serviceSlotUIM:
		return qmi.ServiceUIM
	case serviceSlotWDA:
		return qmi.ServiceWDA
	case serviceSlotWMS:
		return qmi.ServiceWMS
	case serviceSlotIMS:
		return qmi.ServiceIMS
	case serviceSlotIMSA:
		return qmi.ServiceIMSA
	case serviceSlotIMSP:
		return qmi.ServiceIMSP
	case serviceSlotVOICE:
		return qmi.ServiceVOICE
	default:
		return qmi.ServiceControl
	}
}

type serviceOwnerPhase uint8

const (
	serviceOwnerActive serviceOwnerPhase = iota + 1
	serviceOwnerRevoked
)

// serviceIdentityKey is the complete identity carried on the QMUX wire. The
// client pointer is essential: client IDs may be reused after a transport is
// replaced, while they are deliberately never reused within one live client.
type serviceIdentityKey struct {
	client    *qmi.Client
	serviceID uint8
	clientID  uint8
}

// serviceOwner is an immutable token except for phase, which is only changed
// while m.mu is held. epoch is globally monotonic for the Manager lifetime.
type serviceOwner struct {
	epoch             uint64
	coreGeneration    uint64
	listenerBindingID uint64
	client            *qmi.Client
	runCtx            context.Context
	slot              serviceSlot
	identity          serviceIdentityKey
	instance          any
	phase             serviceOwnerPhase
}

// resetServiceOwnerRegistryForBindingLocked starts a fresh identity namespace
// for one exact transport. It requires m.mu and is called when the listener is
// published, before any service wrapper from that transport can be installed.
func (m *Manager) resetServiceOwnerRegistryForBindingLocked(binding *listenerBinding) {
	for _, owner := range m.serviceOwnersBySlot {
		if owner != nil {
			owner.phase = serviceOwnerRevoked
			if m.serviceOwnerTombstones == nil {
				m.serviceOwnerTombstones = make(map[serviceIdentityKey]struct{})
			}
			m.serviceOwnerTombstones[owner.identity] = struct{}{}
		}
	}
	m.serviceOwnersBySlot = nil
	m.serviceOwnersByIdentity = nil
	m.serviceOwnerGeneration = 0
	m.serviceOwnerBindingID = 0
	if binding != nil {
		if m.serviceOwnerClient != binding.client {
			m.serviceOwnerTombstones = nil
		}
		m.serviceOwnerClient = binding.client
		m.serviceOwnerGeneration = binding.coreGeneration
		m.serviceOwnerBindingID = binding.id
	}
}

// serviceOwnerRegistryEnabledLocked reports whether exact service ownership is
// authoritative for binding. Tests that install synthetic zero-value services
// without a live listener retain their historical direct path.
func (m *Manager) serviceOwnerRegistryEnabledLocked(binding *listenerBinding) bool {
	return binding != nil &&
		m.serviceOwnerGeneration != 0 &&
		m.serviceOwnerGeneration == binding.coreGeneration &&
		m.serviceOwnerBindingID == binding.id &&
		m.listenerBindingOwnedLocked(binding)
}

// serviceOwnerCurrentLocked requires m.mu.
func (m *Manager) serviceOwnerCurrentLocked(owner *serviceOwner) bool {
	if owner == nil || owner.phase != serviceOwnerActive {
		return false
	}
	binding := m.listenerBinding
	if !m.serviceOwnerRegistryEnabledLocked(binding) ||
		owner.coreGeneration != binding.coreGeneration ||
		owner.listenerBindingID != binding.id ||
		owner.client != binding.client ||
		owner.runCtx == nil || owner.runCtx.Err() != nil {
		return false
	}
	return m.serviceOwnersBySlot[owner.slot] == owner &&
		m.serviceOwnersByIdentity[owner.identity] == owner
}

// publishServiceOwnerLocked activates one service wrapper. It requires m.mu.
// A tuple that has ever been retired on this same qmi.Client is rejected: an
// old indication still buffered below the manager has no allocation epoch and
// would otherwise be indistinguishable from the replacement session.
func (m *Manager) publishServiceOwnerLocked(slot serviceSlot, clientID uint8, instance any) (*serviceOwner, error) {
	binding := m.listenerBinding
	if !m.serviceOwnerRegistryEnabledLocked(binding) {
		if m.serviceOwnerClient != nil || m.serviceOwnerGeneration != 0 || m.serviceOwnerBindingID != 0 {
			return nil, ErrServiceNotReady(slot.String())
		}
		return nil, nil
	}
	identity := serviceIdentityKey{
		client:    binding.client,
		serviceID: serviceIDForSlot(slot),
		clientID:  clientID,
	}
	if _, tombstoned := m.serviceOwnerTombstones[identity]; tombstoned {
		return nil, fmt.Errorf("%w: slot=%s service=0x%02x client=0x%02x",
			errServiceOwnerIdentityReused, slot, identity.serviceID, identity.clientID)
	}
	if current := m.serviceOwnersByIdentity[identity]; current != nil && current.phase == serviceOwnerActive {
		return nil, fmt.Errorf("%w: requested_slot=%s active_slot=%s service=0x%02x client=0x%02x",
			errServiceOwnerIdentityCollision, slot, current.slot, identity.serviceID, identity.clientID)
	}
	if current := m.serviceOwnersBySlot[slot]; current != nil && current.phase == serviceOwnerActive {
		return nil, fmt.Errorf("%w: slot=%s already owns service=0x%02x client=0x%02x",
			errServiceOwnerIdentityCollision, slot, current.identity.serviceID, current.identity.clientID)
	}

	m.nextServiceOwnerEpoch++
	owner := &serviceOwner{
		epoch:             m.nextServiceOwnerEpoch,
		coreGeneration:    binding.coreGeneration,
		listenerBindingID: binding.id,
		client:            binding.client,
		runCtx:            binding.runCtx,
		slot:              slot,
		identity:          identity,
		instance:          instance,
		phase:             serviceOwnerActive,
	}
	if m.serviceOwnersBySlot == nil {
		m.serviceOwnersBySlot = make(map[serviceSlot]*serviceOwner)
	}
	if m.serviceOwnersByIdentity == nil {
		m.serviceOwnersByIdentity = make(map[serviceIdentityKey]*serviceOwner)
	}
	if m.serviceOwnerTombstones == nil {
		m.serviceOwnerTombstones = make(map[serviceIdentityKey]struct{})
	}
	m.serviceOwnersBySlot[slot] = owner
	m.serviceOwnersByIdentity[identity] = owner
	return owner, nil
}

// revokeServiceOwnerLocked requires m.mu. The tuple remains tombstoned until
// the listener/qmi.Client is replaced.
func (m *Manager) revokeServiceOwnerLocked(slot serviceSlot) *serviceOwner {
	owner := m.serviceOwnersBySlot[slot]
	if owner == nil {
		return nil
	}
	owner.phase = serviceOwnerRevoked
	delete(m.serviceOwnersBySlot, slot)
	if m.serviceOwnersByIdentity[owner.identity] == owner {
		delete(m.serviceOwnersByIdentity, owner.identity)
	}
	if m.serviceOwnerTombstones == nil {
		m.serviceOwnerTombstones = make(map[serviceIdentityKey]struct{})
	}
	m.serviceOwnerTombstones[owner.identity] = struct{}{}
	return owner
}

// revokeAllServiceOwnersLocked requires m.mu.
func (m *Manager) revokeAllServiceOwnersLocked() {
	for slot := range m.serviceOwnersBySlot {
		m.revokeServiceOwnerLocked(slot)
	}
}

// serviceOwnerForEventLocked captures the exact owner at listener receive
// time. required is false only for the legacy/synthetic path where no live
// registry exists. It requires m.mu.
func (m *Manager) serviceOwnerForEventLocked(binding *listenerBinding, event qmi.Event) (owner *serviceOwner, required bool) {
	if !m.serviceOwnerRegistryEnabledLocked(binding) {
		return nil, false
	}
	if event.ServiceID == qmi.ServiceControl || event.Type == qmi.EventModemReset {
		return nil, false
	}
	identity := serviceIdentityKey{
		client:    binding.client,
		serviceID: event.ServiceID,
		clientID:  event.ClientID,
	}
	owner = m.serviceOwnersByIdentity[identity]
	if !m.serviceOwnerCurrentLocked(owner) {
		return nil, true
	}
	return owner, true
}

func (m *Manager) serviceOwnerCurrent(owner *serviceOwner) bool {
	if m == nil || owner == nil {
		return false
	}
	m.mu.RLock()
	current := m.serviceOwnerCurrentLocked(owner)
	m.mu.RUnlock()
	return current
}

type managedService interface {
	ClientID() uint8
	Close() error
}

// installManagedService publishes the field and owner in one state commit.
// Candidate Close, when needed, happens after all manager locks are released.
func installManagedService[T interface {
	managedService
	comparable
}](m *Manager, slot serviceSlot, expectedClient *qmi.Client, field *T, candidate T) error {
	var zero T
	if m == nil || field == nil || candidate == zero {
		return ErrServiceNotReady(slot.String())
	}

	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	if m.client != expectedClient || expectedClient == nil || m.stopped || m.state == StateStopping {
		m.mu.Unlock()
		m.indicationDispatchMu.Unlock()
		_ = candidate.Close()
		return ErrServiceNotReady(slot.String())
	}
	_, err := m.publishServiceOwnerLocked(slot, candidate.ClientID(), candidate)
	if err == nil {
		*field = candidate
	}
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	if err != nil {
		_ = candidate.Close()
		return err
	}
	return nil
}

// detachManagedService revokes ownership before any ReleaseClientID I/O.
func detachManagedService[T comparable](m *Manager, slot serviceSlot, field *T) (previous T, client *qmi.Client) {
	if m == nil || field == nil {
		return previous, nil
	}
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	previous = *field
	var zero T
	*field = zero
	m.revokeServiceOwnerLocked(slot)
	client = m.client
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()
	return previous, client
}

func serviceReleaseAllowsReallocate(err error) bool {
	if err == nil {
		return true
	}
	qmiErr := qmi.GetQMIError(err)
	return qmiErr != nil && qmiErr.ErrorCode == qmi.QMIErrInvalidID
}

func uncertainServiceReleaseError(slot serviceSlot, err error) error {
	if serviceReleaseAllowsReallocate(err) {
		return nil
	}
	return fmt.Errorf("%w: slot=%s: %v", errServiceOwnerReleaseUncertain, slot, err)
}

func isUnsafeServiceOwnerError(err error) bool {
	return errors.Is(err, errServiceOwnerIdentityCollision) ||
		errors.Is(err, errServiceOwnerIdentityReused) ||
		errors.Is(err, errServiceOwnerReleaseUncertain)
}

func (m *Manager) currentQMIClient() *qmi.Client {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	return client
}

func (m *Manager) triggerCoreRecoveryForUnsafeServiceOwner(service, op, phase string, err error) bool {
	if m == nil || !isUnsafeServiceOwnerError(err) {
		return false
	}
	return m.triggerCoreRecoveryFromService(service, op, phase, err)
}
