package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

type serviceRevocationDisposition uint8

const (
	serviceRevocationFallback serviceRevocationDisposition = iota
	serviceRevocationTargeted
	serviceRevocationDuplicate
	serviceRevocationStale
)

// beginServiceOwnerRevocation reserves one exact active owner for a targeted
// CTL revoke worker. It never performs I/O. The revoking phase immediately
// rejects new operations and ordinary indications while keeping the slot
// indexed so no competing allocator can publish a replacement.
func (m *Manager) beginServiceOwnerRevocation(
	binding *listenerBinding,
	event qmi.Event,
) (*serviceOwner, serviceRevocationDisposition, string) {
	if m == nil || binding == nil || event.RevokedClient == nil {
		return nil, serviceRevocationFallback, "missing_target"
	}
	if event.ServiceID != qmi.ServiceControl || event.MessageID != qmi.CTLRevokeClientIDInd {
		return nil, serviceRevocationFallback, "invalid_envelope"
	}

	target := serviceIdentityKey{
		client:    binding.client,
		serviceID: event.RevokedClient.ServiceID,
		clientID:  event.RevokedClient.ClientID,
	}
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.indicationDispatchMu.Unlock()

	if !m.listenerBindingUsableLocked(binding) {
		return nil, serviceRevocationStale, "stale_binding"
	}
	if !m.serviceOwnerRegistryEnabledLocked(binding) {
		return nil, serviceRevocationFallback, "registry_unavailable"
	}

	var matches []*serviceOwner
	for _, owner := range m.serviceOwnersBySlot {
		if owner == nil || owner.phase == serviceOwnerRevoked {
			continue
		}
		if owner.coreGeneration == binding.coreGeneration &&
			owner.listenerBindingID == binding.id &&
			owner.client == binding.client &&
			owner.identity == target {
			matches = append(matches, owner)
		}
	}
	if len(matches) > 1 {
		return nil, serviceRevocationFallback, "ambiguous_target"
	}
	if len(matches) == 1 {
		owner := matches[0]
		if m.serviceOwnersByIdentity[target] != owner || m.serviceOwnersBySlot[owner.slot] != owner {
			return nil, serviceRevocationFallback, "inconsistent_registry"
		}
		switch owner.phase {
		case serviceOwnerActive:
			owner.phase = serviceOwnerRevoking
			return owner, serviceRevocationTargeted, "exact_target"
		case serviceOwnerRevoking:
			return owner, serviceRevocationDuplicate, "already_revoking"
		default:
			return nil, serviceRevocationStale, "retired_target"
		}
	}
	if _, retired := m.serviceOwnerTombstones[target]; retired {
		return nil, serviceRevocationStale, "retired_target"
	}
	return nil, serviceRevocationFallback, "unknown_target"
}

func (m *Manager) serviceOwnerRevoking(owner *serviceOwner) bool {
	if m == nil || owner == nil {
		return false
	}
	m.mu.RLock()
	current := m.serviceOwnerRevokingLocked(owner)
	m.mu.RUnlock()
	return current
}

// handleTargetedServiceRevocation returns true when the event was completely
// handled as targeted, duplicate, or stale. false preserves the historical
// modem-wide fallback for malformed, unknown, or ambiguous revokes.
func (m *Manager) handleTargetedServiceRevocation(binding *listenerBinding, event qmi.Event) bool {
	owner, disposition, reason := m.beginServiceOwnerRevocation(binding, event)
	switch disposition {
	case serviceRevocationDuplicate, serviceRevocationStale:
		m.log.WithField("revoke_reason", reason).Debug("Ignoring duplicate or stale QMI service revoke")
		return true
	case serviceRevocationFallback:
		m.log.WithField("revoke_reason", reason).Warn("QMI service revoke cannot be routed exactly; using core recovery")
		return false
	case serviceRevocationTargeted:
		// Continue below.
	default:
		return false
	}

	launched := m.launchCoreBackgroundTask(func(taskCtx context.Context, token coreSessionToken) {
		if !m.serviceOwnerRevoking(owner) {
			return
		}
		err := m.recoverRevokedService(taskCtx, token, owner)
		if err == nil || errors.Is(err, errServiceOwnerStale) || taskCtx.Err() != nil || !m.coreSessionCurrent(token) {
			return
		}
		m.log.WithField("service_slot", owner.slot.String()).WithError(err).Warn("Targeted QMI service revoke recovery failed")
		m.triggerCoreRecoveryForRevocation(owner, "targeted", err)
	})
	if !launched {
		err := fmt.Errorf("targeted revoke worker admission failed: slot=%s", owner.slot)
		m.triggerCoreRecoveryForRevocation(owner, "admission", err)
	}
	return true
}

func (m *Manager) triggerCoreRecoveryForRevocation(owner *serviceOwner, phase string, cause error) bool {
	if m == nil || owner == nil || cause == nil {
		return false
	}
	request := serviceRecoveryRequest(owner.slot.String(), "CTLRevokeClientID", phase, cause)
	if !m.enqueueCoreRecoveryEvent(request) {
		return false
	}
	m.log.
		WithField("service_slot", owner.slot.String()).
		WithField("phase", phase).
		WithError(cause).
		Warn("Scheduling core recovery after targeted QMI revoke failure")
	return true
}

func (m *Manager) recoverRevokedService(ctx context.Context, token coreSessionToken, owner *serviceOwner) error {
	if owner == nil {
		return fmt.Errorf("%w: missing revoked owner", errServiceOwnerStale)
	}
	if ctx == nil || ctx.Err() != nil || !m.coreSessionCurrent(token) || !m.serviceOwnerRevoking(owner) {
		return staleServiceOperationError(owner.slot)
	}
	err := m.withRevokedServiceRecoveryLock(owner.slot, func() error {
		return m.recoverRevokedServiceExclusive(ctx, token, owner)
	})
	if err != nil {
		return err
	}
	if owner.slot == serviceSlotWMS {
		m.maybeReplayWMSStateAfterRebind("ctl-revoke")
	}
	m.log.WithField("service_slot", owner.slot.String()).Info("Recovered revoked QMI service client")
	return nil
}

func (m *Manager) withRevokedServiceRecoveryLock(slot serviceSlot, fn func() error) error {
	switch slot {
	case serviceSlotDMS:
		m.dmsRecoveryMu.Lock()
		defer m.dmsRecoveryMu.Unlock()
	case serviceSlotNAS:
		m.nasRecoveryMu.Lock()
		defer m.nasRecoveryMu.Unlock()
	case serviceSlotUIM:
		m.uimRecoveryMu.Lock()
		defer m.uimRecoveryMu.Unlock()
	case serviceSlotWMS:
		m.wmsRecoveryMu.Lock()
		defer m.wmsRecoveryMu.Unlock()
	case serviceSlotVOICE:
		m.voiceRecoveryMu.Lock()
		defer m.voiceRecoveryMu.Unlock()
	}
	return fn()
}

func (m *Manager) recoverRevokedServiceExclusive(ctx context.Context, token coreSessionToken, owner *serviceOwner) error {
	switch owner.slot {
	case serviceSlotWDSv4, serviceSlotWDSv6:
		return m.recoverRevokedWDS(ctx, token, owner)
	case serviceSlotDMS:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.dms, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createDMSService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked DMS client: %w", err)
		}
		if err := installManagedService(m, serviceSlotDMS, client, &m.dms, allocated); err != nil {
			return fmt.Errorf("publish replacement DMS owner: %w", err)
		}
	case serviceSlotNAS:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.nas, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createNASService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked NAS client: %w", err)
		}
		if err := installManagedService(m, serviceSlotNAS, client, &m.nas, allocated); err != nil {
			return fmt.Errorf("publish replacement NAS owner: %w", err)
		}
		if err := m.replayNASIndicationsAfterRevoke(ctx, allocated); err != nil {
			return err
		}
	case serviceSlotUIM:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.uim, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createUIMService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked UIM client: %w", err)
		}
		if err := installManagedService(m, serviceSlotUIM, client, &m.uim, allocated); err != nil {
			return fmt.Errorf("publish replacement UIM owner: %w", err)
		}
		indCtx, indCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.IndicationRegister)
		_, err = m.registerUIMIndicationsWithContext(indCtx, allocated)
		indCancel()
		if err != nil {
			return fmt.Errorf("replay UIM indications after revoke: %w", err)
		}
	case serviceSlotWDA:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.wda, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createWDAService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked WDA client: %w", err)
		}
		if err := installManagedService(m, serviceSlotWDA, client, &m.wda, allocated); err != nil {
			return fmt.Errorf("publish replacement WDA owner: %w", err)
		}
	case serviceSlotWMS:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.wms, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createWMSService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked WMS client: %w", err)
		}
		if err := installManagedService(m, serviceSlotWMS, client, &m.wms, allocated); err != nil {
			return fmt.Errorf("publish replacement WMS owner: %w", err)
		}
	case serviceSlotVOICE:
		_, client, detached := detachRevokingManagedService(m, owner.slot, &m.voice, owner, nil)
		if !detached {
			return staleServiceOperationError(owner.slot)
		}
		allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		allocated, err := m.createVOICEService(allocCtx, client)
		cancel()
		if err != nil {
			return fmt.Errorf("allocate revoked VOICE client: %w", err)
		}
		if err := installManagedService(m, serviceSlotVOICE, client, &m.voice, allocated); err != nil {
			return fmt.Errorf("publish replacement VOICE owner: %w", err)
		}
		if cfg, ok := m.voiceIndicationRegistration(); ok {
			indCtx, indCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.IndicationRegister)
			if m.registerVOICEIndications != nil {
				err = m.registerVOICEIndications(indCtx, cfg)
			} else {
				err = allocated.IndicationRegister(indCtx, cfg)
			}
			indCancel()
			if err != nil {
				return fmt.Errorf("replay VOICE indications after revoke: %w", err)
			}
		}
	default:
		return fmt.Errorf("targeted recovery unsupported for slot %s", owner.slot)
	}
	return nil
}

func (m *Manager) replayNASIndicationsAfterRevoke(ctx context.Context, nas *qmi.NASService) error {
	cfg := qmi.NASIndicationRegistration{
		ServingSystemChanged:        true,
		SystemInfo:                  true,
		NetworkTime:                 true,
		SignalInfo:                  true,
		OperatorName:                true,
		NetworkReject:               true,
		IncrementalNetworkScan:      true,
		EventReportSignalThresholds: []int8{-60, -85},
	}
	indCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.IndicationRegister)
	defer cancel()
	if m.registerNASIndications != nil {
		if err := m.registerNASIndications(indCtx, cfg); err != nil {
			return fmt.Errorf("replay NAS indications after revoke: %w", err)
		}
		return nil
	}
	if err := nas.RegisterIndicationsWithConfig(indCtx, cfg); err != nil {
		return fmt.Errorf("replay NAS indications after revoke: %w", err)
	}
	return nil
}

func (m *Manager) recoverRevokedWDS(ctx context.Context, token coreSessionToken, owner *serviceOwner) error {
	var client *qmi.Client
	var detached bool
	switch owner.slot {
	case serviceSlotWDSv4:
		_, client, detached = detachRevokingManagedService(m, owner.slot, &m.wds, owner, func(previous *qmi.WDSService) {
			m.handleV4 = 0
			m.settings = nil
			m.removePendingDataCallsForServiceLocked(previous)
		})
	case serviceSlotWDSv6:
		_, client, detached = detachRevokingManagedService(m, owner.slot, &m.wdsV6, owner, func(previous *qmi.WDSService) {
			m.handleV6 = 0
			m.settings = nil
			m.removePendingDataCallsForServiceLocked(previous)
		})
	}
	if !detached {
		return staleServiceOperationError(owner.slot)
	}

	allocCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
	allocated, err := m.createWDSService(allocCtx, client)
	cancel()
	if err != nil {
		return fmt.Errorf("allocate revoked %s client: %w", owner.slot, err)
	}
	allocated.ProfileIndex = m.cfg.ProfileIndex
	if m.cfg.MuxID > 0 {
		bindCtx, bindCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Dial)
		err = allocated.BindMuxDataPort(bindCtx, qmi.MuxBinding{
			EpType: 0x02, EpIfID: 0x04, MuxID: m.cfg.MuxID, ClientType: 1,
		})
		bindCancel()
		if err != nil {
			closeCtx, closeCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Stop)
			_ = allocated.CloseWithContext(closeCtx)
			closeCancel()
			return fmt.Errorf("bind replacement %s to mux: %w", owner.slot, err)
		}
	}
	if owner.slot == serviceSlotWDSv4 {
		err = installManagedService(m, serviceSlotWDSv4, client, &m.wds, allocated)
	} else {
		err = installManagedService(m, serviceSlotWDSv6, client, &m.wdsV6, allocated)
	}
	if err != nil {
		return fmt.Errorf("publish replacement %s owner: %w", owner.slot, err)
	}

	m.mu.RLock()
	desired := m.desiredConnection
	m.mu.RUnlock()
	if !desired {
		return nil
	}
	dialCtx, dialCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Dial)
	err = m.dialMissingDataFamilies(dialCtx, token)
	dialCancel()
	if err != nil {
		return fmt.Errorf("redial after %s revoke: %w", owner.slot, err)
	}
	if err := m.configureNetwork(); err != nil {
		return fmt.Errorf("configure network after %s revoke: %w", owner.slot, err)
	}
	return nil
}

// removePendingDataCallsForServiceLocked requires m.mu.
func (m *Manager) removePendingDataCallsForServiceLocked(service *qmi.WDSService) {
	if service == nil || len(m.pendingDataCalls) == 0 {
		return
	}
	kept := m.pendingDataCalls[:0]
	for _, call := range m.pendingDataCalls {
		if call.wds != service {
			kept = append(kept, call)
		}
	}
	m.pendingDataCalls = kept
}
