package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

type serviceTimeoutKey struct {
	service    string
	op         string
	ownerEpoch uint64
}

type serviceTimeoutWindow struct {
	first time.Time
	count int
}

func shouldRecoverServiceError(service string, err error, serviceUnavailableText string) bool {
	if err == nil {
		return false
	}
	if isUnsafeServiceOwnerError(err) {
		return true
	}

	if errors.Is(err, qmi.ErrServiceNotSupported) {
		return false
	}

	var notReady *ServiceNotReadyError
	if errors.As(err, &notReady) && strings.EqualFold(strings.TrimSpace(notReady.Service), strings.TrimSpace(service)) {
		return true
	}

	if qe := qmi.GetQMIError(err); qe != nil {
		switch qe.ErrorCode {
		case qmi.QMIErrInvalidID, qmi.QMIErrDeviceNotReady, qmi.QMIErrClientIDsExhausted:
			return true
		}
		if qe.Service == qmi.ServiceControl && qe.MessageID == qmi.CTLGetClientID {
			return true
		}
	}

	lowerErr := strings.ToLower(err.Error())
	lowerSvc := strings.ToLower(strings.TrimSpace(service))
	needle := strings.TrimSpace(serviceUnavailableText)
	if needle == "" {
		needle = fmt.Sprintf("%s service not available", lowerSvc)
	}

	return strings.Contains(lowerErr, strings.ToLower(needle)) ||
		strings.Contains(lowerErr, "qmi 服务未就绪: "+lowerSvc) ||
		strings.Contains(lowerErr, "allocate client id request failed")
}

func isServiceTimeoutError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeoutErr *qmi.TimeoutError
	return errors.As(err, &timeoutErr)
}

func (m *Manager) shouldRecoverServiceOperationError(service string, op string, err error, serviceUnavailableText string) bool {
	return m.shouldRecoverServiceOperationErrorForOwner(serviceOperationOwner{}, service, op, err, serviceUnavailableText)
}

func (m *Manager) shouldRecoverServiceOperationErrorForOwner(
	owner serviceOperationOwner,
	service string,
	op string,
	err error,
	serviceUnavailableText string,
) bool {
	if !m.serviceOperationOwnerCurrent(owner) {
		return false
	}
	if shouldRecoverServiceError(service, err, serviceUnavailableText) {
		return true
	}
	if !isServiceTimeoutError(err) {
		return false
	}
	return m.recordServiceTimeoutFailureForOwner(owner, service, op, err)
}

func (m *Manager) recordServiceTimeoutFailure(service string, op string, err error) bool {
	return m.recordServiceTimeoutFailureForOwner(serviceOperationOwner{}, service, op, err)
}

func (m *Manager) recordServiceTimeoutFailureForOwner(owner serviceOperationOwner, service string, op string, err error) bool {
	if m == nil {
		return false
	}
	if !m.serviceOperationOwnerCurrent(owner) {
		return false
	}
	m.serviceTimeouts.Add(1)
	if m.cfg.RecoveryPolicy.DisableServiceTimeoutRecovery {
		return false
	}

	threshold := m.cfg.RecoveryPolicy.ServiceTimeoutThreshold
	if threshold <= 0 {
		threshold = defaultServiceTimeoutThreshold
	}
	window := m.cfg.RecoveryPolicy.ServiceTimeoutWindow
	if window <= 0 {
		window = defaultServiceTimeoutWindow
	}
	if threshold <= 1 {
		threshold = 1
	}

	now := time.Now()
	key := serviceTimeoutKey{
		service: strings.ToUpper(strings.TrimSpace(service)),
		op:      strings.TrimSpace(op),
	}
	if owner.owner != nil {
		key.ownerEpoch = owner.owner.epoch
	}
	if key.op == "" {
		key.op = "*"
	}

	m.serviceTimeoutMu.Lock()
	if m.serviceTimeoutFailures == nil {
		m.serviceTimeoutFailures = make(map[serviceTimeoutKey]serviceTimeoutWindow)
	}
	state := m.serviceTimeoutFailures[key]
	if state.first.IsZero() || now.Sub(state.first) > window {
		state = serviceTimeoutWindow{first: now}
	}
	state.count++
	m.serviceTimeoutFailures[key] = state
	reached := state.count >= threshold
	firstReached := state.count == threshold
	m.serviceTimeoutMu.Unlock()

	entry := m.log.
		WithField("service_name", key.service).
		WithField("op", key.op).
		WithField("owner_epoch", key.ownerEpoch).
		WithField("timeout_count", state.count).
		WithField("timeout_threshold", threshold).
		WithField("timeout_window_ms", window.Milliseconds())
	if firstReached {
		m.detectTimeoutStorm(key.service)
	}

	if reached {
		if firstReached {
			m.serviceTimeoutRecoveries.Add(1)
			entry.WithError(err).Warn("Service operation timeout threshold reached; enabling recovery")
		} else {
			entry.WithError(err).Debug("Service operation timeout remains above recovery threshold")
		}
		return true
	}
	entry.WithError(err).Debug("Service operation timeout observed below recovery threshold")
	return false
}

func (m *Manager) detectTimeoutStorm(service string) {
	const stormWindow = 5 * time.Second
	const stormMinSvcs = 2
	const stormCooldown = 30 * time.Second

	service = strings.ToUpper(strings.TrimSpace(service))
	if service == "" {
		return
	}
	if !m.canDetectTimeoutStorm() {
		m.clearTimeoutStormCandidates()
		return
	}

	now := time.Now()
	var affectedServices []string

	m.globalTimeoutMu.Lock()
	if m.globalTimeoutServices == nil {
		m.globalTimeoutServices = make(map[string]time.Time)
	}
	for candidate, observedAt := range m.globalTimeoutServices {
		if now.Sub(observedAt) > stormWindow {
			delete(m.globalTimeoutServices, candidate)
		}
	}
	m.globalTimeoutServices[service] = now

	if len(m.globalTimeoutServices) >= stormMinSvcs &&
		(m.globalTimeoutStormAt.IsZero() || now.Sub(m.globalTimeoutStormAt) > stormCooldown) {
		affectedServices = make([]string, 0, len(m.globalTimeoutServices))
		for candidate := range m.globalTimeoutServices {
			affectedServices = append(affectedServices, candidate)
		}
		sort.Strings(affectedServices)
		m.globalTimeoutStormAt = now
		m.globalTimeoutServices = make(map[string]time.Time)
	}
	m.globalTimeoutMu.Unlock()

	if len(affectedServices) == 0 {
		return
	}

	request := recoveryRequest{
		kind:   recoveryKindSoftware,
		reason: recoveryReasonServiceTimeoutStorm,
		detail: strings.Join(affectedServices, ","),
	}
	if !m.enqueueCoreRecoveryEvent(request) {
		return
	}
	m.log.
		WithField("services_affected", len(affectedServices)).
		WithField("services", affectedServices).
		Warn("Timeout storm detected; triggering core recovery")
}

func (m *Manager) canDetectTimeoutStorm() bool {
	m.mu.RLock()
	coreReady := m.coreReady
	stopping := m.state == StateStopping
	m.mu.RUnlock()
	if !coreReady || stopping {
		return false
	}

	m.modemResetMu.Lock()
	recoveryPending := m.modemResetRecovering || m.modemResetEnqueued || m.coreRecoveryEnqueued
	m.modemResetMu.Unlock()
	return !recoveryPending
}

func (m *Manager) clearTimeoutStormCandidates() {
	m.globalTimeoutMu.Lock()
	m.globalTimeoutServices = nil
	m.globalTimeoutMu.Unlock()
}

func (m *Manager) noteServiceOperationSuccess(service string, op string) {
	m.noteServiceOperationSuccessForOwner(serviceOperationOwner{}, service, op)
}

func (m *Manager) noteServiceOperationSuccessForOwner(owner serviceOperationOwner, service string, op string) {
	if m == nil {
		return
	}
	if !m.serviceOperationOwnerCurrent(owner) {
		return
	}
	key := serviceTimeoutKey{
		service: strings.ToUpper(strings.TrimSpace(service)),
		op:      strings.TrimSpace(op),
	}
	if owner.owner != nil {
		key.ownerEpoch = owner.owner.epoch
	}
	if key.op == "" {
		key.op = "*"
	}
	m.serviceTimeoutMu.Lock()
	delete(m.serviceTimeoutFailures, key)
	m.serviceTimeoutMu.Unlock()
}

func (m *Manager) logServiceRecovery(service string, op string, phase string, err error, message string) {
	log := Logger(NewNopLogger())
	if m != nil && m.log != nil {
		log = m.log
	}
	entry := log.WithField("service_name", service).WithField("op", op).WithField("phase", phase)
	if qe := qmi.GetQMIError(err); qe != nil {
		entry = entry.
			WithField("service", fmt.Sprintf("0x%02x", qe.Service)).
			WithField("msg", fmt.Sprintf("0x%04x", qe.MessageID)).
			WithField("error_code", fmt.Sprintf("0x%04x", qe.ErrorCode))
	}
	entry.WithError(err).Warn(message)
}

func (m *Manager) triggerCoreRecoveryFromService(service string, op string, phase string, cause error) bool {
	if m == nil {
		return false
	}

	request := serviceRecoveryRequest(service, op, phase, cause)
	m.mu.RLock()
	coreReady := m.coreReady
	stopping := m.state == StateStopping
	m.mu.RUnlock()
	if !coreReady || stopping {
		return m.enqueueCoreRecoveryEvent(request)
	}

	cooldown := m.uimRecoverCooldown
	if cooldown <= 0 {
		cooldown = defaultUIMRecoverCooldown
	}

	now := time.Now()
	m.uimRecoveryMu.Lock()
	if !m.uimLastRecoverSignal.IsZero() && now.Sub(m.uimLastRecoverSignal) < cooldown {
		m.uimRecoveryMu.Unlock()
		m.log.
			WithField("service_name", service).
			WithField("op", op).
			WithField("phase", phase).
			Debug("Skip core recovery trigger due to cooldown")
		return false
	}
	m.uimLastRecoverSignal = now
	m.uimRecoveryMu.Unlock()

	if !m.enqueueCoreRecoveryEvent(request) {
		return false
	}
	m.log.
		WithField("service_name", service).
		WithField("op", op).
		WithField("phase", "recover-core").
		WithField("recovery_reason", request.reason).
		WithField("recovery_detail", request.detail).
		WithError(cause).
		Warn("Scheduling core recovery due to service failure")
	return true
}

// RequestCoreRecovery asks the manager to run the same core recovery path used
// for modem reset/service-failure handling. It is intended for higher-level
// flows that have already classified a service stall, such as post-eSIM-switch
// convergence.
func (m *Manager) RequestCoreRecovery(reason string) bool {
	if m == nil {
		return false
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "external_request"
	}

	request := explicitRecoveryRequest(reason)
	if !m.enqueueCoreRecoveryEvent(request) {
		return false
	}
	m.log.
		WithField("recovery_reason", request.reason).
		WithField("recovery_detail", request.detail).
		Debug("Core recovery queued")
	return true
}

func (m *Manager) maybeReplayWMSStateAfterRebind(reason string) {
	m.wmsReplayMu.Lock()
	if m.wmsReplayInProgress {
		m.wmsReplayMu.Unlock()
		return
	}
	m.wmsReplayInProgress = true
	m.wmsReplayMu.Unlock()
	defer func() {
		m.wmsReplayMu.Lock()
		m.wmsReplayInProgress = false
		m.wmsReplayMu.Unlock()
	}()

	if m.onWMSRebindReplayHook != nil {
		m.onWMSRebindReplayHook(reason)
		return
	}

	m.log.WithField("reason", reason).Info("Replaying WMS readiness state after WMS rebind")
	m.recoverWMSState()
}

func (m *Manager) withDMSRecovery(op string, fn func(dms *qmi.DMSService) error) error {
	_, err := withDMSRecoveryValue(m, op, func(dms *qmi.DMSService) (struct{}, error) {
		return struct{}{}, fn(dms)
	})
	return err
}

func withDMSRecoveryValue[T any](m *Manager, op string, fn func(dms *qmi.DMSService) (T, error)) (T, error) {
	var zero T

	dms, err := m.ensureDMSService()
	if err != nil {
		if m.shouldRecoverDMSError(op, err) {
			m.logServiceRecovery("DMS", op, "initial", err, "DMS ensure failed (core recovery skipped)")
		}
		m.triggerCoreRecoveryForUnsafeServiceOwner("DMS", op, "ensure", err)
		return zero, err
	}
	owner, ownerErr := captureManagedServiceOwner(m, serviceSlotDMS, dms)
	if ownerErr != nil {
		return zero, ownerErr
	}

	result, err := fn(dms)
	if err == nil {
		if !m.serviceOperationOwnerCurrent(owner) {
			return zero, staleServiceOperationError(serviceSlotDMS)
		}
		m.noteServiceOperationSuccessForOwner(owner, "DMS", op)
		return result, nil
	}
	if !m.shouldRecoverDMSErrorForOwner(owner, op, err) {
		return result, err
	}

	m.logServiceRecovery("DMS", op, "initial", err, "DMS operation failed; rebinding DMS service")

	m.dmsRecoveryMu.Lock()
	dms, rebindErr := m.rebindDMSService("recover:"+op, owner)
	m.dmsRecoveryMu.Unlock()
	if rebindErr != nil {
		if errors.Is(rebindErr, errServiceOwnerStale) {
			return result, err
		}
		m.logServiceRecovery("DMS", op, "rebind", rebindErr, "DMS service rebind failed (core recovery skipped)")
		m.triggerCoreRecoveryForUnsafeServiceOwner("DMS", op, "rebind", rebindErr)
		return zero, fmt.Errorf("%s: DMS rebind failed: %w (initial=%v)", op, rebindErr, err)
	}
	retryOwner, ownerErr := captureManagedServiceOwner(m, serviceSlotDMS, dms)
	if ownerErr != nil {
		return zero, ownerErr
	}

	retryResult, retryErr := fn(dms)
	if retryErr == nil {
		if !m.serviceOperationOwnerCurrent(retryOwner) {
			return zero, staleServiceOperationError(serviceSlotDMS)
		}
		m.noteServiceOperationSuccessForOwner(retryOwner, "DMS", op)
		m.log.WithField("service_name", "DMS").WithField("op", op).WithField("phase", "retry").Info("DMS operation recovered after rebind")
		return retryResult, nil
	}
	if m.shouldRecoverDMSErrorForOwner(retryOwner, op, retryErr) {
		m.logServiceRecovery("DMS", op, "retry", retryErr, "DMS operation still failing after rebind; escalating to core recovery")
		m.triggerCoreRecoveryFromService("DMS", op, "retry", retryErr)
	}
	return retryResult, retryErr
}

func (m *Manager) ensureDMSService() (*qmi.DMSService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("DMS")
	}
	if m.ensureDMSServiceHook != nil {
		return m.ensureDMSServiceHook()
	}

	m.mu.RLock()
	dms := m.dms
	client := m.client
	m.mu.RUnlock()
	if dms != nil {
		return dms, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("DMS")
	}

	m.dmsRecoveryMu.Lock()
	defer m.dmsRecoveryMu.Unlock()

	m.mu.RLock()
	dms = m.dms
	client = m.client
	m.mu.RUnlock()
	if dms != nil {
		return dms, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("DMS")
	}

	allocated, err := qmi.NewDMSService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate DMS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotDMS, client, &m.dms, allocated); err != nil {
		return nil, fmt.Errorf("publish DMS owner: %w", err)
	}
	m.log.Info("DMS service lazily allocated")
	return allocated, nil
}

func (m *Manager) rebindDMSService(reason string, expected serviceOperationOwner) (*qmi.DMSService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("DMS")
	}
	if m.rebindDMSServiceHook != nil {
		if !m.serviceOperationOwnerCurrent(expected) {
			return nil, staleServiceOperationError(serviceSlotDMS)
		}
		return m.rebindDMSServiceHook(reason)
	}

	prev, client, detached := detachManagedServiceIfCurrent(m, serviceSlotDMS, &m.dms, expected)
	if !detached {
		return nil, staleServiceOperationError(serviceSlotDMS)
	}
	if prev != nil {
		closeErr := prev.Close()
		if err := uncertainServiceReleaseError(serviceSlotDMS, closeErr); err != nil {
			m.log.WithError(closeErr).WithField("reason", reason).Warn("DMS client release outcome is uncertain; refusing replacement allocation")
			return nil, err
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("DMS")
	}

	allocated, err := qmi.NewDMSService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate DMS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotDMS, client, &m.dms, allocated); err != nil {
		return nil, fmt.Errorf("publish DMS owner: %w", err)
	}
	m.log.WithField("reason", reason).Info("DMS service rebound")
	return allocated, nil
}

func (m *Manager) shouldRecoverDMSError(op string, err error) bool {
	return m.shouldRecoverServiceOperationError("DMS", op, err, "dms service not available")
}

func (m *Manager) shouldRecoverDMSErrorForOwner(owner serviceOperationOwner, op string, err error) bool {
	return m.shouldRecoverServiceOperationErrorForOwner(owner, "DMS", op, err, "dms service not available")
}

func (m *Manager) withNASRecovery(op string, fn func(nas *qmi.NASService) error) error {
	_, err := withNASRecoveryValue(m, op, func(nas *qmi.NASService) (struct{}, error) {
		return struct{}{}, fn(nas)
	})
	return err
}

func withNASRecoveryValue[T any](m *Manager, op string, fn func(nas *qmi.NASService) (T, error)) (T, error) {
	var zero T

	nas, err := m.ensureNASService()
	if err != nil {
		if m.shouldRecoverNASError(op, err) {
			m.triggerCoreRecoveryFromService("NAS", op, "initial", err)
		}
		return zero, err
	}
	owner, ownerErr := captureManagedServiceOwner(m, serviceSlotNAS, nas)
	if ownerErr != nil {
		return zero, ownerErr
	}

	result, err := fn(nas)
	if err == nil {
		if !m.serviceOperationOwnerCurrent(owner) {
			return zero, staleServiceOperationError(serviceSlotNAS)
		}
		m.noteServiceOperationSuccessForOwner(owner, "NAS", op)
		return result, nil
	}
	if !m.shouldRecoverNASErrorForOwner(owner, op, err) {
		return result, err
	}

	m.logServiceRecovery("NAS", op, "initial", err, "NAS operation failed; rebinding NAS service")

	m.nasRecoveryMu.Lock()
	nas, rebindErr := m.rebindNASService("recover:"+op, owner)
	m.nasRecoveryMu.Unlock()
	if rebindErr != nil {
		if errors.Is(rebindErr, errServiceOwnerStale) {
			return result, err
		}
		m.logServiceRecovery("NAS", op, "rebind", rebindErr, "NAS service rebind failed")
		m.triggerCoreRecoveryFromService("NAS", op, "rebind", rebindErr)
		return zero, fmt.Errorf("%s: NAS rebind failed: %w (initial=%v)", op, rebindErr, err)
	}
	retryOwner, ownerErr := captureManagedServiceOwner(m, serviceSlotNAS, nas)
	if ownerErr != nil {
		return zero, ownerErr
	}

	retryResult, retryErr := fn(nas)
	if retryErr == nil {
		if !m.serviceOperationOwnerCurrent(retryOwner) {
			return zero, staleServiceOperationError(serviceSlotNAS)
		}
		m.noteServiceOperationSuccessForOwner(retryOwner, "NAS", op)
		m.log.WithField("service_name", "NAS").WithField("op", op).WithField("phase", "retry").Info("NAS operation recovered after rebind")
		return retryResult, nil
	}
	if m.shouldRecoverNASErrorForOwner(retryOwner, op, retryErr) {
		m.logServiceRecovery("NAS", op, "retry", retryErr, "NAS operation still failing after rebind")
		m.triggerCoreRecoveryFromService("NAS", op, "retry", retryErr)
	}
	return retryResult, retryErr
}

func (m *Manager) ensureNASService() (*qmi.NASService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("NAS")
	}
	if m.ensureNASServiceHook != nil {
		return m.ensureNASServiceHook()
	}

	m.mu.RLock()
	service := m.nas
	client := m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}

	m.nasRecoveryMu.Lock()
	defer m.nasRecoveryMu.Unlock()

	m.mu.RLock()
	service = m.nas
	client = m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}

	allocated, err := qmi.NewNASService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate NAS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotNAS, client, &m.nas, allocated); err != nil {
		return nil, fmt.Errorf("publish NAS owner: %w", err)
	}
	m.log.Info("NAS service lazily allocated")
	return allocated, nil
}

func (m *Manager) rebindNASService(reason string, expected serviceOperationOwner) (*qmi.NASService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("NAS")
	}
	if m.rebindNASServiceHook != nil {
		if !m.serviceOperationOwnerCurrent(expected) {
			return nil, staleServiceOperationError(serviceSlotNAS)
		}
		return m.rebindNASServiceHook(reason)
	}

	prev, client, detached := detachManagedServiceIfCurrent(m, serviceSlotNAS, &m.nas, expected)
	if !detached {
		return nil, staleServiceOperationError(serviceSlotNAS)
	}
	if prev != nil {
		closeErr := prev.Close()
		if err := uncertainServiceReleaseError(serviceSlotNAS, closeErr); err != nil {
			m.log.WithError(closeErr).WithField("reason", reason).Warn("NAS client release outcome is uncertain; refusing replacement allocation")
			return nil, err
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("NAS")
	}

	allocated, err := qmi.NewNASService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate NAS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotNAS, client, &m.nas, allocated); err != nil {
		return nil, fmt.Errorf("publish NAS owner: %w", err)
	}
	m.log.WithField("reason", reason).Info("NAS service rebound")
	return allocated, nil
}

func (m *Manager) shouldRecoverNASError(op string, err error) bool {
	return m.shouldRecoverServiceOperationError("NAS", op, err, "nas service not available")
}

func (m *Manager) shouldRecoverNASErrorForOwner(owner serviceOperationOwner, op string, err error) bool {
	return m.shouldRecoverServiceOperationErrorForOwner(owner, "NAS", op, err, "nas service not available")
}

func (m *Manager) withWMSRecovery(op string, fn func(wms *qmi.WMSService) error) error {
	_, err := withWMSRecoveryValue(m, op, func(wms *qmi.WMSService) (struct{}, error) {
		return struct{}{}, fn(wms)
	})
	return err
}

func withWMSRecoveryValue[T any](m *Manager, op string, fn func(wms *qmi.WMSService) (T, error)) (T, error) {
	var zero T

	wms, err := m.ensureWMSService()
	if err != nil {
		if m.shouldRecoverWMSError(op, err) {
			m.logServiceRecovery("WMS", op, "initial", err, "WMS ensure failed (core recovery skipped)")
		}
		m.triggerCoreRecoveryForUnsafeServiceOwner("WMS", op, "ensure", err)
		return zero, err
	}
	owner, ownerErr := captureManagedServiceOwner(m, serviceSlotWMS, wms)
	if ownerErr != nil {
		return zero, ownerErr
	}

	result, err := fn(wms)
	if err == nil {
		if !m.serviceOperationOwnerCurrent(owner) {
			return zero, staleServiceOperationError(serviceSlotWMS)
		}
		m.noteServiceOperationSuccessForOwner(owner, "WMS", op)
		return result, nil
	}
	if !m.shouldRecoverWMSErrorForOwner(owner, op, err) {
		return result, err
	}

	m.logServiceRecovery("WMS", op, "initial", err, "WMS operation failed; rebinding WMS service")

	m.wmsRecoveryMu.Lock()
	wms, rebindErr := m.rebindWMSService("recover:"+op, owner)
	m.wmsRecoveryMu.Unlock()
	if rebindErr != nil {
		if errors.Is(rebindErr, errServiceOwnerStale) {
			return result, err
		}
		m.logServiceRecovery("WMS", op, "rebind", rebindErr, "WMS service rebind failed (core recovery skipped)")
		m.triggerCoreRecoveryForUnsafeServiceOwner("WMS", op, "rebind", rebindErr)
		return zero, fmt.Errorf("%s: WMS rebind failed: %w (initial=%v)", op, rebindErr, err)
	}
	retryOwner, ownerErr := captureManagedServiceOwner(m, serviceSlotWMS, wms)
	if ownerErr != nil {
		return zero, ownerErr
	}

	retryResult, retryErr := fn(wms)
	if retryErr == nil {
		if !m.serviceOperationOwnerCurrent(retryOwner) {
			return zero, staleServiceOperationError(serviceSlotWMS)
		}
		m.noteServiceOperationSuccessForOwner(retryOwner, "WMS", op)
		m.log.WithField("service_name", "WMS").WithField("op", op).WithField("phase", "retry").Info("WMS operation recovered after rebind")
		return retryResult, nil
	}
	if m.shouldRecoverWMSErrorForOwner(retryOwner, op, retryErr) {
		m.logServiceRecovery("WMS", op, "retry", retryErr, "WMS operation still failing after rebind (core recovery skipped)")
	}
	return retryResult, retryErr
}

func (m *Manager) ensureWMSService() (*qmi.WMSService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("WMS")
	}
	if m.ensureWMSServiceHook != nil {
		return m.ensureWMSServiceHook()
	}

	m.mu.RLock()
	service := m.wms
	client := m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("WMS")
	}

	m.wmsRecoveryMu.Lock()
	defer m.wmsRecoveryMu.Unlock()

	m.mu.RLock()
	service = m.wms
	client = m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("WMS")
	}

	allocated, err := qmi.NewWMSService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate WMS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotWMS, client, &m.wms, allocated); err != nil {
		return nil, fmt.Errorf("publish WMS owner: %w", err)
	}
	m.log.Info("WMS service lazily allocated")
	return allocated, nil
}

func (m *Manager) rebindWMSService(reason string, expected serviceOperationOwner) (*qmi.WMSService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("WMS")
	}
	if m.rebindWMSServiceHook != nil {
		if !m.serviceOperationOwnerCurrent(expected) {
			return nil, staleServiceOperationError(serviceSlotWMS)
		}
		rebound, err := m.rebindWMSServiceHook(reason)
		if err == nil && rebound != nil {
			m.maybeReplayWMSStateAfterRebind(reason)
		}
		return rebound, err
	}

	prev, client, detached := detachManagedServiceIfCurrent(m, serviceSlotWMS, &m.wms, expected)
	if !detached {
		return nil, staleServiceOperationError(serviceSlotWMS)
	}
	if prev != nil {
		closeErr := prev.Close()
		if err := uncertainServiceReleaseError(serviceSlotWMS, closeErr); err != nil {
			m.log.WithError(closeErr).WithField("reason", reason).Warn("WMS client release outcome is uncertain; refusing replacement allocation")
			return nil, err
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("WMS")
	}

	allocated, err := qmi.NewWMSService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate WMS client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotWMS, client, &m.wms, allocated); err != nil {
		return nil, fmt.Errorf("publish WMS owner: %w", err)
	}
	m.log.WithField("reason", reason).Info("WMS service rebound")
	m.maybeReplayWMSStateAfterRebind(reason)
	return allocated, nil
}

func (m *Manager) shouldRecoverWMSError(op string, err error) bool {
	return m.shouldRecoverServiceOperationError("WMS", op, err, "wms service not available")
}

func (m *Manager) shouldRecoverWMSErrorForOwner(owner serviceOperationOwner, op string, err error) bool {
	return m.shouldRecoverServiceOperationErrorForOwner(owner, "WMS", op, err, "wms service not available")
}

func (m *Manager) withVOICERecovery(op string, fn func(voice *qmi.VOICEService) error) error {
	_, err := withVOICERecoveryValue(m, op, func(voice *qmi.VOICEService) (struct{}, error) {
		return struct{}{}, fn(voice)
	})
	return err
}

func withVOICERecoveryValue[T any](m *Manager, op string, fn func(voice *qmi.VOICEService) (T, error)) (T, error) {
	var zero T

	voice, err := m.ensureVOICEService()
	if err != nil {
		if m.shouldRecoverVOICEError(op, err) {
			m.logServiceRecovery("VOICE", op, "initial", err, "VOICE ensure failed (core recovery skipped)")
		}
		m.triggerCoreRecoveryForUnsafeServiceOwner("VOICE", op, "ensure", err)
		return zero, err
	}
	owner, ownerErr := captureManagedServiceOwner(m, serviceSlotVOICE, voice)
	if ownerErr != nil {
		return zero, ownerErr
	}

	result, err := fn(voice)
	if err == nil {
		if !m.serviceOperationOwnerCurrent(owner) {
			return zero, staleServiceOperationError(serviceSlotVOICE)
		}
		m.noteServiceOperationSuccessForOwner(owner, "VOICE", op)
		return result, nil
	}
	if !m.shouldRecoverVOICEErrorForOwner(owner, op, err) {
		return result, err
	}

	m.logServiceRecovery("VOICE", op, "initial", err, "VOICE operation failed; rebinding VOICE service")

	m.voiceRecoveryMu.Lock()
	voice, rebindErr := m.rebindVOICEService("recover:"+op, owner)
	m.voiceRecoveryMu.Unlock()
	if rebindErr != nil {
		if errors.Is(rebindErr, errServiceOwnerStale) {
			return result, err
		}
		m.logServiceRecovery("VOICE", op, "rebind", rebindErr, "VOICE service rebind failed (core recovery skipped)")
		m.triggerCoreRecoveryForUnsafeServiceOwner("VOICE", op, "rebind", rebindErr)
		return zero, fmt.Errorf("%s: VOICE rebind failed: %w (initial=%v)", op, rebindErr, err)
	}
	retryOwner, ownerErr := captureManagedServiceOwner(m, serviceSlotVOICE, voice)
	if ownerErr != nil {
		return zero, ownerErr
	}

	retryResult, retryErr := fn(voice)
	if retryErr == nil {
		if !m.serviceOperationOwnerCurrent(retryOwner) {
			return zero, staleServiceOperationError(serviceSlotVOICE)
		}
		m.noteServiceOperationSuccessForOwner(retryOwner, "VOICE", op)
		m.log.WithField("service_name", "VOICE").WithField("op", op).WithField("phase", "retry").Info("VOICE operation recovered after rebind")
		return retryResult, nil
	}
	if m.shouldRecoverVOICEErrorForOwner(retryOwner, op, retryErr) {
		m.logServiceRecovery("VOICE", op, "retry", retryErr, "VOICE operation still failing after rebind (core recovery skipped)")
	}
	return retryResult, retryErr
}

func (m *Manager) ensureVOICEService() (*qmi.VOICEService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("VOICE")
	}
	if m.ensureVOICEServiceHook != nil {
		return m.ensureVOICEServiceHook()
	}

	m.mu.RLock()
	service := m.voice
	client := m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("VOICE")
	}

	m.voiceRecoveryMu.Lock()
	defer m.voiceRecoveryMu.Unlock()

	m.mu.RLock()
	service = m.voice
	client = m.client
	m.mu.RUnlock()
	if service != nil {
		return service, nil
	}
	if client == nil {
		return nil, ErrServiceNotReady("VOICE")
	}

	allocated, err := qmi.NewVOICEService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate VOICE client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotVOICE, client, &m.voice, allocated); err != nil {
		return nil, fmt.Errorf("publish VOICE owner: %w", err)
	}
	m.log.Info("VOICE service lazily allocated")
	return allocated, nil
}

func (m *Manager) rebindVOICEService(reason string, expected serviceOperationOwner) (*qmi.VOICEService, error) {
	if m == nil {
		return nil, ErrServiceNotReady("VOICE")
	}
	if m.rebindVOICEServiceHook != nil {
		if !m.serviceOperationOwnerCurrent(expected) {
			return nil, staleServiceOperationError(serviceSlotVOICE)
		}
		return m.rebindVOICEServiceHook(reason)
	}

	prev, client, detached := detachManagedServiceIfCurrent(m, serviceSlotVOICE, &m.voice, expected)
	if !detached {
		return nil, staleServiceOperationError(serviceSlotVOICE)
	}
	if prev != nil {
		closeErr := prev.Close()
		if err := uncertainServiceReleaseError(serviceSlotVOICE, closeErr); err != nil {
			m.log.WithError(closeErr).WithField("reason", reason).Warn("VOICE client release outcome is uncertain; refusing replacement allocation")
			return nil, err
		}
	}
	if client == nil {
		return nil, ErrServiceNotReady("VOICE")
	}

	allocated, err := qmi.NewVOICEService(client)
	if err != nil {
		return nil, fmt.Errorf("allocate VOICE client failed: %w", err)
	}
	if err := installManagedService(m, serviceSlotVOICE, client, &m.voice, allocated); err != nil {
		return nil, fmt.Errorf("publish VOICE owner: %w", err)
	}
	m.log.WithField("reason", reason).Info("VOICE service rebound")
	return allocated, nil
}

func (m *Manager) shouldRecoverVOICEError(op string, err error) bool {
	return m.shouldRecoverServiceOperationError("VOICE", op, err, "voice service not available")
}

func (m *Manager) shouldRecoverVOICEErrorForOwner(owner serviceOperationOwner, op string, err error) bool {
	return m.shouldRecoverServiceOperationErrorForOwner(owner, "VOICE", op, err, "voice service not available")
}
