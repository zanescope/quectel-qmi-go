package manager

import (
	"errors"
	"io"
	"strings"
	"time"
)

type recoveryKind uint8

const (
	recoveryKindSoftware recoveryKind = iota
	recoveryKindModemReset
)

type recoveryReason string

const (
	recoveryReasonRealModemReset      recoveryReason = "real_modem_reset"
	recoveryReasonServiceTimeoutStorm recoveryReason = "service_timeout_storm"
	recoveryReasonServiceRebindFailed recoveryReason = "service_rebind_failed"
	recoveryReasonTransportDown       recoveryReason = "transport_down"
	recoveryReasonExplicitRequest     recoveryReason = "explicit_request"
	recoveryReasonPostSwitch          recoveryReason = "post_switch_recovery"
)

type recoveryRequest struct {
	kind   recoveryKind
	reason recoveryReason
	detail string
	retry  bool
}

func (m *Manager) coreRecoveryLogger() Logger {
	if m != nil && m.log != nil {
		return m.log
	}
	return NewNopLogger()
}

func modemResetRecoveryRequest(source string) recoveryRequest {
	return recoveryRequest{
		kind:   recoveryKindModemReset,
		reason: recoveryReasonRealModemReset,
		detail: strings.TrimSpace(source),
	}
}

func serviceRecoveryRequest(service string, op string, phase string, cause error) recoveryRequest {
	reason := recoveryReasonServiceRebindFailed
	if isTransportDownError(cause) {
		reason = recoveryReasonTransportDown
	}
	detail := strings.ToLower(strings.TrimSpace(service))
	if op = strings.TrimSpace(op); op != "" {
		detail += ":" + op
	}
	if phase = strings.TrimSpace(phase); phase != "" {
		detail += ":" + phase
	}
	return recoveryRequest{
		kind:   recoveryKindSoftware,
		reason: reason,
		detail: detail,
	}
}

func explicitRecoveryRequest(detail string) recoveryRequest {
	detail = strings.TrimSpace(detail)
	reason := recoveryReasonExplicitRequest
	if strings.Contains(strings.ToLower(detail), "post_switch") {
		reason = recoveryReasonPostSwitch
	}
	return recoveryRequest{
		kind:   recoveryKindSoftware,
		reason: reason,
		detail: detail,
	}
}

func isTransportDownError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "unexpected eof") ||
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "transport endpoint")
}

func (m *Manager) enqueueModemResetEvent(source string) bool {
	if m == nil {
		return false
	}
	m.resetEvents.Add(1)

	request := modemResetRecoveryRequest(source)
	now := time.Now()
	m.modemResetMu.Lock()
	if m.modemResetRecovering {
		if m.modemResetPending {
			m.resetCoalesced.Add(1)
		} else {
			m.modemResetPending = true
		}
		m.modemResetMu.Unlock()
		m.coreRecoveryLogger().WithField("source", source).Warn("Preserved modem reset indication while core recovery is running")
		return true
	}
	if m.modemResetEnqueued {
		m.resetCoalesced.Add(1)
		m.modemResetMu.Unlock()
		m.coreRecoveryLogger().WithField("source", source).Debug("Coalesced duplicate modem reset indication")
		return false
	}
	if !m.modemResetEnqueuedAt.IsZero() && now.Sub(m.modemResetEnqueuedAt) < m.modemResetDedupWindow {
		m.resetCoalesced.Add(1)
		m.modemResetMu.Unlock()
		m.coreRecoveryLogger().WithField("source", source).Debug("Deduplicated modem reset indication inside debounce window")
		return false
	}
	m.modemResetEnqueuedAt = now
	m.modemResetEnqueued = true
	m.modemResetRequest = request
	m.modemResetMu.Unlock()

	m.signalRecoveryEvent(eventModemReset)
	return true
}

func (m *Manager) enqueueCoreRecoveryEvent(request recoveryRequest) bool {
	return m.enqueueCoreRecoveryEventWithOptions(request, true, false)
}

func (m *Manager) enqueueCoreRecoveryRetry(request recoveryRequest, detail string) bool {
	request.kind = recoveryKindSoftware
	request.retry = true
	if retryDetail := strings.TrimSpace(detail); retryDetail != "" {
		if request.detail != "" {
			request.detail += ":"
		}
		request.detail += "retry:" + retryDetail
	}
	return m.enqueueCoreRecoveryEventWithOptions(request, false, true)
}

func (m *Manager) enqueueCoreRecoveryEventWithOptions(request recoveryRequest, countRequest bool, allowNotReady bool) bool {
	if m == nil {
		return false
	}
	request.kind = recoveryKindSoftware
	if request.reason == "" {
		request.reason = recoveryReasonExplicitRequest
	}
	if countRequest {
		m.coreRecoveryRequests.Add(1)
	}

	m.mu.RLock()
	coreReady := m.coreReady
	stopping := m.state == StateStopping
	m.mu.RUnlock()
	if stopping || (!coreReady && !allowNotReady) {
		if countRequest {
			m.coreRecoverySuppressed.Add(1)
		}
		m.coreRecoveryLogger().
			WithField("recovery_reason", request.reason).
			WithField("recovery_detail", request.detail).
			Debug("Suppress core recovery request while core is not ready or manager is stopping")
		return false
	}

	m.modemResetMu.Lock()
	if m.modemResetRecovering || m.modemResetEnqueued || m.coreRecoveryEnqueued {
		if countRequest {
			m.coreRecoveryCoalesced.Add(1)
		}
		m.modemResetMu.Unlock()
		m.coreRecoveryLogger().
			WithField("recovery_reason", request.reason).
			WithField("recovery_detail", request.detail).
			Debug("Coalesced core recovery request")
		return false
	}
	m.coreRecoveryEnqueued = true
	m.coreRecoveryRequest = request
	m.modemResetMu.Unlock()

	if countRequest {
		m.emitEvent(Event{
			Type:   EventCoreRecoveryRequested,
			State:  m.State(),
			Reason: string(request.reason),
		})
	}
	m.signalRecoveryEvent(eventCoreRecovery)
	return true
}

func (m *Manager) signalRecoveryEvent(event internalEvent) {
	select {
	case m.eventCh <- event:
		return
	default:
	}

	m.modemResetMu.Lock()
	if m.modemResetDeferred {
		m.modemResetMu.Unlock()
		return
	}
	m.modemResetDeferred = true
	m.modemResetMu.Unlock()
	m.coreRecoveryLogger().Warn("Internal event queue is full; scheduling deferred core recovery event")

	m.scheduleAfter(200*time.Millisecond, m.retryDeferredRecoveryEvent)
}

func (m *Manager) retryDeferredRecoveryEvent() {
	m.modemResetMu.Lock()
	m.modemResetDeferred = false
	event, ok := m.nextRecoveryEventLocked()
	m.modemResetMu.Unlock()
	if !ok {
		return
	}
	m.signalRecoveryEvent(event)
}

func (m *Manager) nextRecoveryEventLocked() (internalEvent, bool) {
	if m.modemResetEnqueued {
		return eventModemReset, true
	}
	if m.coreRecoveryEnqueued {
		return eventCoreRecovery, true
	}
	return 0, false
}

func (m *Manager) beginRecovery(event internalEvent) (recoveryRequest, bool) {
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()

	if m.modemResetRecovering {
		if event == eventModemReset {
			if m.modemResetPending {
				m.resetCoalesced.Add(1)
			} else {
				m.modemResetPending = true
			}
		} else {
			m.coreRecoveryCoalesced.Add(1)
		}
		return recoveryRequest{}, false
	}

	var request recoveryRequest
	switch {
	case m.modemResetEnqueued:
		request = m.modemResetRequest
		m.modemResetEnqueued = false
		if m.coreRecoveryEnqueued {
			m.coreRecoveryEnqueued = false
			m.coreRecoveryCoalesced.Add(1)
		}
	case m.coreRecoveryEnqueued:
		request = m.coreRecoveryRequest
		m.coreRecoveryEnqueued = false
	default:
		return recoveryRequest{}, false
	}

	m.modemResetRecovering = true
	m.currentRecoveryRequest = request
	return request, true
}

func (m *Manager) finishRecovery() {
	m.modemResetMu.Lock()
	m.modemResetRecovering = false
	m.currentRecoveryRequest = recoveryRequest{}

	pendingReset := m.modemResetPending
	m.modemResetPending = false
	if pendingReset {
		m.modemResetEnqueued = true
		m.modemResetRequest = modemResetRecoveryRequest("pending_after_recovery")
		m.modemResetEnqueuedAt = time.Time{}
	}
	event, ok := m.nextRecoveryEventLocked()
	m.modemResetMu.Unlock()

	if ok {
		m.signalRecoveryEvent(event)
	}
}

func (m *Manager) currentRecovery() recoveryRequest {
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	return m.currentRecoveryRequest
}
