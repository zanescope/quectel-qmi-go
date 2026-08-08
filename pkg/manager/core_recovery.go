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
	kind       recoveryKind
	reason     recoveryReason
	detail     string
	retry      bool
	generation uint64
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

// clearRecoveryStateLocked resets all recovery bookkeeping for the current
// manager lifetime. modemResetMu must be held. Callers that also need m.mu
// always acquire m.mu first.
func (m *Manager) clearRecoveryStateLocked() {
	m.modemResetRecovering = false
	m.modemResetPending = false
	m.modemResetEnqueued = false
	m.modemResetRequest = recoveryRequest{}
	m.coreRecoveryEnqueued = false
	m.coreRecoveryRequest = recoveryRequest{}
	m.currentRecoveryRequest = recoveryRequest{}
	m.coreRecoveryRetryPending = false
	m.coreRecoveryRetryRequest = recoveryRequest{}
	m.modemResetEnqueuedAt = time.Time{}
	m.modemResetDeferred = false
	m.recoveryGeneration = 0
}

func (m *Manager) enqueueModemResetEvent(source string) bool {
	return m.enqueueModemResetEventForBinding(source, nil, nil)
}

// enqueueModemResetEventForBinding records a reset only if binding still owns
// the exact live transport. A nil binding preserves the internal/direct path.
func (m *Manager) enqueueModemResetEventForBinding(source string, binding *listenerBinding, externalEvent *Event) bool {
	if m == nil {
		return false
	}
	m.resetEvents.Add(1)

	request := modemResetRecoveryRequest(source)
	now := time.Now()

	// Lock order for recovery state is m.mu -> modemResetMu.
	m.mu.RLock()
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	inactive := m.stopped ||
		m.state == StateStopping ||
		generation == 0 ||
		runCtx == nil ||
		runCtx.Err() != nil
	if binding != nil && !m.listenerBindingOwnedLocked(binding) {
		inactive = true
	}
	if inactive {
		m.mu.RUnlock()
		m.coreRecoveryLogger().WithField("source", source).Debug("Suppress modem reset while manager is stopping")
		return false
	}
	emitExternal := func() {
		if externalEvent == nil || binding == nil || m.events == nil {
			return
		}
		event := *externalEvent
		event.Generation = generation
		m.events.Emit(event)
	}

	request.generation = generation
	m.modemResetMu.Lock()
	if m.modemResetRecovering {
		if m.modemResetPending {
			m.resetCoalesced.Add(1)
		} else {
			m.modemResetPending = true
		}
		m.recoveryGeneration = generation
		m.modemResetMu.Unlock()
		emitExternal()
		m.mu.RUnlock()
		m.coreRecoveryLogger().WithField("source", source).Warn("Preserved modem reset indication while core recovery is running")
		return true
	}
	if m.recoveryGeneration != 0 && m.recoveryGeneration != generation {
		m.clearRecoveryStateLocked()
	}
	if m.modemResetEnqueued {
		m.resetCoalesced.Add(1)
		m.modemResetMu.Unlock()
		emitExternal()
		m.mu.RUnlock()
		m.coreRecoveryLogger().WithField("source", source).Debug("Coalesced duplicate modem reset indication")
		return false
	}
	if !m.modemResetEnqueuedAt.IsZero() && now.Sub(m.modemResetEnqueuedAt) < m.modemResetDedupWindow {
		m.resetCoalesced.Add(1)
		m.modemResetMu.Unlock()
		emitExternal()
		m.mu.RUnlock()
		m.coreRecoveryLogger().WithField("source", source).Debug("Deduplicated modem reset indication inside debounce window")
		return false
	}
	m.modemResetEnqueuedAt = now
	m.modemResetEnqueued = true
	m.modemResetRequest = request
	m.recoveryGeneration = generation
	m.modemResetMu.Unlock()
	emitExternal()
	m.mu.RUnlock()

	m.signalRecoveryEvent(eventModemReset, generation)
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
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	inactive := m.stopped ||
		stopping ||
		generation == 0 ||
		runCtx == nil ||
		runCtx.Err() != nil
	if inactive || (!coreReady && !allowNotReady) {
		m.mu.RUnlock()
		if countRequest {
			m.coreRecoverySuppressed.Add(1)
		}
		m.coreRecoveryLogger().
			WithField("recovery_reason", request.reason).
			WithField("recovery_detail", request.detail).
			Debug("Suppress core recovery request while core is not ready or manager is stopping")
		return false
	}

	if request.retry {
		// Retries belong to the attempt that scheduled them. Never upgrade a
		// retired timer/request into work for the current session.
		if request.generation == 0 || request.generation != generation {
			m.mu.RUnlock()
			m.coreRecoveryLogger().
				WithField("recovery_reason", request.reason).
				WithField("retry_generation", request.generation).
				WithField("current_generation", generation).
				Debug("Suppress stale core recovery retry")
			return false
		}
	} else {
		request.generation = generation
	}
	m.modemResetMu.Lock()
	if !m.modemResetRecovering &&
		m.recoveryGeneration != 0 &&
		m.recoveryGeneration != generation {
		m.clearRecoveryStateLocked()
	}
	if m.modemResetRecovering || m.modemResetEnqueued || m.coreRecoveryEnqueued {
		if request.retry && m.modemResetRecovering {
			// A backoff timer may fire before the current recovery unwinds. Preserve
			// it in recovery state so finishRecovery can promote it atomically rather
			// than coalescing away the only retry.
			request.generation = generation
			m.coreRecoveryRetryPending = true
			m.coreRecoveryRetryRequest = request
			m.recoveryGeneration = generation
			m.modemResetMu.Unlock()
			m.mu.RUnlock()
			return true
		}

		if countRequest {
			m.coreRecoveryCoalesced.Add(1)
		}
		m.modemResetMu.Unlock()
		m.mu.RUnlock()
		m.coreRecoveryLogger().
			WithField("recovery_reason", request.reason).
			WithField("recovery_detail", request.detail).
			Debug("Coalesced core recovery request")
		return false
	}
	m.coreRecoveryEnqueued = true
	m.coreRecoveryRequest = request
	m.recoveryGeneration = generation
	m.modemResetMu.Unlock()

	if countRequest && m.events != nil {
		m.events.Emit(Event{
			Type:       EventCoreRecoveryRequested,
			Generation: generation,
			State:      m.state,
			Reason:     string(request.reason),
		})
	}
	m.mu.RUnlock()

	m.signalRecoveryEvent(eventCoreRecovery, generation)
	return true
}

func (m *Manager) signalRecoveryEvent(event internalEvent, generation uint64) {
	m.mu.RLock()
	runCtx := m.ctx
	if generation == 0 ||
		m.stopped ||
		m.state == StateStopping ||
		m.coreGeneration.Load() != generation ||
		runCtx == nil ||
		runCtx.Err() != nil {
		m.mu.RUnlock()
		return
	}
	if m.recoveryEventValidatedHook != nil {
		m.recoveryEventValidatedHook()
	}

	switch m.enqueueInternalEventLocked(event, generation) {
	case internalEventEnqueued, internalEventInactive:
		m.mu.RUnlock()
		return
	case internalEventQueueFull:
	}

	m.modemResetMu.Lock()
	_, queued := m.nextRecoveryEventLocked()
	if m.recoveryGeneration != generation || !queued || m.modemResetDeferred {
		m.modemResetMu.Unlock()
		m.mu.RUnlock()
		return
	}
	m.modemResetDeferred = true
	m.modemResetMu.Unlock()
	m.mu.RUnlock()
	m.coreRecoveryLogger().Warn("Internal event queue is full; scheduling deferred core recovery event")

	m.scheduleAfter(200*time.Millisecond, m.retryDeferredRecoveryEvent)
}

func (m *Manager) retryDeferredRecoveryEvent() {
	m.mu.RLock()
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	active := !m.stopped &&
		m.state != StateStopping &&
		generation != 0 &&
		runCtx != nil &&
		runCtx.Err() == nil
	m.modemResetMu.Lock()
	m.modemResetDeferred = false
	if !active || m.recoveryGeneration != generation {
		if !m.modemResetRecovering {
			m.clearRecoveryStateLocked()
		}
		m.modemResetMu.Unlock()
		m.mu.RUnlock()
		return
	}
	event, ok := m.nextRecoveryEventLocked()
	m.modemResetMu.Unlock()
	m.mu.RUnlock()
	if !ok {
		return
	}
	m.signalRecoveryEvent(event, generation)
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
	return m.beginRecoveryForGeneration(event, m.coreGeneration.Load())
}

func (m *Manager) beginRecoveryForGeneration(event internalEvent, eventGeneration uint64) (recoveryRequest, bool) {
	m.mu.Lock()
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	active := !m.stopped &&
		m.state != StateStopping &&
		eventGeneration == generation &&
		generation != 0 &&
		runCtx != nil &&
		runCtx.Err() == nil
	m.modemResetMu.Lock()
	defer func() {
		m.modemResetMu.Unlock()
		m.mu.Unlock()
	}()

	if m.modemResetRecovering {
		if event == eventCoreRecovery {
			m.coreRecoveryCoalesced.Add(1)
		} else {
			m.resetCoalesced.Add(1)
		}
		return recoveryRequest{}, false
	}
	if !active {
		return recoveryRequest{}, false
	}
	if m.recoveryGeneration != generation {
		m.clearRecoveryStateLocked()
		return recoveryRequest{}, false
	}

	// A queued software wake must not consume a later real-reset request. Drop
	// the superseded software request and let the reset's own envelope perform
	// the recovery for this exact generation.
	if event == eventCoreRecovery && m.modemResetEnqueued {
		m.coreRecoveryEnqueued = false
		m.coreRecoveryRequest = recoveryRequest{}
		m.coreRecoveryCoalesced.Add(1)
		return recoveryRequest{}, false
	}

	var request recoveryRequest
	switch {
	case event == eventModemReset && m.modemResetEnqueued:
		request = m.modemResetRequest
		m.modemResetEnqueued = false
		if m.coreRecoveryEnqueued {
			m.coreRecoveryEnqueued = false
			m.coreRecoveryCoalesced.Add(1)
		}
	case event == eventCoreRecovery && m.coreRecoveryEnqueued:
		request = m.coreRecoveryRequest
		m.coreRecoveryEnqueued = false
	default:
		return recoveryRequest{}, false
	}
	if request.generation == 0 || request.generation != generation {
		m.clearRecoveryStateLocked()
		return recoveryRequest{}, false
	}

	m.modemResetRecovering = true
	m.currentRecoveryRequest = request
	m.coreStatusLastErr = ""
	m.setCorePhaseLocked(CorePhaseRecovering, "recovery_begin", string(request.reason), nil)
	m.publishCoreStatusLocked()
	return request, true
}

// finishRecoveryStateLocked retires the active attempt and promotes exactly
// one preserved follow-up. modemResetMu must be held; the caller also holds
// m.mu so generation and lifecycle state cannot change around this commit.
func (m *Manager) finishRecoveryStateLocked(generation uint64) (internalEvent, bool) {
	if !m.modemResetRecovering || m.recoveryGeneration != generation {
		return 0, false
	}

	m.modemResetRecovering = false
	m.currentRecoveryRequest = recoveryRequest{}
	pendingReset := m.modemResetPending
	m.modemResetPending = false
	if pendingReset {
		m.modemResetEnqueued = true
		request := modemResetRecoveryRequest("pending_after_recovery")
		request.generation = generation
		m.modemResetRequest = request
		m.modemResetEnqueuedAt = time.Time{}
		m.coreRecoveryRetryPending = false
		m.coreRecoveryRetryRequest = recoveryRequest{}
	} else if m.coreRecoveryRetryPending && m.coreRecoveryRetryRequest.generation == generation {
		m.coreRecoveryEnqueued = true
		m.coreRecoveryRequest = m.coreRecoveryRetryRequest
		m.coreRecoveryRetryPending = false
		m.coreRecoveryRetryRequest = recoveryRequest{}
	} else if m.coreRecoveryRetryPending {
		m.coreRecoveryRetryPending = false
		m.coreRecoveryRetryRequest = recoveryRequest{}
	}

	event, ok := m.nextRecoveryEventLocked()
	if !ok {
		m.modemResetDeferred = false
		m.recoveryGeneration = 0
	}
	return event, ok
}

func (m *Manager) finishRecovery() {
	m.mu.Lock()
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	active := !m.stopped &&
		m.state != StateStopping &&
		generation != 0 &&
		runCtx != nil &&
		runCtx.Err() == nil
	if m.finishRecoveryLockedHook != nil {
		m.finishRecoveryLockedHook()
	}

	m.modemResetMu.Lock()
	if !m.modemResetRecovering {
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		return
	}
	if !active || m.recoveryGeneration != generation {
		m.clearRecoveryStateLocked()
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		return
	}

	reason := string(m.currentRecoveryRequest.reason)
	event, ok := m.finishRecoveryStateLocked(generation)
	if ok {
		m.setCorePhaseLocked(CorePhaseRecovering, "recovery_follow_up_queued", reason, nil)
	} else {
		stage := m.coreStatusStage
		if stage == "" || stage == "recovery_begin" {
			stage = "recovery_failed"
		}
		m.setCorePhaseLocked(CorePhaseDegraded, stage, reason, nil)
	}
	m.publishCoreStatusLocked()
	m.modemResetMu.Unlock()
	m.mu.Unlock()

	if ok {
		m.signalRecoveryEvent(event, generation)
	}
}

func (m *Manager) currentRecovery() recoveryRequest {
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	return m.currentRecoveryRequest
}
