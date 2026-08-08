package qmi

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClientLogfUsesInjectedLogger(t *testing.T) {
	var gotLevel ClientLogLevel
	var gotMessage string
	c := &Client{
		opts: ClientOptions{
			Logf: func(level ClientLogLevel, format string, args ...any) {
				gotLevel = level
				gotMessage = fmt.Sprintf(format, args...)
			},
		},
	}

	c.logf(ClientLogLevelDebug, "hello %s", "qmi")

	if gotLevel != ClientLogLevelDebug {
		t.Fatalf("level=%v want %v", gotLevel, ClientLogLevelDebug)
	}
	if gotMessage != "hello qmi" {
		t.Fatalf("message=%q want %q", gotMessage, "hello qmi")
	}
}

func TestDispatchIndicationClassifiesNASEventReportSeparately(t *testing.T) {
	const clientID = 0x2a

	c := &Client{
		eventCh:            make(chan Event, 1),
		indicationInCh:     nil,
		closeCh:            make(chan struct{}),
		transactions:       make(map[uint32]*transactionEntry),
		recentTransactions: make(map[uint32]recentTransaction),
		clientIDs:          make(map[uint8]uint8),
	}

	c.dispatchIndication(&Packet{
		ServiceType:  ServiceNAS,
		ClientID:     clientID,
		MessageID:    NASEventReportInd,
		IsIndication: true,
	})

	got := <-c.eventCh
	if got.Type != EventNASEventReport {
		t.Fatalf("event type=%v want EventNASEventReport", got.Type)
	}
	if got.ServiceID != ServiceNAS || got.MessageID != NASEventReportInd {
		t.Fatalf("raw ids service=0x%02x msg=0x%04x", got.ServiceID, got.MessageID)
	}
	if got.ClientID != clientID {
		t.Fatalf("client ID=0x%02x want 0x%02x", got.ClientID, clientID)
	}
}

func TestDispatchCTLRevokeCarriesPacketAndTargetIdentity(t *testing.T) {
	const (
		staleClientID = 0x31
		boundClientID = 0x32
	)
	c := &Client{
		eventCh:   make(chan Event, 1),
		closeCh:   make(chan struct{}),
		clientIDs: map[uint8]uint8{ServiceWDS: boundClientID},
	}
	packet := &Packet{
		ServiceType:  ServiceControl,
		ClientID:     0,
		MessageID:    CTLRevokeClientIDInd,
		IsIndication: true,
		TLVs: []TLV{{
			Type:  0x01,
			Value: []byte{ServiceWDS, staleClientID},
		}},
	}

	c.dispatchIndication(packet)

	got := <-c.eventCh
	if got.Type != EventModemReset {
		t.Fatalf("event type=%v want EventModemReset", got.Type)
	}
	if got.ServiceID != ServiceControl || got.ClientID != packet.ClientID || got.Packet != packet {
		t.Fatalf("raw identity service=0x%02x client=0x%02x packet=%p, want CTL/0/%p",
			got.ServiceID, got.ClientID, got.Packet, packet)
	}
	if got.RevokedClient == nil {
		t.Fatal("valid CTL revoke did not expose its target client")
	}
	if got.RevokedClient.ServiceID != ServiceWDS || got.RevokedClient.ClientID != staleClientID {
		t.Fatalf("revoke target=%+v want service=0x%02x client=0x%02x",
			*got.RevokedClient, ServiceWDS, staleClientID)
	}
	if cached := c.GetClientID(ServiceWDS); cached != boundClientID {
		t.Fatalf("stale revoke removed rebound client ID: got=0x%02x want=0x%02x", cached, boundClientID)
	}
}

func TestDispatchCTLRevokeDeletesMatchingCachedIdentity(t *testing.T) {
	const clientID = 0x33
	c := &Client{
		eventCh:   make(chan Event, 1),
		closeCh:   make(chan struct{}),
		clientIDs: map[uint8]uint8{ServiceWDS: clientID},
	}

	c.dispatchIndication(&Packet{
		ServiceType:  ServiceControl,
		ClientID:     0,
		MessageID:    CTLRevokeClientIDInd,
		IsIndication: true,
		TLVs: []TLV{{
			Type:  0x01,
			Value: []byte{ServiceWDS, clientID},
		}},
	})

	if cached := c.GetClientID(ServiceWDS); cached != 0 {
		t.Fatalf("matching revoke left cached client ID 0x%02x", cached)
	}
}

func TestDispatchMalformedCTLRevokeKeepsCompatibilityWithoutInventingTarget(t *testing.T) {
	const boundClientID = 0x44
	c := &Client{
		eventCh:   make(chan Event, 1),
		closeCh:   make(chan struct{}),
		clientIDs: map[uint8]uint8{ServiceWDS: boundClientID},
	}

	c.dispatchIndication(&Packet{
		ServiceType:  ServiceControl,
		ClientID:     0,
		MessageID:    CTLRevokeClientIDInd,
		IsIndication: true,
		TLVs: []TLV{{
			Type:  0x01,
			Value: []byte{ServiceWDS},
		}},
	})

	got := <-c.eventCh
	if got.Type != EventModemReset {
		t.Fatalf("event type=%v want compatibility EventModemReset", got.Type)
	}
	if got.RevokedClient != nil {
		t.Fatalf("malformed revoke invented target %+v", *got.RevokedClient)
	}
	if cached := c.GetClientID(ServiceWDS); cached != boundClientID {
		t.Fatalf("malformed revoke changed cache: got=0x%02x want=0x%02x", cached, boundClientID)
	}
}

func TestCoalescingSeparatesSameServiceIndicationsByClientID(t *testing.T) {
	c := newBackpressuredIndicationClient()
	const (
		firstClientID  = 0x11
		secondClientID = 0x22
	)

	for _, clientID := range []uint8{firstClientID, secondClientID} {
		c.dispatchIndication(&Packet{
			ServiceType:  ServiceWDS,
			ClientID:     clientID,
			MessageID:    WDSGetPktSrvcStatusInd,
			IsIndication: true,
		})
	}

	if got := len(c.coalesced.events); got != 2 {
		t.Fatalf("coalesced event count=%d want 2 distinct client sessions", got)
	}
	for _, wantClientID := range []uint8{firstClientID, secondClientID} {
		got, ok := c.popCoalescedEvent()
		if !ok {
			t.Fatalf("missing coalesced event for client ID 0x%02x", wantClientID)
		}
		if got.ClientID != wantClientID {
			t.Fatalf("coalesced client ID=0x%02x want 0x%02x", got.ClientID, wantClientID)
		}
	}
}

func TestCoalescingSeparatesCTLRevokeTargets(t *testing.T) {
	c := newBackpressuredIndicationClient()
	const (
		firstTarget  = 0x51
		secondTarget = 0x52
	)

	for _, targetClientID := range []uint8{firstTarget, secondTarget} {
		c.dispatchIndication(&Packet{
			ServiceType:  ServiceControl,
			ClientID:     0,
			MessageID:    CTLRevokeClientIDInd,
			IsIndication: true,
			TLVs: []TLV{{
				Type:  0x01,
				Value: []byte{ServiceWDS, targetClientID},
			}},
		})
	}

	if got := len(c.coalesced.events); got != 2 {
		t.Fatalf("coalesced revoke count=%d want 2 distinct targets", got)
	}
	for _, wantTarget := range []uint8{firstTarget, secondTarget} {
		got, ok := c.popCoalescedEvent()
		if !ok {
			t.Fatalf("missing coalesced revoke for target client ID 0x%02x", wantTarget)
		}
		if got.RevokedClient == nil || got.RevokedClient.ClientID != wantTarget {
			t.Fatalf("coalesced revoke target=%+v want client ID 0x%02x", got.RevokedClient, wantTarget)
		}
	}
}

func newBackpressuredIndicationClient() *Client {
	c := &Client{
		opts:              ClientOptions{ReadDeadline: 5 * time.Millisecond},
		indicationInCh:    make(chan Event, 1),
		coalescedSignalCh: make(chan struct{}, 1),
		closeCh:           make(chan struct{}),
		clientIDs:         make(map[uint8]uint8),
		coalesced: coalescedEventStore{
			events: make(map[string]Event),
		},
	}
	c.indicationInCh <- Event{Type: EventUnknown}
	return c
}

func TestModemResetIndicationNotDroppedWhenQueueFull(t *testing.T) {
	c := &Client{
		opts:              ClientOptions{ReadDeadline: 5 * time.Millisecond},
		eventCh:           make(chan Event, 4),
		indicationInCh:    make(chan Event, 1),
		coalescedSignalCh: make(chan struct{}, 1),
		closeCh:           make(chan struct{}),
		coalesced: coalescedEventStore{
			events: make(map[string]Event),
		},
	}

	// Fill indication queue first so enqueueIndication must use coalesced fallback.
	c.indicationInCh <- Event{Type: EventUnknown}
	c.enqueueIndication(Event{
		Type:      EventModemReset,
		ServiceID: ServiceControl,
		MessageID: CTLRevokeClientIDInd,
	})

	done := make(chan struct{})
	c.wg.Add(1)
	go func() {
		c.indicationLoop()
		close(done)
	}()
	defer func() {
		close(c.closeCh)
		<-done
	}()

	deadline := time.After(1 * time.Second)
	for {
		select {
		case evt := <-c.eventCh:
			if evt.Type == EventModemReset {
				return
			}
		case <-deadline:
			t.Fatal("expected EventModemReset to be delivered")
		}
	}
}

func TestSendRequestWithCanceledContextDoesNotQueueWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &Client{
		opts:         DefaultClientOptions(),
		writeCh:      make(chan writeRequest, 1),
		closeCh:      make(chan struct{}),
		transactions: make(map[uint32]*transactionEntry),
	}

	_, err := c.SendRequest(ctx, ServiceUIM, 1, UIMReadRecord, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := len(c.writeCh); got != 0 {
		t.Fatalf("SendRequest queued %d write(s) after context cancellation", got)
	}
	if got := len(c.transactions); got != 0 {
		t.Fatalf("SendRequest left %d transaction(s) after context cancellation", got)
	}
}

func TestCompletedTimedOutTransactionIsRememberedForLateResponse(t *testing.T) {
	c := &Client{
		opts:               DefaultClientOptions(),
		writeCh:            make(chan writeRequest, 1),
		closeCh:            make(chan struct{}),
		transactions:       make(map[uint32]*transactionEntry),
		recentTransactions: make(map[uint32]recentTransaction),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := c.SendRequest(ctx, ServiceUIM, 1, UIMReadRecord, nil)
		errCh <- err
	}()

	wr := <-c.writeCh
	wr.result <- nil

	err := <-errCh
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	key := uint32(ServiceUIM)<<16 | 1
	if !c.isRecentTransaction(key, ServiceUIM, UIMReadRecord) {
		t.Fatalf("timed out UIMReadRecord transaction was not retained for late response matching")
	}
	if got := len(c.transactions); got != 0 {
		t.Fatalf("timed out request left %d active transaction(s)", got)
	}
}
