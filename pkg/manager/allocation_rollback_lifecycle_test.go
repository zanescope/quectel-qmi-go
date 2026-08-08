package manager

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

func TestAllocationFailureRollsBackServicesBeforeClientAndNextAttemptReallocates(t *testing.T) {
	allocErr := errors.New("core service allocation failed")
	m := New(Config{
		EnableIPv4:      true,
		DataPlanePolicy: DataPlanePolicyEager,
	}, NewNopLogger())
	defer m.events.Close()

	clients := []*qmi.Client{{}, {}}
	var openIndex int
	m.openQMIClientHook = func(context.Context, string, qmi.ClientOptions) (*qmi.Client, error) {
		client := clients[openIndex]
		openIndex++
		return client, nil
	}

	wdsByClient := make(map[*qmi.Client]*qmi.WDSService)
	m.newWDSService = func(_ context.Context, client *qmi.Client) (*qmi.WDSService, error) {
		wds := &qmi.WDSService{}
		wdsByClient[client] = wds
		return wds, nil
	}
	m.newNASService = func(context.Context, *qmi.Client) (*qmi.NASService, error) {
		return nil, allocErr
	}
	m.newDMSService = func(context.Context, *qmi.Client) (*qmi.DMSService, error) {
		return nil, allocErr
	}
	m.newUIMService = func(context.Context, *qmi.Client) (*qmi.UIMService, error) {
		return nil, allocErr
	}

	var closeOrder []string
	m.closeWDSServiceWithContext = func(_ context.Context, wds *qmi.WDSService) error {
		for i, client := range clients {
			if wdsByClient[client] == wds {
				closeOrder = append(closeOrder, fmt.Sprintf("wds-%d", i+1))
				return nil
			}
		}
		t.Fatalf("closed unknown WDS service %p", wds)
		return nil
	}
	m.closeQMIClientHook = func(client *qmi.Client) error {
		for i, candidate := range clients {
			if candidate == client {
				closeOrder = append(closeOrder, fmt.Sprintf("client-%d", i+1))
				return nil
			}
		}
		t.Fatalf("closed unknown client %p", client)
		return nil
	}

	for attempt := 0; attempt < 2; attempt++ {
		err := m.openClientAndAllocateServices(context.Background(), OpenReasonInitial)
		if !errors.Is(err, allocErr) {
			t.Fatalf("attempt %d error = %v, want %v", attempt+1, err, allocErr)
		}
		m.mu.RLock()
		client, wds := m.client, m.wds
		nas, dms, uim := m.nas, m.dms, m.uim
		m.mu.RUnlock()
		if client != nil || wds != nil || nas != nil || dms != nil || uim != nil {
			t.Fatalf("attempt %d left published allocation: client=%p wds=%p nas=%p dms=%p uim=%p",
				attempt+1, client, wds, nas, dms, uim)
		}
	}

	if openIndex != 2 {
		t.Fatalf("opened clients = %d, want 2", openIndex)
	}
	if wdsByClient[clients[0]] == nil || wdsByClient[clients[1]] == nil ||
		wdsByClient[clients[0]] == wdsByClient[clients[1]] {
		t.Fatal("second attempt did not allocate a fresh WDS service for its client")
	}
	wantOrder := []string{"wds-1", "client-1", "wds-2", "client-2"}
	if !reflect.DeepEqual(closeOrder, wantOrder) {
		t.Fatalf("close order = %v, want %v", closeOrder, wantOrder)
	}
}

func TestFatalStartupTasksReturnContextCancellationWhenTasksReturnNil(t *testing.T) {
	m := New(Config{}, NewNopLogger())
	defer m.events.Close()
	ctx, cancel := context.WithCancel(context.Background())
	tasks := []startupServiceTask{{
		run: func(context.Context) error {
			cancel()
			return nil
		},
	}}

	err := m.runStartupServiceTasks(ctx, true, tasks)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runStartupServiceTasks() error = %v, want context.Canceled", err)
	}
}
