package manager

import (
	"context"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/netcfg"
	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

type fakeSIMCOMConfigurator struct {
	ip         net.IP
	up         bool
	rawIP      bool
	flushed    bool
	routeFlush bool
}

func (f *fakeSIMCOMConfigurator) SetIPAddress(string, net.IP, int) error   { return nil }
func (f *fakeSIMCOMConfigurator) SetIPv6Address(string, net.IP, int) error { return nil }
func (f *fakeSIMCOMConfigurator) FlushAddresses(string) error {
	f.flushed = true
	f.ip = nil
	return nil
}
func (f *fakeSIMCOMConfigurator) AddDefaultRoute(string, net.IP) error     { return nil }
func (f *fakeSIMCOMConfigurator) AddDefaultRouteDirect(string, bool) error { return nil }
func (f *fakeSIMCOMConfigurator) FlushRoutes(string) error {
	f.routeFlush = true
	return nil
}
func (f *fakeSIMCOMConfigurator) BringUp(string) error {
	f.up = true
	return nil
}
func (f *fakeSIMCOMConfigurator) BringDown(string) error {
	f.up = false
	return nil
}
func (f *fakeSIMCOMConfigurator) SetMTU(string, int) error                 { return nil }
func (f *fakeSIMCOMConfigurator) GetCurrentIP(string) (net.IP, error)      { return f.ip, nil }
func (f *fakeSIMCOMConfigurator) IsUp(string) (bool, error)                { return f.up, nil }
func (f *fakeSIMCOMConfigurator) UpdateDNS(string, string) error           { return nil }
func (f *fakeSIMCOMConfigurator) RestoreDNS() error                        { return nil }
func (f *fakeSIMCOMConfigurator) AddQMAPMux(string, uint8) (string, error) { return "", nil }
func (f *fakeSIMCOMConfigurator) DelQMAPMux(string, uint8) error           { return nil }
func (f *fakeSIMCOMConfigurator) GetQMAPMuxIface(string, uint8) string     { return "" }
func (f *fakeSIMCOMConfigurator) EnableRawIP(string) error {
	f.rawIP = true
	return nil
}

func prepareDirectSIMCOMConnect(t *testing.T, m *Manager) {
	t.Helper()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.coreGeneration.Store(1)
	m.client = &qmi.Client{}
	m.lifetimeActive = true
	m.coreReady = true
	t.Cleanup(m.cancel)
}

func TestEffectiveDataCallModeDefaultsSIMCOMToNDIS(t *testing.T) {
	m := New(Config{Device: ModemDevice{VendorID: VendorSIMCOM}}, nil)
	if got := m.effectiveDataCallMode(); got != DataCallModeSIMCOMNDIS {
		t.Fatalf("effectiveDataCallMode()=%q want %q", got, DataCallModeSIMCOMNDIS)
	}
}

func TestEffectiveDataCallModeAllowsQMIOverride(t *testing.T) {
	m := New(Config{
		Device:       ModemDevice{VendorID: VendorSIMCOM},
		DataCallMode: DataCallModeQMI,
	}, nil)
	if got := m.effectiveDataCallMode(); got != DataCallModeQMI {
		t.Fatalf("effectiveDataCallMode()=%q want %q", got, DataCallModeQMI)
	}
}

func TestSIMCOMNDISConnectRunsATDialAndDHCP(t *testing.T) {
	fakeNet := &fakeSIMCOMConfigurator{}
	netcfg.SetConfigurator(fakeNet)
	t.Cleanup(func() { netcfg.SetConfigurator(nil) })

	var commands []string
	m := New(Config{
		Device: ModemDevice{
			VendorID:     VendorSIMCOM,
			ProductID:    0x9001,
			NetInterface: "wwan0",
			ATPort:       "/dev/ttyUSB2",
		},
		APN:        "cmnet",
		EnableIPv4: true,
		EnableIPv6: true,
	}, nil)
	prepareDirectSIMCOMConnect(t, m)
	m.simcomATCommand = func(ctx context.Context, port string, command string, timeout time.Duration) (string, error) {
		if port != "/dev/ttyUSB2" {
			t.Fatalf("AT port=%q want /dev/ttyUSB2", port)
		}
		commands = append(commands, command)
		return "\r\nOK\r\n", nil
	}
	m.simcomDHCP = func(ctx context.Context, ifname string) error {
		if ifname != "wwan0" {
			t.Fatalf("DHCP ifname=%q want wwan0", ifname)
		}
		fakeNet.ip = net.IPv4(10, 64, 1, 2)
		return nil
	}
	m.desiredConnection = true

	if err := m.doConnect(); err != nil {
		t.Fatalf("doConnect() error = %v", err)
	}
	wantCommands := []string{`AT+CGDCONT=1,"IP","cmnet"`, "AT$QCRMCALL=1,1"}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands=%v want %v", commands, wantCommands)
	}
	if !fakeNet.rawIP || !fakeNet.flushed || !fakeNet.routeFlush || !fakeNet.up {
		t.Fatalf("network prep rawIP=%v flushed=%v routeFlush=%v up=%v", fakeNet.rawIP, fakeNet.flushed, fakeNet.routeFlush, fakeNet.up)
	}
	if got := m.State(); got != StateConnected {
		t.Fatalf("State()=%v want %v", got, StateConnected)
	}
	settings := m.Settings()
	if settings == nil || !settings.IPv4Address.Equal(net.IPv4(10, 64, 1, 2)) {
		t.Fatalf("settings IPv4=%v want 10.64.1.2", settings)
	}
}

func TestSIMCOMNDISConnectRequiresATPort(t *testing.T) {
	fakeNet := &fakeSIMCOMConfigurator{}
	netcfg.SetConfigurator(fakeNet)
	t.Cleanup(func() { netcfg.SetConfigurator(nil) })

	m := New(Config{
		Device: ModemDevice{
			VendorID:     VendorSIMCOM,
			NetInterface: "wwan0",
		},
		APN:        "cmnet",
		EnableIPv4: true,
	}, nil)
	prepareDirectSIMCOMConnect(t, m)
	m.desiredConnection = true

	if err := m.doConnect(); err == nil {
		t.Fatal("doConnect() error = nil, want missing AT port error")
	}
}

func TestDoConnectSerializesConcurrentCalls(t *testing.T) {
	fakeNet := &fakeSIMCOMConfigurator{}
	netcfg.SetConfigurator(fakeNet)
	t.Cleanup(func() { netcfg.SetConfigurator(nil) })

	m := New(Config{
		Device: ModemDevice{
			VendorID:     VendorSIMCOM,
			NetInterface: "wwan0",
			ATPort:       "/dev/ttyUSB2",
		},
		APN:        "cmnet",
		EnableIPv4: true,
	}, nil)
	prepareDirectSIMCOMConnect(t, m)
	m.desiredConnection = true

	var commandCalls atomic.Int32
	firstCommandStarted := make(chan struct{})
	releaseCommands := make(chan struct{})
	var signalFirst sync.Once
	m.simcomATCommand = func(context.Context, string, string, time.Duration) (string, error) {
		commandCalls.Add(1)
		signalFirst.Do(func() { close(firstCommandStarted) })
		<-releaseCommands
		return "\r\nOK\r\n", nil
	}
	m.simcomDHCP = func(context.Context, string) error {
		fakeNet.ip = net.IPv4(10, 64, 1, 2)
		return nil
	}

	errCh := make(chan error, 2)
	go func() { errCh <- m.doConnect() }()
	<-firstCommandStarted
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		errCh <- m.doConnect()
	}()
	<-secondStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseCommands)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("doConnect() error = %v", err)
		}
	}
	if got := commandCalls.Load(); got != 2 {
		t.Fatalf("AT command calls=%d want 2 from one serialized connection", got)
	}
}
