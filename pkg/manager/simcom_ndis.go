package manager

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/netcfg"
	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
	"go.bug.st/serial"
)

const simcomNDISHandle uint32 = 0x53494d43 // "SIMC"

func (m *Manager) effectiveDataCallMode() DataCallMode {
	if m.cfg.DataCallMode != DataCallModeAuto {
		return m.cfg.DataCallMode
	}
	if m.cfg.Device.IsSIMCOM() {
		return DataCallModeSIMCOMNDIS
	}
	return DataCallModeQMI
}

func (m *Manager) useSIMCOMNDIS() bool {
	return m.effectiveDataCallMode() == DataCallModeSIMCOMNDIS
}

func (m *Manager) simcomATPort() string {
	if strings.TrimSpace(m.cfg.ATPort) != "" {
		return strings.TrimSpace(m.cfg.ATPort)
	}
	return strings.TrimSpace(m.cfg.Device.ATPort)
}

func (m *Manager) runSIMCOMAT(ctx context.Context, command string, timeout time.Duration) (string, error) {
	port := m.simcomATPort()
	if port == "" {
		return "", fmt.Errorf("SIMCOM NDIS mode requires an AT port")
	}
	if m.simcomATCommand != nil {
		return m.simcomATCommand(ctx, port, command, timeout)
	}
	return runATCommand(ctx, port, command, timeout)
}

func (m *Manager) runSIMCOMDHCP(ctx context.Context, ifname string) error {
	if m.simcomDHCP != nil {
		return m.simcomDHCP(ctx, ifname)
	}
	return runDHCPClient(ctx, ifname)
}

func (m *Manager) doSIMCOMNDISConnect(ctx context.Context, generation uint64) error {
	if m.cfg.MuxID > 0 {
		err := fmt.Errorf("SIMCOM NDIS AT data mode does not support QMAP mux")
		m.handleDialFailure(err, generation)
		return err
	}
	if !m.cfg.EnableIPv4 {
		err := fmt.Errorf("SIMCOM NDIS AT data mode requires IPv4")
		m.handleDialFailure(err, generation)
		return err
	}
	if m.cfg.EnableIPv6 {
		m.log.Warn("SIMCOM NDIS AT data mode ignores IPv6; use qmi data mode for IPv6 WDS dialing")
	}
	if m.cfg.Username != "" || m.cfg.Password != "" || m.cfg.AuthType != 0 {
		m.log.Warn("SIMCOM NDIS AT data mode does not apply WDS authentication parameters")
	}

	ifname := strings.TrimSpace(m.cfg.Device.NetInterface)
	if ifname == "" {
		err := fmt.Errorf("SIMCOM NDIS mode requires a network interface")
		m.handleDialFailure(err, generation)
		return err
	}

	m.log.Infof("Starting SIMCOM NDIS data call on %s via AT port %s", ifname, m.simcomATPort())
	if err := netcfg.EnableRawIP(ifname); err != nil {
		m.log.WithError(err).Debug("Raw IP sysfs enable skipped or failed")
	}
	_ = netcfg.FlushAddresses(ifname)
	_ = netcfg.FlushRoutes(ifname)
	if err := netcfg.BringUp(ifname); err != nil {
		m.log.WithError(err).Warn("Failed to bring SIMCOM network interface up before dialing")
	}

	if apn := strings.TrimSpace(m.cfg.APN); apn != "" {
		cmd := fmt.Sprintf(`AT+CGDCONT=1,"IP","%s"`, escapeATString(apn))
		if _, err := m.runSIMCOMAT(ctx, cmd, m.cfg.Timeouts.Dial); err != nil {
			m.handleDialFailure(err, generation)
			return fmt.Errorf("set SIMCOM PDP context: %w", err)
		}
	}
	if _, err := m.runSIMCOMAT(ctx, "AT$QCRMCALL=1,1", m.cfg.Timeouts.Dial); err != nil {
		m.handleDialFailure(err, generation)
		return fmt.Errorf("start SIMCOM NDIS call: %w", err)
	}
	if err := m.runSIMCOMDHCP(ctx, ifname); err != nil {
		m.handleDialFailure(err, generation)
		return fmt.Errorf("acquire SIMCOM NDIS DHCP lease: %w", err)
	}

	ip, err := netcfg.GetCurrentIP(ifname)
	if err != nil {
		m.log.WithError(err).Warn("Failed to read SIMCOM DHCP IPv4 address")
	}
	if ip == nil {
		err := fmt.Errorf("SIMCOM NDIS DHCP completed without an IPv4 address")
		m.handleDialFailure(err, generation)
		return err
	}
	settings := &qmi.RuntimeSettings{IPv4Address: ip, IPv4Subnet: net.CIDRMask(32, 32)}

	if err := m.commitConnected(ctx, generation, settings, simcomNDISHandle, true); err != nil {
		return err
	}
	m.log.Infof("SIMCOM NDIS data call established, IPv4=%s", ip)
	return nil
}

func (m *Manager) doSIMCOMNDISDisconnect(ctx context.Context) {
	if _, err := m.runSIMCOMAT(ctx, "AT$QCRMCALL=0,1", m.cfg.Timeouts.Stop); err != nil {
		m.log.WithError(err).Warn("SIMCOM NDIS hangup failed")
	}
	ifname := strings.TrimSpace(m.cfg.Device.NetInterface)
	if ifname != "" {
		_ = netcfg.FlushAddresses(ifname)
		_ = netcfg.FlushRoutes(ifname)
		_ = netcfg.BringDown(ifname)
	}
	m.mu.Lock()
	m.handleV4 = 0
	m.handleV6 = 0
	m.settings = nil
	m.mu.Unlock()
}

func (m *Manager) doSIMCOMNDISStatusCheck(ctx context.Context, currentState State, generation uint64) {
	ifname := strings.TrimSpace(m.cfg.Device.NetInterface)
	if ifname == "" {
		return
	}
	ip, err := netcfg.GetCurrentIP(ifname)
	if err != nil {
		m.log.WithError(err).Debug("SIMCOM NDIS IP status query failed")
		return
	}
	if ip != nil {
		if currentState != StateConnected {
			settings := &qmi.RuntimeSettings{IPv4Address: ip, IPv4Subnet: net.CIDRMask(32, 32)}
			_ = m.commitConnected(ctx, generation, settings, simcomNDISHandle, true)
		}
		return
	}
	if currentState == StateConnected {
		m.log.Warn("SIMCOM NDIS connection lost")
		m.mu.Lock()
		runCtx := m.ctx
		if m.stopped ||
			m.state == StateStopping ||
			generation == 0 ||
			m.coreGeneration.Load() != generation ||
			runCtx == nil ||
			runCtx.Err() != nil {
			m.mu.Unlock()
			return
		}
		m.handleV4 = 0
		m.settings = nil
		m.state = StateDisconnected
		reconnect := m.cfg.AutoReconnect && m.desiredConnection
		if m.events != nil {
			m.events.Emit(Event{
				Type:       EventDisconnected,
				Generation: generation,
				State:      StateDisconnected,
			})
		}
		if reconnect {
			if m.events != nil {
				m.events.Emit(Event{
					Type:       EventReconnecting,
					Generation: generation,
					State:      StateDisconnected,
				})
			}
		}
		m.mu.Unlock()
		if reconnect {
			_ = m.enqueueReconnectEvent(generation)
		}
	}
}

func runATCommand(ctx context.Context, portName string, command string, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	mode := &serial.Mode{BaudRate: 115200}
	port, err := serial.Open(portName, mode)
	if err != nil {
		return "", err
	}
	defer port.Close()

	if err := port.SetReadTimeout(200 * time.Millisecond); err != nil {
		return "", err
	}
	if _, err := port.Write([]byte(command + "\r")); err != nil {
		return "", err
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	buf := make([]byte, 256)
	var out bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			return out.String(), ctx.Err()
		case <-deadline.C:
			return out.String(), fmt.Errorf("timeout waiting for AT response to %s", command)
		default:
		}
		n, err := port.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
			text := out.String()
			if strings.Contains(text, "\r\nOK\r\n") || strings.HasSuffix(strings.TrimSpace(text), "OK") {
				return text, nil
			}
			if strings.Contains(text, "\r\nERROR\r\n") || strings.Contains(text, "+CME ERROR") || strings.Contains(text, "+CMS ERROR") {
				return text, fmt.Errorf("AT command failed: %s", compactATResponse(text))
			}
		}
		if err != nil && ctx.Err() != nil {
			return out.String(), ctx.Err()
		}
	}
}

func runDHCPClient(ctx context.Context, ifname string) error {
	if path, err := exec.LookPath("udhcpc"); err == nil {
		cmd := exec.CommandContext(ctx, path, "-i", ifname, "-q", "-n")
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("udhcpc failed: %w: %s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if path, err := exec.LookPath("dhclient"); err == nil {
		cmd := exec.CommandContext(ctx, path, "-1", ifname)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("dhclient failed: %w: %s", err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	return fmt.Errorf("no DHCP client found (tried udhcpc, dhclient)")
}

func escapeATString(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}

func compactATResponse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
