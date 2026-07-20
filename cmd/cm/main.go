package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zanescope/quectel-qmi-go/pkg/device"
	"github.com/zanescope/quectel-qmi-go/pkg/manager"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Connection options / 连接选项
	apn      = flag.String("s", "", "APN name [user password [auth]] / APN名称")
	username = flag.String("u", "", "Username for authentication / 认证用户名")
	password = flag.String("p", "", "Password for authentication / 认证密码")
	authType = flag.Int("a", 0, "Auth type: 0=none, 1=PAP, 2=CHAP, 3=PAP|CHAP / 认证类型")
	pincode  = flag.String("pin", "", "SIM PIN code / SIM卡PIN码")

	// Interface selection / 接口选择
	ifaceName = flag.String("i", "", "Network interface name (e.g., wwan0) / 网络接口名称")
	devPath   = flag.String("d", "", "Control device path (e.g., /dev/cdc-wdm0) / 控制设备路径")
	atPath    = flag.String("at", "", "AT port path for vendor-specific data modes (e.g., /dev/ttyUSB2) / 厂商专有拨号使用的 AT 口")

	// IP version / IP版本
	ipv4Only = flag.Bool("4", false, "IPv4 only / 仅IPv4")
	ipv6Only = flag.Bool("6", false, "IPv6 only / 仅IPv6")

	// Network configuration options / 网络配置选项
	setRoute = flag.Bool("set-route", false, "Add default route (disabled by default for debugging) / 添加默认路由 (默认禁用，用于调试)")
	setDNS   = flag.Bool("set-dns", false, "Configure DNS (disabled by default for debugging) / 配置DNS (默认禁用，用于调试)")

	// Debugging / 调试
	verbose = flag.Bool("v", false, "Verbose output / 详细输出")
	version = flag.Bool("version", false, "Print version and exit / 打印版本并退出")

	// 多路拨号 (QMAP)
	profileIndex = flag.Int("n", 0, "PDN Profile index for data call (default 0 = modem default) / 拨号使用的 PDN Profile 索引")
	muxID        = flag.Int("m", 0, "QMAP Mux ID, bind data call to qmimux<N> (0 = disabled) / QMAP Mux ID，将拨号绑定到虚拟网卡")
	dataMode     = flag.String("data-mode", "auto", "Data call mode: auto, qmi, simcom-ndis / 数据拨号模式")
)

const Version = "1.0.0"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Quectel-CM Go Edition v%s\n", Version)
		fmt.Fprintf(os.Stderr, "A pure Go implementation of the Quectel Connection Manager\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -s internet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -s myapn -u user -p pass -a 1\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i wwan0 -s internet -4\n", os.Args[0])
	}
	flag.Parse()

	if *version {
		fmt.Printf("quectel-qmi-go version %s\n", Version)
		os.Exit(0)
	}

	// Setup logger / 设置日志记录器
	config := zap.NewDevelopmentConfig() // Defaults to "console" encoding with colors
	config.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("[2006-01-02 15:04:05]")
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.DisableStacktrace = true // Keep it clean for normal usage
	// config.DisableCaller = true     // Hide caller (line number) for compactness

	if *verbose {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	zapLog, err := config.Build()
	if err != nil {
		panic(err)
	}
	defer zapLog.Sync()

	// Use SugaredLogger for convenience (printf style)
	log := zapLog.Sugar()

	log.Infof("Quectel-CM Go Edition v%s", Version)

	mode, err := parseDataCallMode(*dataMode)
	if err != nil {
		log.Fatal(err)
	}

	// Select modem / 选择modem
	var selectedModem manager.ModemDevice
	log.Info("Discovering modems...")
	modems, discoverErr := device.Discover()
	if *ifaceName != "" || *devPath != "" {
		// Match by specified interface or device / 根据指定的接口或设备匹配
		if discoverErr == nil {
			for _, m := range modems {
				if (*ifaceName != "" && m.NetInterface == *ifaceName) ||
					(*devPath != "" && m.ControlPath == *devPath) {
					selectedModem = m
					break
				}
			}
		}
		if selectedModem.ControlPath == "" {
			if *ifaceName == "" || *devPath == "" {
				if discoverErr != nil {
					log.Fatalf("Failed to discover modems and explicit -d/-i are incomplete: %v", discoverErr)
				}
				log.Fatal("Specified modem not found; provide both -d and -i to bypass discovery")
			}
			selectedModem = manager.ModemDevice{
				ControlPath:  *devPath,
				NetInterface: *ifaceName,
			}
			if mode == manager.DataCallModeSIMCOMNDIS {
				selectedModem.VendorID = manager.VendorSIMCOM
				selectedModem.ProductID = 0x9001
			}
		}
	} else {
		if discoverErr != nil {
			log.Fatal("Failed to discover modems: ", discoverErr)
		}
		// Use first modem / 使用第一个发现的modem
		selectedModem = modems[0]
	}
	if strings.TrimSpace(*atPath) != "" {
		selectedModem.ATPort = strings.TrimSpace(*atPath)
	}

	log.Infof("Using modem: %s", selectedModem)

	// Determine IP versions / 确定IP版本
	enableV4 := !*ipv6Only
	enableV6 := !*ipv4Only
	if *ipv6Only {
		enableV4 = false
		enableV6 = true
	}

	// Create manager config / 创建管理器配置
	cfg := manager.Config{
		Device:        selectedModem,
		APN:           *apn,
		Username:      *username,
		Password:      *password,
		AuthType:      uint8(*authType),
		EnableIPv4:    enableV4,
		EnableIPv6:    enableV6,
		PINCode:       *pincode,
		AutoReconnect: true,
		NoRoute:       !*setRoute,
		NoDNS:         !*setDNS,
		ProfileIndex:  uint8(*profileIndex),
		MuxID:         uint8(*muxID),
		ATPort:        *atPath,
		DataCallMode:  mode,
	}

	// Create and start manager / 创建并启动管理器
	// Wrap zap logger for manager
	mgrLogger := manager.NewZapLogger(zapLog)
	mgr := manager.New(cfg, mgrLogger)
	if err := mgr.Start(); err != nil {
		log.Fatal("Failed to start connection manager: ", err)
	}

	// Setup signal handlers / 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Status display ticker / 状态显示定时器
	statusTicker := time.NewTicker(30 * time.Second)
	defer statusTicker.Stop()

	log.Info("Connection manager started. Press Ctrl+C to stop.")

	// Main loop / 主循环
	for {
		select {
		case sig := <-sigCh:
			log.Infof("Received signal %v, shutting down...", sig)
			mgr.Stop()
			log.Info("Goodbye!")
			os.Exit(0)

		case <-statusTicker.C:
			state := mgr.State()
			settings := mgr.Settings()
			if state == manager.StateConnected && settings != nil {
				log.Infof("Status: %s | IP: %s", state, settings.IPv4Address)
			} else {
				log.Infof("Status: %s", state)
			}
		}
	}
}

func parseDataCallMode(s string) (manager.DataCallMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return manager.DataCallModeAuto, nil
	case string(manager.DataCallModeQMI):
		return manager.DataCallModeQMI, nil
	case string(manager.DataCallModeSIMCOMNDIS):
		return manager.DataCallModeSIMCOMNDIS, nil
	default:
		return "", fmt.Errorf("invalid -data-mode %q (expected auto, qmi, simcom-ndis)", s)
	}
}
