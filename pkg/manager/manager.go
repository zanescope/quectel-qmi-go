package manager

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/warthog618/sms"
	"github.com/warthog618/sms/encoding/tpdu"
	"github.com/warthog618/sms/encoding/ucs2"
	"github.com/zanescope/quectel-qmi-go/pkg/netcfg"
	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

// ============================================================================
// Connection State Machine / 连接状态机
// ============================================================================

type State int

const (
	StateDisconnected State = iota // Disconnected / 已断开
	StateConnecting                // Connecting / 连接中
	StateConnected                 // Connected / 已连接
	StateStopping                  // Stopping / 正在停止
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}

// ============================================================================
// Configuration / 配置
// ============================================================================

type DataPlanePolicy int

const (
	DataPlanePolicyEager DataPlanePolicy = iota
	DataPlanePolicyLazy
	DataPlanePolicyDisabled
)

type OpenPhase string

const (
	OpenPhaseBefore OpenPhase = "before"
	OpenPhaseAfter  OpenPhase = "after"
)

type OpenReason string

const (
	OpenReasonInitial  OpenReason = "initial"
	OpenReasonRecovery OpenReason = "recovery"
)

// OpenAttempt describes one real QMI transport open boundary. OpenGuard is
// called immediately before and after the low-level client is opened so an
// embedding application can enforce host-level ownership policy on every
// initial open, retry, and recovery reopen.
type OpenAttempt struct {
	Phase      OpenPhase
	Reason     OpenReason
	Attempt    int
	DevicePath string
	UseProxy   bool
}

type Config struct {
	Device          ModemDevice // Modem device info / Modem 设备信息
	APN             string      // APN (Access Point Name) / APN（接入点名称）
	Username        string      // Authentication username / 认证用户名
	Password        string      // Authentication password / 认证密码
	AuthType        uint8       // 0=none, 1=PAP, 2=CHAP, 3=PAP|CHAP / 认证类型
	EnableIPv4      bool        // Enable IPv4 / 启用 IPv4
	EnableIPv6      bool        // Enable IPv6 / 启用 IPv6
	PINCode         string      // SIM PIN code / SIM 卡 PIN 码
	AutoReconnect   bool        // Automatically reconnect on disconnect / 断开后自动重连
	NoRoute         bool        // Don't add default route (useful for debugging) / 不添加默认路由 (用于调试)
	NoDNS           bool        // Don't configure DNS (useful for debugging) / 不配置DNS (用于调试)
	DisableWMSInd   bool        // Disable WMS indications (Event Report) / 禁用 WMS 指示 (事件报告)
	DisableIMSAInd  bool        // Disable IMSA indications / 禁用 IMSA 指示
	DisableVOICEInd bool        // Disable VOICE indications / 禁用 VOICE 指示

	ProfileIndex uint8 // PDN Profile 索引 (对应 -n 参数, 默认 0 表示使用模组默认 Profile)
	MuxID        uint8 // QMAP Mux ID (对应 -m 参数, 默认 0 表示不启用多路复用)
	NoDial       bool  // Only open QMI services, don't perform WDS dialing / 仅打开 QMI 服务, 不进行 WDS 拨号
	ATPort       string
	DataCallMode DataCallMode

	DataPlanePolicy DataPlanePolicy // Data-plane service allocation policy / 数据面服务分配策略
	Timeouts        TimeoutConfig
	RetryPolicy     RetryPolicy
	HealthPolicy    HealthPolicy
	EventPolicy     EventPolicy
	RecoveryPolicy  RecoveryPolicy
	ClientOptions   qmi.ClientOptions
	// OpenGuard runs immediately before and after each low-level QMI transport
	// open. It runs inside the manager lifecycle serialization boundary and
	// therefore must return promptly, honor ctx cancellation, and must not call
	// lifecycle methods on the same Manager.
	OpenGuard func(context.Context, OpenAttempt) error
}

type TimeoutConfig struct {
	Init               time.Duration
	Dial               time.Duration
	SIMCheck           time.Duration
	StatusCheck        time.Duration
	Stop               time.Duration
	IndicationRegister time.Duration
}

type RetryPolicy struct {
	ReconnectDelays []time.Duration
	ReinitDelays    []time.Duration
	RadioResetAfter int
}

type HealthPolicy struct {
	FullCheckInterval     time.Duration
	IndicationDebounce    time.Duration
	IPConsistencyInterval time.Duration
}

type EventPolicy struct {
	CallbackQueueSize int
}

type RecoveryPolicy struct {
	DisableServiceTimeoutRecovery bool
	ServiceTimeoutThreshold       int
	ServiceTimeoutWindow          time.Duration
	ServiceRecoverCooldown        time.Duration
	MaxRecoverAttempts            int           // >0 时，核心恢复连续重试超过该次数即放弃并发终态事件；0 = 无限（默认，向后兼容）
	MaxRecoverElapsed             time.Duration // >0 时，自首次恢复失败起超过该时长即放弃并发终态事件；0 = 不启用（默认）
}

type ManagerStats struct {
	StatusChecks               uint64
	DebouncedChecks            uint64
	ReconnectScheduled         uint64
	StaleTimerIgnored          uint64
	ResetEvents                uint64
	ResetCoalesced             uint64
	CoreRecoveryRequests       uint64
	CoreRecoveryCoalesced      uint64
	CoreRecoverySuppressed     uint64
	CoreRecoverySuccess        uint64
	CoreRecoveryFailures       uint64
	RecoverAttempts            uint64
	RecoverSuccess             uint64
	RecoverBackoffMs           uint64
	ServiceTimeouts            uint64
	ServiceTimeoutRecoveries   uint64
	ListenerIndicationsDropped uint64
}

// ============================================================================
// Manager - Core connection manager / 核心连接管理器
// ============================================================================

type pendingDataCall struct {
	generation uint64
	wds        *qmi.WDSService
	family     uint8
	handle     uint32
}

type Manager struct {
	cfg Config
	log Logger

	// QMI services / QMI服务
	client *qmi.Client
	wds    *qmi.WDSService
	wdsV6  *qmi.WDSService // Separate WDS for IPv6 / 用于IPv6的独立WDS服务
	nas    *qmi.NASService
	dms    *qmi.DMSService
	uim    *qmi.UIMService
	wda    *qmi.WDAService
	wms    *qmi.WMSService // SMS
	ims    *qmi.IMSService
	imsa   *qmi.IMSAService
	imsp   *qmi.IMSPService
	voice  *qmi.VOICEService

	// Connection handles / 连接句柄
	handleV4         uint32
	handleV6         uint32
	pendingDataCalls []pendingDataCall

	// State
	mu                   sync.RWMutex
	stopOnce             sync.Once
	stopErr              error
	stopped              bool
	lifecycleMu          sync.Mutex
	indicationDispatchMu sync.RWMutex
	timerCallbackMu      sync.Mutex
	timerCallbackWG      sync.WaitGroup
	timerCallbacksPaused bool
	timerEpoch           uint64
	connectMu            sync.Mutex
	dataPlaneMu          sync.Mutex
	coreGeneration       atomic.Uint64
	rawIPConfigured      atomic.Bool
	state                State
	settings             *qmi.RuntimeSettings
	lifetimeActive       bool
	controlReady         bool
	controlReadyStage    string
	controlReadySince    time.Time
	coreReady            bool
	coreReadyStage       string
	coreReadyLastErr     string
	coreReadySince       time.Time
	corePhase            CorePhase
	coreStatusStage      string
	coreStatusReason     string
	coreStatusLastErr    string
	coreStatusTerminal   bool
	coreStatusHub        coreStatusHub
	desiredConnection    bool

	// Event handling
	// Event handling / 事件处理
	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
	eventCh               chan internalEventEnvelope
	listenerIndicationCh  chan listenerIndication
	listenerBinding       *listenerBinding
	listenerChanged       chan struct{}
	nextListenerBindingID uint64
	backgroundTaskMu      sync.Mutex
	backgroundTaskWG      sync.WaitGroup
	events                *EventEmitter // External event callbacks / 外部事件回调

	// Reconnection / 重连相关
	retryCount          int
	retryDelays         []time.Duration
	reinitDelays        []time.Duration
	isRotating          bool // Flag to suppress status checks during IP rotation / 标志位: IP轮换期间抑制状态检查
	reconnectPending    bool
	reconnectGeneration uint64
	recoverCount        int
	recoverFirstFailAt  time.Time // 本轮连续恢复失败的首次时间，用于 MaxRecoverElapsed 判据
	lastIPCheck         time.Time

	// Internal notification / 内部通知
	regNotify chan bool // For fast registration detection / 用于快速注册检测

	// 多路拨号 (QMAP) / Multi-PDN
	muxIface string // QMAP 绑定后的虚拟网卡名 (如 qmimux0)

	timerMu                  sync.Mutex
	scheduledTimers          map[*time.Timer]struct{}
	targetedCheckScheduled   bool
	postRegRefreshScheduled  bool
	postRegRefreshLastAt     time.Time
	modemResetMu             sync.Mutex
	modemResetRecovering     bool
	modemResetPending        bool
	modemResetEnqueued       bool
	modemResetRequest        recoveryRequest
	coreRecoveryEnqueued     bool
	coreRecoveryRequest      recoveryRequest
	currentRecoveryRequest   recoveryRequest
	modemResetEnqueuedAt     time.Time
	modemResetDedupWindow    time.Duration
	modemResetQuietWindow    time.Duration
	modemResetDeferred       bool
	recoveryGeneration       uint64
	coreRecoveryRetryPending bool
	coreRecoveryRetryRequest recoveryRequest
	uimRecoveryMu            sync.Mutex
	dmsRecoveryMu            sync.Mutex
	nasRecoveryMu            sync.Mutex
	wmsRecoveryMu            sync.Mutex
	wmsReplayMu              sync.Mutex
	voiceRecoveryMu          sync.Mutex
	uimLastRecoverSignal     time.Time
	uimRecoverCooldown       time.Duration
	wmsReplayInProgress      bool
	serviceTimeoutMu         sync.Mutex
	serviceTimeoutFailures   map[serviceTimeoutKey]serviceTimeoutWindow

	globalTimeoutMu       sync.Mutex
	globalTimeoutServices map[string]time.Time
	globalTimeoutStormAt  time.Time

	// SMS recovery state / 短信恢复状态
	lastKnownGoodRoutes        *qmi.WMSRouteConfig
	wmsTransportStatus         qmi.WMSTransportNetworkRegistration
	wmsTransportKnown          bool
	wmsTransportUnsupported    bool
	wmsTransportQueryError     string
	wmsLastTransportWarn       string
	wmsLastTransportWarnAt     time.Time
	wmsSMSCValue               string
	wmsSMSCAvailable           bool
	wmsSMSCKnown               bool
	wmsSMSCStale               bool
	wmsSMSCUpdatedAt           time.Time
	wmsSMSCLastCheckAt         time.Time
	wmsSMSCRefreshPending      bool
	wmsRoutesKnown             bool
	wmsLastNASRegistered       bool
	wmsLastNASRegisteredKnown  bool
	wmsReadinessRefreshPending bool
	wmsReadinessRefreshTicket  uint64

	// Test hooks / 测试注入点
	querySignalStrength               func(ctx context.Context) (*qmi.SignalStrength, error)
	queryServingSystem                func(ctx context.Context) (*qmi.ServingSystem, error)
	queryPacketServiceState           func(ctx context.Context) (qmi.ConnectionStatus, error)
	queryExistingPacketServiceState   func(ctx context.Context, wds *qmi.WDSService) (qmi.ConnectionStatus, error)
	stopExistingDataCall              func(ctx context.Context, wds *qmi.WDSService) error
	closeWDSService                   func(wds *qmi.WDSService) error
	closeWDSServiceWithContext        func(ctx context.Context, wds *qmi.WDSService) error
	startDataCallHook                 func(ctx context.Context, wds *qmi.WDSService, family uint8) (uint32, error)
	stopDataCallHook                  func(ctx context.Context, wds *qmi.WDSService, handle uint32) error
	getRotationRuntimeSettingsHook    func(ctx context.Context, wds *qmi.WDSService) (*qmi.RuntimeSettings, error)
	flushRotationAddressesHook        func(ifname string) error
	disconnectHostCleanupHook         func(ifname string) error
	flushRadioDataPlaneHook           func(ifname string) error
	configureNetworkHook              func() error
	radioPowerCommandHook             func(ctx context.Context, token coreSessionToken, on bool, op string) error
	stopWDSForCleanup                 func(ctx context.Context, wds *qmi.WDSService, handle uint32) error
	simcomATCommand                   func(ctx context.Context, port string, command string, timeout time.Duration) (string, error)
	simcomDHCP                        func(ctx context.Context, ifname string) error
	registerWMSEventReport            func(ctx context.Context) error
	registerWMSIndications            func(ctx context.Context, reportTransportNetworkRegistration bool) error
	registerNASIndications            func(ctx context.Context, cfg qmi.NASIndicationRegistration) error
	registerUIMIndications            func(ctx context.Context) (uint32, error)
	registerVOICEIndications          func(ctx context.Context, cfg qmi.VoiceIndicationRegistration) error
	queryWMSTransportState            func(ctx context.Context) (qmi.WMSTransportNetworkRegistration, error)
	queryWMSRoutes                    func(ctx context.Context) (*qmi.WMSRouteConfig, error)
	setWMSRoutes                      func(ctx context.Context, routes []qmi.WMSRoute, transferStatusReportToClient bool) error
	querySMSC                         func(ctx context.Context) (string, error)
	queryNASRegistered                func(ctx context.Context) (bool, error)
	afterFunc                         func(time.Duration, func()) *time.Timer
	ensureUIMServiceHook              func() (*qmi.UIMService, error)
	rebindUIMServiceHook              func(reason string) (*qmi.UIMService, error)
	ensureDMSServiceHook              func() (*qmi.DMSService, error)
	rebindDMSServiceHook              func(reason string) (*qmi.DMSService, error)
	ensureNASServiceHook              func() (*qmi.NASService, error)
	rebindNASServiceHook              func(reason string) (*qmi.NASService, error)
	ensureWMSServiceHook              func() (*qmi.WMSService, error)
	rebindWMSServiceHook              func(reason string) (*qmi.WMSService, error)
	ensureVOICEServiceHook            func() (*qmi.VOICEService, error)
	rebindVOICEServiceHook            func(reason string) (*qmi.VOICEService, error)
	openLogicalChannelHook            func(ctx context.Context, slot uint8, aid []byte) (byte, error)
	closeLogicalChannelHook           func(ctx context.Context, slot uint8, channel uint8) error
	sendAPDUHook                      func(ctx context.Context, slot uint8, channel uint8, command []byte) ([]byte, error)
	newWDSService                     func(ctx context.Context, client *qmi.Client) (*qmi.WDSService, error)
	newNASService                     func(ctx context.Context, client *qmi.Client) (*qmi.NASService, error)
	newDMSService                     func(ctx context.Context, client *qmi.Client) (*qmi.DMSService, error)
	newUIMService                     func(ctx context.Context, client *qmi.Client) (*qmi.UIMService, error)
	newWDAService                     func(ctx context.Context, client *qmi.Client) (*qmi.WDAService, error)
	newWMSService                     func(ctx context.Context, client *qmi.Client) (*qmi.WMSService, error)
	newVOICEService                   func(ctx context.Context, client *qmi.Client) (*qmi.VOICEService, error)
	enableRawIPHook                   func(ctx context.Context) error
	onWMSRebindReplayHook             func(reason string)
	openClientAndAllocateServicesHook func(context.Context) error
	openQMIClientHook                 func(context.Context, string, qmi.ClientOptions) (*qmi.Client, error)
	closeQMIClientHook                func(*qmi.Client) error
	checkSIMHook                      func() error
	checkSIMContextHook               func(context.Context) error
	getICCIDStrictHook                func(ctx context.Context) (string, error)
	getIMSIStrictHook                 func(ctx context.Context) (string, error)
	beforeRecoveryCommitHook          func()
	beforeConnectCommitHook           func()
	recoveryEventValidatedHook        func()
	internalEventValidatedHook        func(internalEvent, uint64)
	finishRecoveryLockedHook          func()
	scheduledTimerClaimedHook         func()
	scheduledTimerBeforeClaimHook     func()

	statusChecks               atomic.Uint64
	debouncedChecks            atomic.Uint64
	reconnectScheduled         atomic.Uint64
	staleTimerIgnored          atomic.Uint64
	resetEvents                atomic.Uint64
	resetCoalesced             atomic.Uint64
	coreRecoveryRequests       atomic.Uint64
	coreRecoveryCoalesced      atomic.Uint64
	coreRecoverySuppressed     atomic.Uint64
	coreRecoverySuccess        atomic.Uint64
	coreRecoveryFailures       atomic.Uint64
	recoverAttempts            atomic.Uint64
	recoverSuccess             atomic.Uint64
	recoverBackoffMs           atomic.Uint64
	serviceTimeouts            atomic.Uint64
	serviceTimeoutRecoveries   atomic.Uint64
	listenerIndicationsDropped atomic.Uint64

	// 设备状态快照（由 NAS Indication 事件驱动，供上层零 IPC 读取）
	snapshot DeviceSnapshot
}

// internalEvent represents an internal event for the manager's event loop. / internalEvent 表示管理器事件循环的内部事件。
type internalEvent int

const (
	eventStart                internalEvent = iota // Start connection / 开始连接
	eventStop                                      // Stop connection / 停止连接
	eventCheckFull                                 // Periodic full status check / 周期性完整状态检查
	eventCheckTargeted                             // Debounced targeted status check / 去抖后的定向状态检查
	eventPacketStatusChanged                       // Packet service status changed indication / 数据包服务状态改变指示
	eventServingSystemChanged                      // Serving system changed indication / 服务系统改变指示
	eventModemReset                                // Modem reset indication / Modem重置指示
	eventCoreRecovery                              // Software core recovery request / 软件 Core 恢复请求
)

// internalEventEnvelope binds queued work to the manager lifetime that
// produced it. Consumers must validate both fields before acting; a stale
// event must never be reinterpreted as work for a replacement QMI session.
type internalEventEnvelope struct {
	kind       internalEvent
	generation uint64
}

// coreSessionToken identifies one manager lifetime and its exact transport.
// Service-level tokens are intentionally layered separately.
type coreSessionToken struct {
	generation uint64
	client     *qmi.Client
	runCtx     context.Context
}

// coreSessionCurrentLocked requires m.mu for reading or writing.
func (m *Manager) coreSessionCurrentLocked(token coreSessionToken) bool {
	return token.generation != 0 &&
		token.client != nil &&
		token.runCtx != nil &&
		token.runCtx.Err() == nil &&
		!m.stopped &&
		m.lifetimeActive &&
		m.state != StateStopping &&
		m.coreReady &&
		m.coreGeneration.Load() == token.generation &&
		m.client == token.client &&
		m.ctx == token.runCtx
}

func (m *Manager) coreSessionCurrent(token coreSessionToken) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coreSessionCurrentLocked(token)
}

type internalEventEnqueueResult uint8

const (
	internalEventInactive internalEventEnqueueResult = iota
	internalEventEnqueued
	internalEventQueueFull
)

var (
	// ErrManagerStopped is returned after Stop establishes the manager's
	// terminal lifecycle state. A stopped Manager cannot be restarted.
	ErrManagerStopped = errors.New("manager has been stopped")

	errInternalEventQueueFull = errors.New("internal event queue is full")
)

var defaultRetryDelays = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	60 * time.Second,
}

var defaultReinitDelays = []time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	32 * time.Second,
}

var defaultTimeouts = TimeoutConfig{
	Init:               10 * time.Second,
	Dial:               30 * time.Second,
	SIMCheck:           10 * time.Second,
	StatusCheck:        5 * time.Second,
	Stop:               3 * time.Second,
	IndicationRegister: 5 * time.Second,
}

var defaultHealthPolicy = HealthPolicy{
	FullCheckInterval:     15 * time.Second,
	IndicationDebounce:    500 * time.Millisecond,
	IPConsistencyInterval: 60 * time.Second,
}

var defaultEventPolicy = EventPolicy{
	CallbackQueueSize: 128,
}

const defaultUIMRecoverCooldown = 30 * time.Second
const defaultServiceTimeoutThreshold = 5
const defaultServiceTimeoutWindow = 10 * time.Minute
const defaultModemResetDedupWindow = 1 * time.Second
const defaultModemResetQuietWindow = 1 * time.Second
const defaultPostRegRefreshDelay = 800 * time.Millisecond
const defaultPostRegRefreshTimeout = 4 * time.Second
const defaultPostRegRefreshCooldown = 2 * time.Second
const recoverBackoffBase = 500 * time.Millisecond
const recoverBackoffMax = 15 * time.Second
const recoverBackoffJitterRatio = 0.2

// fullCheckJitterRatio 是健康检查周期 (HealthPolicy.FullCheckInterval) 每次重新调度时
// 附加的随机抖动比例。多个 Manager 实例（如双模组共用同一个 qmi-proxy 时）若各自的
// 检查周期固定且启动时间相近，会长期保持锁相，导致周期性同时向 proxy 发起突发请求；
// 每次 tick 后用新的随机间隔重新调度，能持续打散这种对齐，而不只是一次性错开。
const fullCheckJitterRatio = 0.2

var (
	smsReadyWaitTimeout   = 8 * time.Second
	smsReadyPollInterval  = 500 * time.Millisecond
	smscRefreshCooldown   = 30 * time.Second
	transportWarnCooldown = 30 * time.Second
)

type wmsRefreshOptions struct {
	includeRoutes    bool
	includeTransport bool
	includeSMSC      bool
	forceSMSC        bool
	allowRouteReplay bool
	emitSummary      bool
	quiet            bool
	reason           string
}

func normalizeConfig(cfg Config) Config {
	if cfg.Timeouts.Init <= 0 {
		cfg.Timeouts.Init = defaultTimeouts.Init
	}
	if cfg.Timeouts.Dial <= 0 {
		cfg.Timeouts.Dial = defaultTimeouts.Dial
	}
	if cfg.Timeouts.SIMCheck <= 0 {
		cfg.Timeouts.SIMCheck = defaultTimeouts.SIMCheck
	}
	if cfg.Timeouts.StatusCheck <= 0 {
		cfg.Timeouts.StatusCheck = defaultTimeouts.StatusCheck
	}
	if cfg.Timeouts.Stop <= 0 {
		cfg.Timeouts.Stop = defaultTimeouts.Stop
	}
	if cfg.Timeouts.IndicationRegister <= 0 {
		cfg.Timeouts.IndicationRegister = defaultTimeouts.IndicationRegister
	}

	if len(cfg.RetryPolicy.ReconnectDelays) == 0 {
		cfg.RetryPolicy.ReconnectDelays = append([]time.Duration(nil), defaultRetryDelays...)
	}
	if len(cfg.RetryPolicy.ReinitDelays) == 0 {
		cfg.RetryPolicy.ReinitDelays = append([]time.Duration(nil), defaultReinitDelays...)
	}
	if cfg.RetryPolicy.RadioResetAfter <= 0 {
		cfg.RetryPolicy.RadioResetAfter = 3
	}

	if cfg.HealthPolicy.FullCheckInterval <= 0 {
		cfg.HealthPolicy.FullCheckInterval = defaultHealthPolicy.FullCheckInterval
	}
	if cfg.HealthPolicy.IndicationDebounce <= 0 {
		cfg.HealthPolicy.IndicationDebounce = defaultHealthPolicy.IndicationDebounce
	}
	if cfg.HealthPolicy.IPConsistencyInterval <= 0 {
		cfg.HealthPolicy.IPConsistencyInterval = defaultHealthPolicy.IPConsistencyInterval
	}

	if cfg.EventPolicy.CallbackQueueSize <= 0 {
		cfg.EventPolicy.CallbackQueueSize = defaultEventPolicy.CallbackQueueSize
	}
	if cfg.RecoveryPolicy.ServiceTimeoutThreshold <= 0 {
		cfg.RecoveryPolicy.ServiceTimeoutThreshold = defaultServiceTimeoutThreshold
	}
	if cfg.RecoveryPolicy.ServiceTimeoutWindow <= 0 {
		cfg.RecoveryPolicy.ServiceTimeoutWindow = defaultServiceTimeoutWindow
	}
	if cfg.RecoveryPolicy.ServiceRecoverCooldown <= 0 {
		cfg.RecoveryPolicy.ServiceRecoverCooldown = defaultUIMRecoverCooldown
	}
	defaultClientOpts := qmi.DefaultClientOptions()
	if cfg.ClientOptions.ReadDeadline <= 0 {
		cfg.ClientOptions.ReadDeadline = defaultClientOpts.ReadDeadline
	}
	if cfg.ClientOptions.DefaultRequestTimeout <= 0 {
		cfg.ClientOptions.DefaultRequestTimeout = defaultClientOpts.DefaultRequestTimeout
	}
	if cfg.ClientOptions.TxQueueSize <= 0 {
		cfg.ClientOptions.TxQueueSize = defaultClientOpts.TxQueueSize
	}
	if cfg.ClientOptions.IndicationQueueSize <= 0 {
		cfg.ClientOptions.IndicationQueueSize = defaultClientOpts.IndicationQueueSize
	}
	if cfg.ClientOptions.ProxyPath == "" {
		cfg.ClientOptions.ProxyPath = defaultClientOpts.ProxyPath
	}
	if cfg.ClientOptions.ProxyExecutable == "" {
		cfg.ClientOptions.ProxyExecutable = defaultClientOpts.ProxyExecutable
	}
	if cfg.ClientOptions.ProxyOpenTimeout <= 0 {
		cfg.ClientOptions.ProxyOpenTimeout = defaultClientOpts.ProxyOpenTimeout
	}
	if !cfg.ClientOptions.SyncOnOpen &&
		cfg.ClientOptions.ReadDeadline == defaultClientOpts.ReadDeadline &&
		cfg.ClientOptions.DefaultRequestTimeout == defaultClientOpts.DefaultRequestTimeout &&
		cfg.ClientOptions.TxQueueSize == defaultClientOpts.TxQueueSize &&
		cfg.ClientOptions.IndicationQueueSize == defaultClientOpts.IndicationQueueSize {
		cfg.ClientOptions.SyncOnOpen = defaultClientOpts.SyncOnOpen
	}
	return cfg
}

// New creates a new connection manager / New 创建新的连接管理器
// logger is optional, if nil a default logger will be used / logger 是可选的，如果为 nil 则使用默认日志器
func New(cfg Config, logger Logger) *Manager {
	cfg = normalizeConfig(cfg)
	if logger == nil {
		logger = NewNopLogger()
	}
	baseLog := logger.WithField("iface", cfg.Device.NetInterface)
	if cfg.ClientOptions.Logf == nil {
		clientLog := baseLog.WithField("component", "qmi_client")
		cfg.ClientOptions.Logf = func(level qmi.ClientLogLevel, format string, args ...any) {
			switch level {
			case qmi.ClientLogLevelWarn:
				clientLog.Warnf(format, args...)
			case qmi.ClientLogLevelError:
				clientLog.Errorf(format, args...)
			default:
				clientLog.Debugf(format, args...)
			}
		}
	}

	m := &Manager{
		cfg:                   cfg,
		log:                   baseLog,
		retryDelays:           append([]time.Duration(nil), cfg.RetryPolicy.ReconnectDelays...),
		reinitDelays:          append([]time.Duration(nil), cfg.RetryPolicy.ReinitDelays...),
		eventCh:               make(chan internalEventEnvelope, 16),
		listenerIndicationCh:  make(chan listenerIndication, listenerIndicationQueueSize),
		listenerChanged:       make(chan struct{}, 1),
		events:                NewEventEmitterWithQueueSize(cfg.EventPolicy.CallbackQueueSize),
		scheduledTimers:       make(map[*time.Timer]struct{}),
		modemResetDedupWindow: defaultModemResetDedupWindow,
		modemResetQuietWindow: defaultModemResetQuietWindow,
		uimRecoverCooldown:    cfg.RecoveryPolicy.ServiceRecoverCooldown,
		corePhase:             CorePhaseIdle,
	}
	m.mu.Lock()
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	return m
}

// Start initializes and starts the connection manager / Start 初始化并启动连接管理器
func (m *Manager) Start() error {
	if err := m.StartCoreContext(context.Background()); err != nil {
		return err
	}

	if !m.cfg.NoDial {
		return m.requestDataConnection()
	} else {
		m.log.Info("NoDial mode enabled, core started without initial connection")
	}

	return nil
}

func (m *Manager) requestDataConnection() error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrManagerStopped
	}
	runCtx := m.ctx
	if !m.coreReady || m.state == StateStopping || runCtx == nil || runCtx.Err() != nil {
		m.mu.Unlock()
		if runCtx != nil && runCtx.Err() != nil {
			return runCtx.Err()
		}
		return context.Canceled
	}

	m.desiredConnection = true
	generation := m.coreGeneration.Load()
	m.mu.Unlock()
	switch m.enqueueReconnectEvent(generation) {
	case internalEventEnqueued:
		return nil
	case internalEventQueueFull:
		return nil
	default:
		m.mu.RLock()
		stopped := m.stopped
		currentCtx := m.ctx
		m.mu.RUnlock()
		if stopped {
			return ErrManagerStopped
		}
		if currentCtx != nil && currentCtx.Err() != nil {
			return currentCtx.Err()
		}
		return context.Canceled
	}
}

// StartCore initializes the QMI core services without starting a data call.
func (m *Manager) StartCore() error {
	return m.StartCoreContext(context.Background())
}

// startCoreAdmissionErrorLocked requires m.mu.
func (m *Manager) startCoreAdmissionErrorLocked() error {
	if m.stopped {
		return ErrManagerStopped
	}
	if m.lifetimeActive || m.coreReady || m.state != StateDisconnected {
		return fmt.Errorf("manager already started")
	}
	return nil
}

// StartCoreContext initializes the QMI core services without starting a data call.
func (m *Manager) StartCoreContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	// Reject an already active manager without disturbing its timers.
	m.mu.Lock()
	if err := m.startCoreAdmissionErrorLocked(); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	// Generation ownership cannot advance while a callback from the prior
	// epoch is still executing. The helper returns with the timer gate retained
	// so no new callback can register between the drain and the generation bump.
	m.pauseAndDrainScheduledTimers()
	m.mu.Lock()
	if err := m.startCoreAdmissionErrorLocked(); err != nil {
		m.mu.Unlock()
		m.resumeScheduledTimersLocked()
		return err
	}
	m.state = StateConnecting
	m.desiredConnection = false
	m.reconnectPending = false
	m.reconnectGeneration = 0
	runCtx, runCancel := context.WithCancel(context.Background())
	m.ctx = runCtx
	m.cancel = runCancel
	m.lifetimeActive = true
	generation := m.coreGeneration.Add(1)
	m.setCorePhaseLocked(CorePhaseStarting, "start_begin", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.resumeScheduledTimersLocked()

	operationCtx, operationCancel := context.WithCancel(ctx)
	stopCallerCancellation := context.AfterFunc(runCtx, operationCancel)
	defer func() {
		stopCallerCancellation()
		operationCancel()
	}()

	cleanupFailedStart := func(stage string, cause error) {
		runCancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), m.cfg.Timeouts.Stop)
		defer cleanupCancel()
		m.cleanupLocked(cleanupCtx)
		m.mu.Lock()
		if m.coreGeneration.Load() == generation && !m.stopped {
			m.lifetimeActive = false
			m.state = StateDisconnected
			m.setCorePhaseLocked(CorePhaseDegraded, stage, "start_failed", cause)
			m.publishCoreStatusLocked()
		}
		m.mu.Unlock()
	}

	openErr := error(nil)
	if m.openClientAndAllocateServicesHook != nil {
		openErr = m.openClientAndAllocateServicesHook(operationCtx)
	} else {
		openErr = m.openClientAndAllocateServices(operationCtx, OpenReasonInitial)
	}
	if openErr != nil {
		cleanupFailedStart("start_open_failed", openErr)
		return openErr
	}

	m.mu.RLock()
	stopping := m.state == StateStopping
	m.mu.RUnlock()
	if err := operationCtx.Err(); err != nil || stopping || m.coreGeneration.Load() != generation {
		failureErr := err
		if failureErr == nil {
			failureErr = context.Canceled
		}
		cleanupFailedStart("start_canceled", failureErr)
		if err != nil {
			return err
		}
		return context.Canceled
	}

	m.mu.Lock()
	m.markControlReadyLocked("start_control_ready")
	m.setCorePhaseLocked(CorePhaseStarting, "start_control_ready", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()

	// Check SIM status / 检查SIM卡状态
	if err := m.checkSIMContext(operationCtx); err != nil {
		m.log.WithError(err).Warn("SIM check failed")
		// Continue anyway - might work / 继续尝试，也许能工作
	}

	// Start event loop / 启动事件循环
	m.stopScheduledTimers()

	// Publish the running loops, readiness, and terminal startup state under the
	// same lock Stop uses to enter StateStopping. This is the lifecycle
	// linearization point: either startup commits first and Stop observes the
	// WaitGroup entries, or Stop commits first and startup aborts without
	// resurrecting readiness.
	m.mu.Lock()
	startErr := operationCtx.Err()
	startFailureStage := "start_commit_canceled"
	if startErr == nil && (m.stopped || m.state == StateStopping || m.coreGeneration.Load() != generation) {
		startErr = context.Canceled
	}
	if startErr == nil && m.currentListenerTerminalLocked(generation) {
		terminalErr := m.listenerBinding.err()
		if terminalErr == nil {
			terminalErr = errQMIClientEventStreamClosed
		}
		startErr = fmt.Errorf("QMI transport terminated before startup commit: %w", terminalErr)
		startFailureStage = "start_transport_terminal"
	}
	if startErr != nil {
		m.mu.Unlock()
		cleanupFailedStart(startFailureStage, startErr)
		return startErr
	}

	m.wg.Add(3)
	go m.eventLoop(runCtx)
	go m.indicationHandler(runCtx)
	go m.listenerIndicationHandler(runCtx)

	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
	}
	m.markCoreReadyLocked("start_core_ready")
	m.setCorePhaseLocked(CorePhaseReady, "start_core_ready", "", nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.log.Info("QMI core started")

	// 在底层 QMI 就绪后立即在后台异步全量加载设备标识。
	m.PreWarmIdentities(true)

	return nil
}

// Stop gracefully stops the connection manager / Stop 优雅停止连接管理器
func (m *Manager) Stop() error {
	if m == nil {
		return nil
	}
	m.stopOnce.Do(func() {
		m.stopErr = m.stopOnceBody()
	})
	return m.stopErr
}

func (m *Manager) stopOnceBody() error {
	m.indicationDispatchMu.Lock()
	m.mu.Lock()
	m.modemResetMu.Lock()
	m.retireListenerBindingLocked(nil)
	m.stopped = true
	m.lifetimeActive = false
	m.desiredConnection = false
	m.reconnectPending = false
	m.reconnectGeneration = 0
	if m.state != StateStopping {
		m.state = StateStopping
	}
	m.clearRecoveryStateLocked()
	m.setCorePhaseLocked(CorePhaseStopping, "stop_begin", "", nil)
	m.publishCoreStatusLocked()
	m.modemResetMu.Unlock()
	cancel := m.cancel
	m.mu.Unlock()
	m.indicationDispatchMu.Unlock()

	m.log.Info("Stopping connection manager...")
	if cancel != nil {
		cancel()
	}
	m.stopScheduledTimers()

	// Wait for loops to finish / 等待循环结束
	m.wg.Wait()

	// Join admitted background work before lifecycle cleanup. Admission is
	// already closed by stopped=true; holding the gate linearizes Add and Wait.
	m.backgroundTaskMu.Lock()
	m.backgroundTaskMu.Unlock()
	m.backgroundTaskWG.Wait()

	// Event-loop work has exited. Now join any external lifecycle operation
	// (Connect/RotateIP/recovery/startup) before closing shared QMI objects.
	// We intentionally do not hold lifecycleMu during wg.Wait: an event already
	// dequeued may itself need that lock in order to observe cancellation.
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	// All producers for the old lifetime are now stopped. Remove queued work
	// before releasing resources.
	m.drainInternalEvents()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), m.cfg.Timeouts.Stop)
	m.cleanupLocked(cleanupCtx)
	cleanupCancel()
	if m.events != nil {
		m.events.Close()
	}
	m.finishStopState()
	m.log.Info("Connection manager stopped")
	return nil
}

// Connect establishes a data call on top of an already-started QMI core.
func (m *Manager) Connect() error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrManagerStopped
	}
	if !m.coreReady {
		m.mu.Unlock()
		return fmt.Errorf("manager core not started")
	}
	dataReady := (m.cfg.EnableIPv4 && m.handleV4 != 0) || (!m.cfg.EnableIPv4 && m.cfg.EnableIPv6 && m.handleV6 != 0)
	if m.state == StateConnected && dataReady {
		m.desiredConnection = true
		m.mu.Unlock()
		return nil
	}
	if m.state == StateConnecting && dataReady {
		m.desiredConnection = true
		m.mu.Unlock()
		return fmt.Errorf("connection already in progress")
	}
	m.desiredConnection = true
	m.mu.Unlock()

	return m.doConnect()
}

// Disconnect tears down the current data call but keeps the QMI core available.
func (m *Manager) Disconnect() error {
	m.mu.Lock()
	if !m.coreReady {
		m.mu.Unlock()
		return fmt.Errorf("manager core not started")
	}
	if m.state == StateStopping {
		m.mu.Unlock()
		return fmt.Errorf("manager is stopping")
	}
	m.desiredConnection = false
	m.reconnectPending = false
	m.reconnectGeneration = 0
	hasData := m.handleV4 != 0 ||
		m.handleV6 != 0 ||
		len(m.pendingDataCalls) != 0 ||
		m.isRotating ||
		m.state == StateConnected ||
		m.state == StateConnecting
	m.mu.Unlock()

	if !hasData {
		return nil
	}

	return m.doDisconnect()
}

func (m *Manager) ResetExistingDataConnection(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.RLock()
	coreReady := m.coreReady
	wds := m.wds
	client := m.client
	runCtx := m.ctx
	generation := m.coreGeneration.Load()
	stopping := m.state == StateStopping
	m.mu.RUnlock()

	if !coreReady || client == nil {
		return false, ErrServiceNotReady("qmi-core")
	}
	if stopping {
		return false, context.Canceled
	}

	operationCtx, operationCancel := contextWithLifetime(ctx, runCtx)
	defer operationCancel()
	if err := operationCtx.Err(); err != nil {
		return false, err
	}

	validateSession := func() error {
		if err := operationCtx.Err(); err != nil {
			return err
		}
		m.mu.RLock()
		defer m.mu.RUnlock()
		if m.state == StateStopping {
			return context.Canceled
		}
		if !m.coreReady ||
			m.client != client ||
			m.ctx != runCtx ||
			m.coreGeneration.Load() != generation {
			return ErrServiceNotReady("qmi-core")
		}
		return nil
	}

	temporaryWDS := false
	if wds == nil {
		var err error
		if m.newWDSService != nil {
			wds, err = m.newWDSService(operationCtx, client)
		} else {
			wds, err = qmi.NewWDSServiceWithContext(operationCtx, client)
		}
		if err != nil {
			return false, fmt.Errorf("allocate WDS client for existing data cleanup: %w", err)
		}
		temporaryWDS = true
	}
	if temporaryWDS {
		defer func() {
			releaseCtx, releaseCancel := contextWithMaxTimeout(runCtx, m.cfg.Timeouts.Stop)
			defer releaseCancel()
			if err := m.closeTemporaryWDSService(releaseCtx, wds); err != nil &&
				!errors.Is(err, context.Canceled) {
				m.log.WithError(err).Warn("Failed to release temporary WDS client after existing data cleanup")
			}
		}()
	}
	if err := validateSession(); err != nil {
		return false, err
	}

	status, err := m.queryExistingPacketServiceStatus(operationCtx, wds)
	if err != nil {
		return false, fmt.Errorf("query existing packet service status: %w", err)
	}
	if err := validateSession(); err != nil {
		return false, err
	}
	if !packetServiceStatusHasDataCall(status) {
		return false, nil
	}

	if err := m.stopExistingPacketDataCall(operationCtx, wds); err != nil {
		return false, fmt.Errorf("stop existing qmi data call: %w", err)
	}
	if err := validateSession(); err != nil {
		return false, err
	}

	status, err = m.queryExistingPacketServiceStatus(operationCtx, wds)
	if err != nil {
		return false, fmt.Errorf("verify existing packet service status after stop: %w", err)
	}
	if err := validateSession(); err != nil {
		return false, err
	}
	if packetServiceStatusHasDataCall(status) {
		return false, fmt.Errorf("existing qmi data call still connected after stop: %s", status.String())
	}

	m.mu.Lock()
	if err := operationCtx.Err(); err != nil {
		m.mu.Unlock()
		return false, err
	}
	if m.state == StateStopping {
		m.mu.Unlock()
		return false, context.Canceled
	}
	if !m.coreReady ||
		m.client != client ||
		m.ctx != runCtx ||
		m.coreGeneration.Load() != generation {
		m.mu.Unlock()
		return false, ErrServiceNotReady("qmi-core")
	}
	m.handleV4 = 0
	m.handleV6 = 0
	m.settings = nil
	m.desiredConnection = false
	m.reconnectPending = false
	m.reconnectGeneration = 0
	if m.state == StateConnected || m.state == StateConnecting {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
		m.publishCoreStatusLocked()
	}
	m.mu.Unlock()
	return true, nil
}

// State returns the current connection state / State 返回当前的连接状态
func (m *Manager) State() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// IsCoreReady reports whether the QMI core services are initialized.
func (m *Manager) IsCoreReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coreReady
}

// IsControlReady reports whether QMI client/services are initialized.
// It can be true while SIM identity is still converging.
func (m *Manager) IsControlReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.controlReady
}

func (m *Manager) markControlReadyLocked(stage string) {
	now := time.Now()
	m.controlReady = true
	if stage == "" {
		stage = "control_ready"
	}
	m.controlReadyStage = stage
	m.controlReadySince = now
}

func (m *Manager) markControlNotReadyLocked(stage string) {
	now := time.Now()
	m.controlReady = false
	if stage != "" {
		m.controlReadyStage = stage
	}
	if m.controlReadyStage == "" {
		m.controlReadyStage = "control_not_ready"
	}
	if m.controlReadySince.IsZero() || stage != "" {
		m.controlReadySince = now
	}
}

func (m *Manager) markCoreReadyLocked(stage string) {
	now := time.Now()
	m.markControlReadyLocked(stage)
	m.coreReady = true
	if stage == "" {
		stage = "ready"
	}
	m.coreReadyStage = stage
	m.coreReadyLastErr = ""
	m.coreReadySince = now
}

func (m *Manager) markCoreNotReadyLocked(stage string, err error) {
	now := time.Now()
	m.coreReady = false
	if stage != "" {
		m.coreReadyStage = stage
	}
	if m.coreReadyStage == "" {
		m.coreReadyStage = "not_ready"
	}
	if err != nil {
		m.coreReadyLastErr = err.Error()
	}
	if m.coreReadySince.IsZero() || stage != "" {
		m.coreReadySince = now
	}
}

// WaitCoreReady 阻塞等待直到 QMI core 服务恢复就绪（coreReady == true）。
// 用于 modem reset 后的调用方门控，避免在 QMI 服务恢复窗口期内发起请求。
// 如果 core 已就绪，立即返回 nil。
func (m *Manager) WaitCoreReady(ctx context.Context) error {
	start := time.Now()
	m.mu.RLock()
	if m.coreReady {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.mu.RLock()
			stage := m.coreReadyStage
			lastErr := m.coreReadyLastErr
			since := m.coreReadySince
			m.mu.RUnlock()
			if stage == "" {
				stage = "unknown"
			}
			stageFor := time.Duration(0)
			if !since.IsZero() {
				stageFor = time.Since(since)
			}
			return fmt.Errorf(
				"等待 QMI core 收敛就绪超时 (waited=%s stage=%s stage_for=%s last_err=%s): %w",
				time.Since(start).Round(time.Millisecond),
				stage,
				stageFor.Round(time.Millisecond),
				strings.TrimSpace(lastErr),
				ctx.Err(),
			)
		case <-ticker.C:
			m.mu.RLock()
			ready := m.coreReady
			m.mu.RUnlock()
			if ready {
				return nil
			}
		}
	}
}

// WaitControlReady blocks until QMI client/services are initialized.
// It intentionally does not wait for SIM identity readability.
func (m *Manager) WaitControlReady(ctx context.Context) error {
	start := time.Now()
	m.mu.RLock()
	if m.controlReady {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.mu.RLock()
			stage := m.controlReadyStage
			since := m.controlReadySince
			m.mu.RUnlock()
			if stage == "" {
				stage = "unknown"
			}
			stageFor := time.Duration(0)
			if !since.IsZero() {
				stageFor = time.Since(since)
			}
			return fmt.Errorf(
				"等待 QMI control 就绪超时 (waited=%s stage=%s stage_for=%s): %w",
				time.Since(start).Round(time.Millisecond),
				stage,
				stageFor.Round(time.Millisecond),
				ctx.Err(),
			)
		case <-ticker.C:
			m.mu.RLock()
			ready := m.controlReady
			m.mu.RUnlock()
			if ready {
				return nil
			}
		}
	}
}

// WaitIdentityReady first waits for the QMI core, then proves that at least one
// live SIM identity (ICCID or IMSI) is readable. Core readiness alone is not an
// identity guarantee because recovery may remain usable after identity probes
// fail.
func (m *Manager) WaitIdentityReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.WaitCoreReady(ctx); err != nil {
		return fmt.Errorf("等待 QMI identity 收敛就绪失败: %w", err)
	}
	if err := m.waitIdentityReadable(ctx); err != nil {
		return fmt.Errorf("wait for readable QMI identity: %w", err)
	}
	return nil
}

// IsConnected reports whether the data plane is currently connected.
func (m *Manager) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state == StateConnected
}

// Settings returns the current IP settings / Settings 返回当前的 IP 设置
func (m *Manager) Settings() *qmi.RuntimeSettings {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.settings
}

func (m *Manager) Stats() ManagerStats {
	if m == nil {
		return ManagerStats{}
	}
	return ManagerStats{
		StatusChecks:               m.statusChecks.Load(),
		DebouncedChecks:            m.debouncedChecks.Load(),
		ReconnectScheduled:         m.reconnectScheduled.Load(),
		StaleTimerIgnored:          m.staleTimerIgnored.Load(),
		ResetEvents:                m.resetEvents.Load(),
		ResetCoalesced:             m.resetCoalesced.Load(),
		CoreRecoveryRequests:       m.coreRecoveryRequests.Load(),
		CoreRecoveryCoalesced:      m.coreRecoveryCoalesced.Load(),
		CoreRecoverySuppressed:     m.coreRecoverySuppressed.Load(),
		CoreRecoverySuccess:        m.coreRecoverySuccess.Load(),
		CoreRecoveryFailures:       m.coreRecoveryFailures.Load(),
		RecoverAttempts:            m.recoverAttempts.Load(),
		RecoverSuccess:             m.recoverSuccess.Load(),
		RecoverBackoffMs:           m.recoverBackoffMs.Load(),
		ServiceTimeouts:            m.serviceTimeouts.Load(),
		ServiceTimeoutRecoveries:   m.serviceTimeoutRecoveries.Load(),
		ListenerIndicationsDropped: m.listenerIndicationsDropped.Load(),
	}
}

func (m *Manager) ClientStats() qmi.ClientStats {
	if m == nil {
		return qmi.ClientStats{}
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return qmi.ClientStats{}
	}
	return client.Stats()
}

func (m *Manager) emitEvent(event Event) {
	if m == nil || m.events == nil {
		return
	}
	m.events.Emit(event)
}

func (m *Manager) emitEventForGeneration(event Event, generation uint64) bool {
	if m == nil || m.events == nil || generation == 0 {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	runCtx := m.ctx
	if m.stopped ||
		m.state == StateStopping ||
		m.coreGeneration.Load() != generation ||
		runCtx == nil ||
		runCtx.Err() != nil {
		return false
	}
	event.Generation = generation
	return m.events.Emit(event)
}

func (m *Manager) qmiIndicationEvent(eventType EventType, evt qmi.Event) Event {
	return Event{
		Type:       eventType,
		State:      m.State(),
		RawQMIType: evt.Type,
		ServiceID:  evt.ServiceID,
		MessageID:  evt.MessageID,
	}
}

func (m *Manager) emitQMIIndicationEvent(eventType EventType, evt qmi.Event) {
	m.emitEvent(m.qmiIndicationEvent(eventType, evt))
}

func (m *Manager) emitSignalUpdate(sig *qmi.SignalStrength) {
	if sig == nil {
		return
	}
	// 原地更新快照，供上层零 IPC 读取信号强度
	m.snapshot.updateSignal(sig)
	m.emitEvent(Event{
		Type:   EventSignalUpdate,
		State:  m.State(),
		Signal: sig,
	})
}

func packetTLVMeta(packet *qmi.Packet) []qmi.TLVMeta {
	if packet == nil || len(packet.TLVs) == 0 {
		return nil
	}
	meta := make([]qmi.TLVMeta, 0, len(packet.TLVs))
	for _, tlv := range packet.TLVs {
		meta = append(meta, qmi.TLVMeta{
			Type:   tlv.Type,
			Length: len(tlv.Value),
		})
	}
	return meta
}

func isUnsupportedOptionalWMSIndicationError(err error) bool {
	if err == nil {
		return false
	}
	var qmiErr *qmi.QMIError
	if !errors.As(err, &qmiErr) || qmiErr == nil {
		return false
	}
	// QMIErrOpDeviceUnsupported (0x0034) 是 EC20 对 WMSGetTransportNetworkRegistrationStatus
	// (msg 0x004A) 的常见回应，含义等同于「设备不支持该操作」
	return qmiErr.ErrorCode == qmi.QMIErrInvalidQmiCmd ||
		qmiErr.ErrorCode == qmi.QMIErrNotSupported ||
		qmiErr.ErrorCode == qmi.QMIErrOpDeviceUnsupported
}

func (m *Manager) getSignalStrength(ctx context.Context) (*qmi.SignalStrength, error) {
	if m.querySignalStrength != nil {
		return m.querySignalStrength(ctx)
	}
	return withNASRecoveryValue(m, "getSignalStrength", func(nas *qmi.NASService) (*qmi.SignalStrength, error) {
		return nas.GetSignalStrength(ctx)
	})
}

func (m *Manager) getServingSystem(ctx context.Context) (*qmi.ServingSystem, error) {
	if m.queryServingSystem != nil {
		return m.queryServingSystem(ctx)
	}
	return withNASRecoveryValue(m, "getServingSystem", func(nas *qmi.NASService) (*qmi.ServingSystem, error) {
		return nas.GetServingSystem(ctx)
	})
}

func (m *Manager) packetServiceStatusTarget() (*qmi.WDSService, uint8) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cfg.EnableIPv4 {
		return m.wds, qmi.IpFamilyV4
	}
	if m.cfg.EnableIPv6 {
		return m.wdsV6, qmi.IpFamilyV6
	}
	// Preserve the historical IPv4 default for zero-value/test configs.
	return m.wds, qmi.IpFamilyV4
}

func (m *Manager) getPacketServiceState(ctx context.Context) (qmi.ConnectionStatus, uint8, error) {
	wds, family := m.packetServiceStatusTarget()
	if m.queryPacketServiceState != nil {
		status, err := m.queryPacketServiceState(ctx)
		return status, family, err
	}
	if wds == nil {
		return qmi.StatusUnknown, family, fmt.Errorf("wds service not available for IP family %d", family)
	}
	status, err := wds.GetPacketServiceStatus(ctx)
	return status, family, err
}

func packetServiceStatusHasDataCall(status qmi.ConnectionStatus) bool {
	switch status {
	case qmi.StatusConnected, qmi.StatusSuspended, qmi.StatusAuthenticating:
		return true
	default:
		return false
	}
}

func (m *Manager) queryExistingPacketServiceStatus(ctx context.Context, wds *qmi.WDSService) (qmi.ConnectionStatus, error) {
	if m.queryExistingPacketServiceState != nil {
		return m.queryExistingPacketServiceState(ctx, wds)
	}
	if wds == nil {
		return qmi.StatusUnknown, fmt.Errorf("wds service not available")
	}
	return wds.GetPacketServiceStatus(ctx)
}

func (m *Manager) stopExistingPacketDataCall(ctx context.Context, wds *qmi.WDSService) error {
	if m.stopExistingDataCall != nil {
		return m.stopExistingDataCall(ctx, wds)
	}
	if wds == nil {
		return fmt.Errorf("wds service not available")
	}
	if err := wds.StopAnyNetworkInterface(ctx, true); err != nil {
		if isExistingDataStopNoopError(err) {
			return nil
		}
		return err
	}
	return nil
}

func isExistingDataStopNoopError(err error) bool {
	var qmiErr *qmi.QMIError
	if !errors.As(err, &qmiErr) || qmiErr == nil {
		return false
	}
	return qmiErr.ErrorCode == qmi.QMIErrOutOfCall || qmiErr.ErrorCode == qmi.QMIErrNoEffect
}

func (m *Manager) closeTemporaryWDSService(ctx context.Context, wds *qmi.WDSService) error {
	if m.closeWDSServiceWithContext != nil {
		return m.closeWDSServiceWithContext(ctx, wds)
	}
	if m.closeWDSService != nil {
		return m.closeWDSService(wds)
	}
	if wds == nil {
		return nil
	}
	return wds.CloseWithContext(ctx)
}

func (m *Manager) opContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	base := context.Background()
	m.mu.RLock()
	if m.ctx != nil {
		base = m.ctx
	}
	m.mu.RUnlock()

	if timeout <= 0 {
		return context.WithCancel(base)
	}
	return context.WithTimeout(base, timeout)
}

// launchCoreBackgroundTask admits asynchronous work into one exact core
// session. The worker acquires lifecycle ownership before touching services;
// a recovery that wins the lock first makes the captured token stale.
func (m *Manager) launchCoreBackgroundTask(fn func(context.Context, coreSessionToken)) bool {
	if m == nil || fn == nil {
		return false
	}
	m.backgroundTaskMu.Lock()
	m.mu.RLock()
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	active := m.coreSessionCurrentLocked(token)
	m.mu.RUnlock()
	if !active {
		m.backgroundTaskMu.Unlock()
		return false
	}
	taskCtx, taskCancel := context.WithCancel(token.runCtx)
	m.backgroundTaskWG.Add(1)
	go func() {
		defer m.backgroundTaskWG.Done()
		defer taskCancel()

		m.lifecycleMu.Lock()
		defer m.lifecycleMu.Unlock()
		if taskCtx.Err() != nil || !m.coreSessionCurrent(token) {
			return
		}
		fn(taskCtx, token)
	}()
	m.backgroundTaskMu.Unlock()
	return true
}

func maxDuration(a, b time.Duration) time.Duration {
	if a >= b {
		return a
	}
	return b
}

func (m *Manager) createWDSService(ctx context.Context) (*qmi.WDSService, error) {
	if m.newWDSService != nil {
		return m.newWDSService(ctx, m.client)
	}
	return qmi.NewWDSServiceWithContext(ctx, m.client)
}

func (m *Manager) createNASService(ctx context.Context) (*qmi.NASService, error) {
	if m.newNASService != nil {
		return m.newNASService(ctx, m.client)
	}
	return qmi.NewNASServiceWithContext(ctx, m.client)
}

func (m *Manager) createDMSService(ctx context.Context) (*qmi.DMSService, error) {
	if m.newDMSService != nil {
		return m.newDMSService(ctx, m.client)
	}
	return qmi.NewDMSServiceWithContext(ctx, m.client)
}

func (m *Manager) createUIMService(ctx context.Context) (*qmi.UIMService, error) {
	if m.newUIMService != nil {
		return m.newUIMService(ctx, m.client)
	}
	return qmi.NewUIMServiceWithContext(ctx, m.client)
}

func (m *Manager) createWDAService(ctx context.Context) (*qmi.WDAService, error) {
	if m.newWDAService != nil {
		return m.newWDAService(ctx, m.client)
	}
	return qmi.NewWDAServiceWithContext(ctx, m.client)
}

func (m *Manager) createWMSService(ctx context.Context) (*qmi.WMSService, error) {
	if m.newWMSService != nil {
		return m.newWMSService(ctx, m.client)
	}
	return qmi.NewWMSServiceWithContext(ctx, m.client)
}

func (m *Manager) createVOICEService(ctx context.Context) (*qmi.VOICEService, error) {
	if m.newVOICEService != nil {
		return m.newVOICEService(ctx, m.client)
	}
	return qmi.NewVOICEServiceWithContext(ctx, m.client)
}

func (m *Manager) shouldAllocateWDA() bool {
	return strings.TrimSpace(m.cfg.Device.NetInterface) != "" && (m.cfg.EnableIPv4 || m.cfg.EnableIPv6)
}

func (m *Manager) shouldAllocateDataPlaneAtStart() bool {
	if m.useSIMCOMNDIS() {
		return false
	}
	return m.cfg.DataPlanePolicy == DataPlanePolicyEager
}

func (m *Manager) dataPlaneDisabled() bool {
	return m.cfg.DataPlanePolicy == DataPlanePolicyDisabled
}

func (m *Manager) ensureDataPlaneServices(ctx context.Context) error {
	m.dataPlaneMu.Lock()
	defer m.dataPlaneMu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if m.dataPlaneDisabled() {
		return ErrServiceNotReady("data-plane")
	}

	if m.cfg.EnableIPv4 && m.wds == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.log.Debug("Allocating WDS client for IPv4...")
		wds, err := m.createWDSService(ctx)
		if err != nil {
			return fmt.Errorf("failed to allocate WDS client: %w", err)
		}
		m.wds = wds
		m.log.Debug("Allocated WDS client for IPv4")
	}

	if m.cfg.EnableIPv6 && m.wdsV6 == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.log.Debug("Allocating WDS client for IPv6...")
		wdsV6, err := m.createWDSService(ctx)
		if err != nil {
			return fmt.Errorf("failed to allocate IPv6 WDS client: %w", err)
		}
		m.wdsV6 = wdsV6
		m.log.Debug("Allocated WDS client for IPv6")
	}

	if m.shouldAllocateWDA() {
		if m.wda == nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			m.log.Debug("Allocating WDA client...")
			wda, err := m.createWDAService(ctx)
			if err != nil {
				return fmt.Errorf("failed to allocate WDA client: %w", err)
			}
			m.wda = wda
			m.rawIPConfigured.Store(false)
			m.log.Debug("Allocated WDA client")
		}

		if !m.rawIPConfigured.Load() {
			if err := m.enableRawIP(ctx); err != nil {
				return fmt.Errorf("failed to enable RawIP mode: %w", err)
			}
			m.rawIPConfigured.Store(true)
		}
	} else {
		m.log.Debug("Skipping WDA client allocation")
	}

	return nil
}

func contextWithMaxTimeout(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 {
		return context.WithCancel(ctx)
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining <= limit {
			return context.WithCancel(ctx)
		}
	}
	return context.WithTimeout(ctx, limit)
}

func contextWithLifetime(ctx context.Context, lifetime context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	operationCtx, cancel := context.WithCancel(ctx)
	if lifetime == nil {
		return operationCtx, cancel
	}

	stopLifetimeCancellation := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return operationCtx, func() {
		stopLifetimeCancellation()
		cancel()
	}
}

func copyWMSRouteConfig(cfg *qmi.WMSRouteConfig) *qmi.WMSRouteConfig {
	if cfg == nil {
		return nil
	}
	dup := &qmi.WMSRouteConfig{
		TransferStatusReportToClient: cfg.TransferStatusReportToClient,
		HasTransferStatusReport:      cfg.HasTransferStatusReport,
	}
	if len(cfg.Routes) > 0 {
		dup.Routes = append([]qmi.WMSRoute(nil), cfg.Routes...)
	}
	return dup
}

func isUsableWMSRouteConfig(cfg *qmi.WMSRouteConfig) bool {
	return cfg != nil && len(cfg.Routes) > 0
}

func boolPtr(v bool) *bool {
	out := v
	return &out
}

func (m *Manager) setWMSTransportState(status qmi.WMSTransportNetworkRegistration, known, unsupported bool) {
	m.mu.Lock()
	m.wmsTransportStatus = status
	m.wmsTransportKnown = known
	m.wmsTransportUnsupported = unsupported
	m.mu.Unlock()
}

func (m *Manager) setWMSTransportQueryError(err error) {
	m.mu.Lock()
	if err == nil {
		m.wmsTransportQueryError = ""
	} else {
		m.wmsTransportQueryError = err.Error()
	}
	m.mu.Unlock()
}

func (m *Manager) setWMSSMSCState(value string, known, available, stale bool, updatedAt, checkedAt time.Time) {
	m.mu.Lock()
	m.wmsSMSCValue = value
	m.wmsSMSCKnown = known
	m.wmsSMSCAvailable = available
	m.wmsSMSCStale = stale
	m.wmsSMSCUpdatedAt = updatedAt
	if checkedAt.IsZero() {
		checkedAt = updatedAt
	}
	m.wmsSMSCLastCheckAt = checkedAt
	m.mu.Unlock()
}

func (m *Manager) beginWMSSMSCRefresh() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wmsSMSCRefreshPending {
		return false
	}
	m.wmsSMSCRefreshPending = true
	return true
}

func (m *Manager) endWMSSMSCRefresh() {
	m.mu.Lock()
	m.wmsSMSCRefreshPending = false
	m.mu.Unlock()
}

func (m *Manager) setWMSRoutesKnown(known bool) {
	m.mu.Lock()
	m.wmsRoutesKnown = known
	m.mu.Unlock()
}

func (m *Manager) cacheWMSRoutes(cfg *qmi.WMSRouteConfig) {
	m.mu.Lock()
	m.lastKnownGoodRoutes = copyWMSRouteConfig(cfg)
	m.wmsRoutesKnown = isUsableWMSRouteConfig(cfg)
	m.mu.Unlock()
}

func (m *Manager) setWMSLastNASRegistered(registered *bool) {
	m.mu.Lock()
	if registered == nil {
		m.wmsLastNASRegisteredKnown = false
		m.wmsLastNASRegistered = false
	} else {
		m.wmsLastNASRegisteredKnown = true
		m.wmsLastNASRegistered = *registered
	}
	m.mu.Unlock()
}

func (m *Manager) markWMSReadinessStale() {
	m.mu.Lock()
	m.wmsTransportStatus = 0
	m.wmsTransportKnown = false
	m.wmsTransportUnsupported = false
	m.wmsTransportQueryError = ""
	m.wmsLastTransportWarn = ""
	m.wmsLastTransportWarnAt = time.Time{}
	m.wmsLastNASRegisteredKnown = false
	m.wmsLastNASRegistered = false
	if m.wmsSMSCKnown {
		m.wmsSMSCStale = true
	}
	m.mu.Unlock()
}

func (m *Manager) cachedWMSRoutes() *qmi.WMSRouteConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return copyWMSRouteConfig(m.lastKnownGoodRoutes)
}

func (m *Manager) wmsReadinessSnapshot() (status qmi.WMSTransportNetworkRegistration, known, unsupported, smscAvailable, routesKnown bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.wmsTransportStatus, m.wmsTransportKnown, m.wmsTransportUnsupported, m.wmsSMSCAvailable, m.wmsRoutesKnown
}

func (m *Manager) wmsSMSCSnapshot() (value string, available, known, stale bool, updatedAt, lastCheckAt time.Time, pending bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.wmsSMSCValue, m.wmsSMSCAvailable, m.wmsSMSCKnown, m.wmsSMSCStale, m.wmsSMSCUpdatedAt, m.wmsSMSCLastCheckAt, m.wmsSMSCRefreshPending
}

func (m *Manager) wmsDiagnosticSnapshot() (status qmi.WMSTransportNetworkRegistration, known, unsupported bool, transportQueryError string, smscValue string, smscAvailable, smscKnown, smscStale bool, smscUpdatedAt time.Time, routesKnown bool, nasRegistered *bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.wmsLastNASRegisteredKnown {
		nasRegistered = boolPtr(m.wmsLastNASRegistered)
	}
	return m.wmsTransportStatus, m.wmsTransportKnown, m.wmsTransportUnsupported, m.wmsTransportQueryError, m.wmsSMSCValue, m.wmsSMSCAvailable, m.wmsSMSCKnown, m.wmsSMSCStale, m.wmsSMSCUpdatedAt, m.wmsRoutesKnown, nasRegistered
}

func (m *Manager) wmsTransportStatusString() string {
	status, known, unsupported, _, _, _, _, _, _, _, _ := m.wmsDiagnosticSnapshot()
	if unsupported {
		return "unsupported"
	}
	if !known {
		return "unknown"
	}
	return status.String()
}

func (m *Manager) shouldWarnTransportError(err error) bool {
	if err == nil {
		return false
	}

	now := time.Now()
	message := err.Error()

	m.mu.Lock()
	defer m.mu.Unlock()
	if message != "" && message == m.wmsLastTransportWarn && !m.wmsLastTransportWarnAt.IsZero() && now.Sub(m.wmsLastTransportWarnAt) < transportWarnCooldown {
		return false
	}
	m.wmsLastTransportWarn = message
	m.wmsLastTransportWarnAt = now
	return true
}

func (m *Manager) hasWMSReadyPath() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.wms != nil || m.registerWMSEventReport != nil || m.registerWMSIndications != nil || m.queryWMSTransportState != nil || m.queryWMSRoutes != nil
}

func (m *Manager) registerWMSEventReportWithContext(ctx context.Context) error {
	if m.registerWMSEventReport != nil {
		return m.registerWMSEventReport(ctx)
	}
	return m.withWMSRecovery("registerWMSEventReportWithContext", func(wms *qmi.WMSService) error {
		return wms.RegisterEventReport(ctx)
	})
}

func (m *Manager) registerWMSIndicationsWithContext(ctx context.Context, reportTransportNetworkRegistration bool) error {
	if m.registerWMSIndications != nil {
		return m.registerWMSIndications(ctx, reportTransportNetworkRegistration)
	}
	return m.withWMSRecovery("registerWMSIndicationsWithContext", func(wms *qmi.WMSService) error {
		return wms.IndicationRegister(ctx, reportTransportNetworkRegistration)
	})
}

func (m *Manager) registerNASIndicationsWithContext(ctx context.Context, cfg qmi.NASIndicationRegistration) error {
	if m.registerNASIndications != nil {
		return m.registerNASIndications(ctx, cfg)
	}
	return m.withNASRecovery("registerNASIndicationsWithContext", func(nas *qmi.NASService) error {
		return nas.RegisterIndicationsWithConfig(ctx, cfg)
	})
}

func (m *Manager) uimIndicationRegistrationMask() uint32 {
	return qmi.UIMEventRegistrationCardStatus |
		qmi.UIMEventRegistrationExtendedCardStatus |
		qmi.UIMEventRegistrationPhysicalSlotStatus
}

func (m *Manager) registerUIMIndicationsWithContext(ctx context.Context, uim *qmi.UIMService) (uint32, error) {
	if m.registerUIMIndications != nil {
		return m.registerUIMIndications(ctx)
	}
	if uim == nil {
		return 0, ErrServiceNotReady("UIM")
	}
	return uim.RegisterEvents(ctx, m.uimIndicationRegistrationMask())
}

func (m *Manager) registerVOICEIndicationsWithContext(ctx context.Context, cfg qmi.VoiceIndicationRegistration) error {
	if m.registerVOICEIndications != nil {
		return m.registerVOICEIndications(ctx, cfg)
	}
	return m.withVOICERecovery("registerVOICEIndicationsWithContext", func(voice *qmi.VOICEService) error {
		return voice.IndicationRegister(ctx, cfg)
	})
}

func (m *Manager) queryWMSTransportStateWithContext(ctx context.Context) (qmi.WMSTransportNetworkRegistration, error) {
	if m.queryWMSTransportState != nil {
		return m.queryWMSTransportState(ctx)
	}
	return m.WMSGetTransportNetworkRegistrationStatus(ctx)
}

func (m *Manager) queryWMSRoutesWithContext(ctx context.Context) (*qmi.WMSRouteConfig, error) {
	if m.queryWMSRoutes != nil {
		return m.queryWMSRoutes(ctx)
	}
	return m.WMSGetRoutes(ctx)
}

func (m *Manager) setWMSRoutesWithContext(ctx context.Context, routes []qmi.WMSRoute, transferStatusReportToClient bool) error {
	if m.setWMSRoutes != nil {
		return m.setWMSRoutes(ctx, routes, transferStatusReportToClient)
	}
	return m.WMSSetRoutes(ctx, routes, transferStatusReportToClient)
}

func (m *Manager) querySMSCWithContext(ctx context.Context) (string, error) {
	if m.querySMSC != nil {
		return m.querySMSC(ctx)
	}
	return m.querySMSCFromDevice(ctx)
}

func (m *Manager) queryNASRegisteredWithContext(ctx context.Context) (bool, error) {
	if m.queryNASRegistered != nil {
		return m.queryNASRegistered(ctx)
	}
	return withNASRecoveryValue(m, "queryNASRegisteredWithContext", func(nas *qmi.NASService) (bool, error) {
		return nas.IsRegistered(ctx)
	})
}

func (m *Manager) replayCachedWMSRoutes(ctx context.Context) {
	cfg := m.cachedWMSRoutes()
	if !isUsableWMSRouteConfig(cfg) {
		m.setWMSRoutesKnown(false)
		m.log.Debug("WMS routes unavailable and no cached routes to replay")
		return
	}

	if err := m.setWMSRoutesWithContext(ctx, cfg.Routes, cfg.TransferStatusReportToClient); err != nil {
		m.setWMSRoutesKnown(false)
		m.log.WithError(err).Warn("Failed to replay cached WMS routes")
		return
	}

	m.setWMSRoutesKnown(true)
	m.log.WithField("route_count", len(cfg.Routes)).Info("WMS routes replayed from cache")
}

func (m *Manager) refreshWMSRouteState(ctx context.Context, allowRouteReplay bool, quiet bool) {
	cfg, err := m.queryWMSRoutesWithContext(ctx)
	if err == nil && isUsableWMSRouteConfig(cfg) {
		m.cacheWMSRoutes(cfg)
		if !quiet {
			m.log.WithField("route_count", len(cfg.Routes)).Debug("WMS routes cached")
		}
		return
	}

	m.setWMSRoutesKnown(false)
	if err != nil {
		if !quiet {
			m.log.WithError(err).Warn("Failed to query WMS routes")
		}
	} else if !quiet {
		m.log.Debug("WMS routes unavailable or empty")
	}
	if allowRouteReplay {
		m.replayCachedWMSRoutes(ctx)
	}
}

func (m *Manager) refreshWMSTransportState(ctx context.Context, quiet bool) {
	status, err := m.queryWMSTransportStateWithContext(ctx)
	switch {
	case err == nil:
		m.setWMSTransportState(status, true, false)
		m.setWMSTransportQueryError(nil)
	case isUnsupportedOptionalWMSIndicationError(err):
		m.setWMSTransportState(0, false, true)
		m.setWMSTransportQueryError(err)
		if !quiet {
			m.log.WithError(err).Debug("WMS transport registration query not supported by modem")
		}
	default:
		m.setWMSTransportState(0, false, false)
		m.setWMSTransportQueryError(err)
		if !quiet {
			entry := m.log.WithError(err)
			if m.shouldWarnTransportError(err) {
				entry.Warn("Failed to query WMS transport registration status")
			} else {
				entry.Debug("Failed to query WMS transport registration status")
			}
		}
	}
}

func (m *Manager) refreshWMSSMSCState(ctx context.Context, force bool, quiet bool) {
	_, _, _, _, _, cachedLastCheckAt, pending := m.wmsSMSCSnapshot()
	if pending {
		return
	}
	if !force && !cachedLastCheckAt.IsZero() && time.Since(cachedLastCheckAt) < smscRefreshCooldown {
		return
	}
	if !m.beginWMSSMSCRefresh() {
		return
	}
	defer m.endWMSSMSCRefresh()

	checkedAt := time.Now()
	smsc, err := m.querySMSCWithContext(ctx)
	if err != nil {
		cachedValue, cachedAvailable, cachedKnown, _, cachedUpdatedAt, _, _ := m.wmsSMSCSnapshot()
		if cachedKnown {
			m.setWMSSMSCState(cachedValue, true, cachedAvailable, true, cachedUpdatedAt, checkedAt)
			if !quiet && force {
				m.log.WithError(err).Debug("WMS SMSC refresh failed, keeping cached SMSC")
			}
			return
		}
		m.setWMSSMSCState("", false, false, false, time.Time{}, checkedAt)
		if !quiet {
			m.log.WithError(err).Debug("WMS SMSC unavailable")
		}
		return
	}

	trimmed := strings.TrimSpace(smsc)
	known := trimmed != ""
	m.setWMSSMSCState(trimmed, known, known, false, checkedAt, checkedAt)
	if !known && !quiet {
		m.log.Debug("WMS SMSC unavailable: empty result")
	}
}

func (m *Manager) logWMSRecoveryState(message string) {
	_, known, unsupported, transportQueryError, _, smscAvailable, smscKnown, smscStale, smscUpdatedAt, routesKnown, nasRegistered := m.wmsDiagnosticSnapshot()
	entry := m.log.
		WithField("transport_status", m.wmsTransportStatusString()).
		WithField("transport_known", known).
		WithField("transport_unsupported", unsupported).
		WithField("smsc_available", smscAvailable).
		WithField("smsc_known", smscKnown).
		WithField("smsc_stale", smscStale).
		WithField("routes_known", routesKnown)
	if transportQueryError != "" {
		entry = entry.WithField("transport_query_error", transportQueryError)
	}
	if !smscUpdatedAt.IsZero() {
		entry = entry.WithField("smsc_last_updated_at", smscUpdatedAt)
	}
	if nasRegistered != nil {
		entry = entry.WithField("nas_registered", *nasRegistered)
	}
	entry.Debug(message)
}

func (m *Manager) refreshWMSState(ctx context.Context, opts wmsRefreshOptions) {
	if opts.includeRoutes {
		m.refreshWMSRouteState(ctx, opts.allowRouteReplay, opts.quiet)
	}
	if opts.includeTransport {
		m.refreshWMSTransportState(ctx, opts.quiet)
	}
	if opts.includeSMSC {
		m.refreshWMSSMSCState(ctx, opts.forceSMSC, opts.quiet)
	}

	if opts.emitSummary {
		_, known, unsupported, smscAvailable, routesKnown := m.wmsReadinessSnapshot()
		switch {
		case routesKnown && smscAvailable && ((known && m.wmsTransportStatusString() == qmi.WMSTransportNetworkRegistrationFullService.String()) || unsupported):
			m.logWMSRecoveryState("WMS recovery state ready")
		default:
			m.logWMSRecoveryState("WMS recovery state degraded")
		}
	}
}

func (m *Manager) recoverWMSState() {
	m.recoverWMSStateWithContext(context.Background())
}

func (m *Manager) recoverWMSStateWithContext(parent context.Context) {
	if !m.hasWMSReadyPath() {
		return
	}

	if !m.cfg.DisableWMSInd {
		ctx, cancel := contextWithMaxTimeout(parent, m.cfg.Timeouts.IndicationRegister)
		if err := m.registerWMSEventReportWithContext(ctx); err != nil {
			m.log.WithError(err).Warn("Failed to register SMS indications")
		}
		cancel()

		ctx, cancel = contextWithMaxTimeout(parent, m.cfg.Timeouts.IndicationRegister)
		if err := m.registerWMSIndicationsWithContext(ctx, true); err != nil {
			if isUnsupportedOptionalWMSIndicationError(err) {
				m.log.WithError(err).Debug("WMS transport registration indications not supported by modem")
			} else {
				m.log.WithError(err).Warn("Failed to register WMS transport registration indications")
			}
		}
		cancel()
	} else {
		m.log.Debug("WMS indications disabled by config")
	}

	ctx, cancel := contextWithMaxTimeout(parent, maxDuration(m.cfg.Timeouts.IndicationRegister, m.cfg.Timeouts.StatusCheck))
	defer cancel()
	m.refreshWMSState(ctx, wmsRefreshOptions{
		includeRoutes:    true,
		includeTransport: true,
		includeSMSC:      true,
		forceSMSC:        true,
		allowRouteReplay: true,
		emitSummary:      true,
		reason:           "recover",
	})
}

func (m *Manager) maybeRefreshWMSReadiness(reason string) {
	if !m.hasWMSReadyPath() {
		return
	}

	m.mu.Lock()
	if m.wmsReadinessRefreshPending {
		m.mu.Unlock()
		return
	}
	m.wmsReadinessRefreshTicket++
	ticket := m.wmsReadinessRefreshTicket
	m.wmsReadinessRefreshPending = true
	m.mu.Unlock()

	launched := m.launchCoreBackgroundTask(func(lifetimeCtx context.Context, token coreSessionToken) {
		defer func() {
			m.mu.Lock()
			if m.wmsReadinessRefreshTicket == ticket {
				m.wmsReadinessRefreshPending = false
			}
			m.mu.Unlock()
		}()

		ctx, cancel := contextWithMaxTimeout(lifetimeCtx, m.cfg.Timeouts.StatusCheck)
		defer cancel()
		m.refreshWMSState(ctx, wmsRefreshOptions{
			includeRoutes:    true,
			includeTransport: true,
			includeSMSC:      false,
			allowRouteReplay: false,
			emitSummary:      true,
			reason:           reason,
		})
		if m.coreSessionCurrent(token) {
			m.log.WithField("reason", reason).Debug("WMS readiness refresh completed")
		}
	})
	if !launched {
		m.mu.Lock()
		if m.wmsReadinessRefreshTicket == ticket {
			m.wmsReadinessRefreshPending = false
		}
		m.mu.Unlock()
	}
}

func (m *Manager) smsReadyWithContext(ctx context.Context) (bool, bool, error) {
	if !m.hasWMSReadyPath() {
		return false, false, ErrServiceNotReady("WMS")
	}

	_, _, smscKnown, _, _, _, _ := m.wmsSMSCSnapshot()
	m.refreshWMSState(ctx, wmsRefreshOptions{
		includeRoutes:    true,
		includeTransport: true,
		includeSMSC:      !smscKnown,
		allowRouteReplay: false,
		emitSummary:      false,
		quiet:            true,
		reason:           "sms-ready-check",
	})

	status, known, unsupported, smscAvailable, routesKnown := m.wmsReadinessSnapshot()
	transportStatus := m.wmsTransportStatusString()
	if !smscAvailable {
		m.setWMSLastNASRegistered(nil)
		return false, false, nil
	}
	if known && transportStatus == qmi.WMSTransportNetworkRegistrationFullService.String() {
		m.setWMSLastNASRegistered(nil)
		return true, false, nil
	}
	if known {
		switch status {
		case qmi.WMSTransportNetworkRegistrationNoService,
			qmi.WMSTransportNetworkRegistrationInProcess,
			qmi.WMSTransportNetworkRegistrationFailure,
			qmi.WMSTransportNetworkRegistrationLimitedService:
			m.setWMSLastNASRegistered(nil)
			return false, false, nil
		}
	}
	if unsupported || transportStatus == "unknown" {
		if !routesKnown {
			m.setWMSLastNASRegistered(nil)
			return false, false, nil
		}
		registered, err := m.queryNASRegisteredWithContext(ctx)
		if err != nil {
			m.setWMSLastNASRegistered(nil)
			return false, false, err
		}
		m.setWMSLastNASRegistered(boolPtr(registered))
		return registered, true, nil
	}
	m.setWMSLastNASRegistered(nil)
	return false, false, nil
}

// EnsureSMSReady performs a minimal compatibility check for SMS sending.
func (m *Manager) EnsureSMSReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !m.hasWMSReadyPath() {
		return ErrServiceNotReady("WMS")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

func (m *Manager) currentSMSReadinessDetails() SMSNotReadyError {
	status, known, unsupported, transportQueryError, _, smscAvailable, _, _, _, routesKnown, nasRegistered := m.wmsDiagnosticSnapshot()
	transportStatus := "unknown"
	switch {
	case unsupported:
		transportStatus = "unsupported"
	case known:
		transportStatus = status.String()
	}
	return SMSNotReadyError{
		TransportStatus:      transportStatus,
		TransportKnown:       known,
		TransportUnsupported: unsupported,
		TransportQueryError:  transportQueryError,
		SMSCAvailable:        smscAvailable,
		RoutesKnown:          routesKnown,
		NASRegistered:        nasRegistered,
	}
}

func (m *Manager) currentSMSNotReadyError() *SMSNotReadyError {
	details := m.currentSMSReadinessDetails()
	return &details
}

func (m *Manager) scheduleAfter(delay time.Duration, fn func()) {
	if m == nil || fn == nil {
		return
	}

	// timerCallbackMu is a registration gate, not an execution lock. It makes
	// timer registration atomic with stopScheduledTimers while still allowing a
	// callback to schedule another timer without recursively locking a mutex.
	m.timerCallbackMu.Lock()
	defer m.timerCallbackMu.Unlock()
	epoch := m.timerEpoch
	m.mu.RLock()
	generation := m.coreGeneration.Load()
	scheduledCtx := m.ctx
	active := !m.timerCallbacksPaused &&
		!m.stopped &&
		m.state != StateStopping &&
		generation != 0 &&
		scheduledCtx != nil &&
		scheduledCtx.Err() == nil
	m.mu.RUnlock()
	if !active {
		return
	}

	var timer *time.Timer
	wrapped := func() {
		if m.scheduledTimerBeforeClaimHook != nil {
			m.scheduledTimerBeforeClaimHook()
		}
		m.timerCallbackMu.Lock()
		m.timerMu.Lock()
		if timer != nil {
			delete(m.scheduledTimers, timer)
		}
		m.timerMu.Unlock()

		m.mu.RLock()
		currentCtx := m.ctx
		inactive := m.timerCallbacksPaused || m.stopped || m.state == StateStopping
		currentGeneration := m.coreGeneration.Load()
		m.mu.RUnlock()
		if epoch != m.timerEpoch ||
			inactive ||
			currentCtx != scheduledCtx ||
			scheduledCtx.Err() != nil ||
			generation != currentGeneration {
			m.staleTimerIgnored.Add(1)
			m.timerCallbackMu.Unlock()
			return
		}
		// Add and stopScheduledTimers' Wait are linearized by the same gate.
		m.timerCallbackWG.Add(1)
		m.timerCallbackMu.Unlock()
		defer m.timerCallbackWG.Done()
		if m.scheduledTimerClaimedHook != nil {
			m.scheduledTimerClaimedHook()
		}
		fn()
	}
	if m.afterFunc != nil {
		timer = m.afterFunc(delay, wrapped)
	} else {
		timer = time.AfterFunc(delay, wrapped)
	}

	if timer != nil {
		m.timerMu.Lock()
		if m.scheduledTimers == nil {
			m.scheduledTimers = make(map[*time.Timer]struct{})
		}
		m.scheduledTimers[timer] = struct{}{}
		m.timerMu.Unlock()
	}
}

// pauseAndDrainScheduledTimers retires every unclaimed callback, waits for
// callbacks that already claimed the current epoch, and returns with the
// registration gate locked and callbacks paused. Generation transitions use
// that retained lock to keep the drain and generation bump atomic.
func (m *Manager) pauseAndDrainScheduledTimers() {
	if m == nil {
		return
	}
	m.timerCallbackMu.Lock()
	m.timerEpoch++
	m.timerCallbacksPaused = true
	m.timerMu.Lock()
	for timer := range m.scheduledTimers {
		timer.Stop()
		delete(m.scheduledTimers, timer)
	}
	m.targetedCheckScheduled = false
	m.postRegRefreshScheduled = false
	m.timerMu.Unlock()
	m.timerCallbackMu.Unlock()

	// No callback can Add after paused=true was committed under the gate.
	// A wrapper that fired before the pause is rejected by timerEpoch when it
	// eventually acquires the gate.
	m.timerCallbackWG.Wait()
	m.timerCallbackMu.Lock()
}

// resumeScheduledTimersLocked releases the registration gate retained by
// pauseAndDrainScheduledTimers. A terminal Stop leaves callbacks paused.
func (m *Manager) resumeScheduledTimersLocked() {
	m.mu.RLock()
	runCtx := m.ctx
	active := !m.stopped &&
		m.state != StateStopping &&
		runCtx != nil &&
		runCtx.Err() == nil
	m.mu.RUnlock()
	if active {
		m.timerCallbacksPaused = false
	}
	m.timerCallbackMu.Unlock()
}

func (m *Manager) stopScheduledTimers() {
	if m == nil {
		return
	}
	m.pauseAndDrainScheduledTimers()
	m.resumeScheduledTimersLocked()
}

// enqueueInternalEventLocked is the single producer boundary for eventCh.
// The caller must hold m.mu for reading or writing so validation and enqueue
// are linearized with Stop.
func (m *Manager) enqueueInternalEventLocked(event internalEvent, generation uint64) internalEventEnqueueResult {
	runCtx := m.ctx
	if generation == 0 ||
		m.stopped ||
		m.state == StateStopping ||
		m.coreGeneration.Load() != generation ||
		runCtx == nil ||
		runCtx.Err() != nil {
		return internalEventInactive
	}
	if m.internalEventValidatedHook != nil {
		m.internalEventValidatedHook(event, generation)
	}
	select {
	case m.eventCh <- internalEventEnvelope{kind: event, generation: generation}:
		return internalEventEnqueued
	default:
		return internalEventQueueFull
	}
}

func (m *Manager) enqueueInternalEvent(event internalEvent, generation uint64) internalEventEnqueueResult {
	if m == nil {
		return internalEventInactive
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enqueueInternalEventLocked(event, generation)
}

func (m *Manager) enqueueCurrentInternalEvent(event internalEvent) internalEventEnqueueResult {
	if m == nil {
		return internalEventInactive
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enqueueInternalEventLocked(event, m.coreGeneration.Load())
}

// enqueueReconnectEvent records a generation-bound pending intent when the
// bounded event channel is full. The event loop retries after every dequeue,
// guaranteeing forward progress without an untracked goroutine or timer.
func (m *Manager) enqueueReconnectEvent(generation uint64) internalEventEnqueueResult {
	if m == nil {
		return internalEventInactive
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.desiredConnection {
		if m.reconnectGeneration == generation {
			m.reconnectPending = false
			m.reconnectGeneration = 0
		}
		return internalEventInactive
	}
	result := m.enqueueInternalEventLocked(eventStart, generation)
	switch result {
	case internalEventEnqueued:
		if m.reconnectGeneration == generation {
			m.reconnectPending = false
			m.reconnectGeneration = 0
		}
	case internalEventQueueFull:
		m.reconnectPending = true
		m.reconnectGeneration = generation
	default:
		if m.reconnectGeneration == generation {
			m.reconnectPending = false
			m.reconnectGeneration = 0
		}
	}
	return result
}

func (m *Manager) promotePendingReconnect() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.reconnectPending {
		return
	}
	generation := m.reconnectGeneration
	if !m.desiredConnection || generation == 0 || m.coreGeneration.Load() != generation {
		m.reconnectPending = false
		m.reconnectGeneration = 0
		return
	}
	if m.enqueueInternalEventLocked(eventStart, generation) == internalEventEnqueued {
		m.reconnectPending = false
		m.reconnectGeneration = 0
	}
}

func (m *Manager) consumeQueuedReconnect(envelope internalEventEnvelope) {
	if m == nil || envelope.kind != eventStart || envelope.generation == 0 {
		return
	}
	m.mu.Lock()
	if m.reconnectPending && m.reconnectGeneration == envelope.generation {
		m.reconnectPending = false
		m.reconnectGeneration = 0
	}
	m.mu.Unlock()
}

func (m *Manager) drainInternalEvents() {
	for {
		select {
		case <-m.eventCh:
			continue
		default:
			return
		}
	}
}

func (m *Manager) scheduleTargetedCheck() {
	m.timerMu.Lock()
	if m.targetedCheckScheduled {
		m.timerMu.Unlock()
		m.debouncedChecks.Add(1)
		return
	}
	m.targetedCheckScheduled = true
	m.timerMu.Unlock()

	m.scheduleAfter(m.cfg.HealthPolicy.IndicationDebounce, func() {
		m.timerMu.Lock()
		m.targetedCheckScheduled = false
		m.timerMu.Unlock()

		if m.enqueueCurrentInternalEvent(eventCheckTargeted) != internalEventEnqueued {
			m.debouncedChecks.Add(1)
		}
	})
}

func isRegisteredOrRoaming(state qmi.RegistrationState) bool {
	return state == qmi.RegStateRegistered || state == qmi.RegStateRoaming
}

func (m *Manager) maybeSchedulePostRegRefresh(prev, curr *qmi.ServingSystem, reason string) {
	if m == nil || curr == nil {
		return
	}
	if !isRegisteredOrRoaming(curr.RegistrationState) {
		return
	}

	// 当注册状态从未注册/搜索切到已注册时优先触发；
	// 即使已是注册态，只要长时间未补拉也允许周期性兜底。
	transitionToReady := prev == nil || !isRegisteredOrRoaming(prev.RegistrationState)

	now := time.Now()
	m.timerMu.Lock()
	if m.postRegRefreshScheduled {
		m.timerMu.Unlock()
		return
	}
	if !transitionToReady && !m.postRegRefreshLastAt.IsZero() && now.Sub(m.postRegRefreshLastAt) < defaultPostRegRefreshCooldown {
		m.timerMu.Unlock()
		return
	}
	m.postRegRefreshScheduled = true
	m.timerMu.Unlock()

	m.scheduleAfter(defaultPostRegRefreshDelay, func() {
		m.timerMu.Lock()
		m.postRegRefreshScheduled = false
		m.postRegRefreshLastAt = time.Now()
		m.timerMu.Unlock()
		m.doPostRegistrationRefresh(reason)
	})
}

func (m *Manager) doPostRegistrationRefresh(reason string) {
	if m == nil {
		return
	}

	m.mu.RLock()
	coreReady := m.coreReady
	stopping := m.state == StateStopping
	m.mu.RUnlock()
	if !coreReady || stopping {
		return
	}

	start := time.Now()
	ctx, cancel := m.opContext(defaultPostRegRefreshTimeout)
	defer cancel()

	var (
		servingUpdated bool
		signalUpdated  bool
		rssiUpdated    bool
	)

	if ss, err := m.getServingSystem(ctx); err != nil {
		m.log.WithError(err).Debug("Post-reg refresh: failed to query serving system")
	} else if ss != nil {
		servingUpdated = true
		m.snapshot.updateServingFromQuery(ss)
		m.emitEvent(Event{
			Type:          EventServingSystemChanged,
			State:         m.State(),
			ServingSystem: ss,
		})
	}

	if info, err := m.GetSignalInfo(ctx); err != nil {
		m.log.WithError(err).Debug("Post-reg refresh: failed to query NAS signal info")
	} else if info != nil {
		signalUpdated = true
		m.snapshot.updateNASSignalInfo(info)
		m.emitEvent(Event{
			Type:          EventNASSignalInfoChanged,
			State:         m.State(),
			NASSignalInfo: info,
		})
	}

	if sig, err := m.getSignalStrength(ctx); err != nil {
		m.log.WithError(err).Debug("Post-reg refresh: failed to query signal strength")
	} else if sig != nil {
		rssiUpdated = true
		m.emitSignalUpdate(sig)
	}

	m.log.WithField("reason", reason).
		WithField("serving_updated", servingUpdated).
		WithField("signal_info_updated", signalUpdated).
		WithField("signal_strength_updated", rssiUpdated).
		WithField("elapsed_ms", time.Since(start).Milliseconds()).
		Debug("Post-reg refresh completed")
}

// OpenLogicalChannel opens a UIM logical channel using the manager stop timeout.
func (m *Manager) OpenLogicalChannel(slot uint8, aid []byte) (byte, error) {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Stop)
	defer cancel()
	return m.OpenLogicalChannelContext(ctx, slot, aid)
}

// OpenLogicalChannelContext opens a UIM logical channel using the caller context.
func (m *Manager) OpenLogicalChannelContext(ctx context.Context, slot uint8, aid []byte) (byte, error) {
	if m.openLogicalChannelHook != nil {
		return m.openLogicalChannelHook(ctx, slot, aid)
	}
	return withUIMRecoveryValue(m, "OpenLogicalChannel", func(uim *qmi.UIMService) (byte, error) {
		return uim.OpenLogicalChannel(ctx, slot, aid)
	})
}

// CloseLogicalChannel closes a UIM logical channel using the manager stop timeout.
func (m *Manager) CloseLogicalChannel(slot uint8, channel uint8) error {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Stop)
	defer cancel()
	return m.CloseLogicalChannelContext(ctx, slot, channel)
}

// CloseLogicalChannelContext closes a UIM logical channel using the caller context.
func (m *Manager) CloseLogicalChannelContext(ctx context.Context, slot uint8, channel uint8) error {
	if m.closeLogicalChannelHook != nil {
		return m.closeLogicalChannelHook(ctx, slot, channel)
	}
	return m.withUIMRecovery("CloseLogicalChannel", func(uim *qmi.UIMService) error {
		return uim.CloseLogicalChannel(ctx, slot, channel)
	})
}

// SendAPDU transmits a raw APDU using the manager stop timeout.
func (m *Manager) SendAPDU(slot uint8, channel uint8, command []byte) ([]byte, error) {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Stop)
	defer cancel()
	return m.SendAPDUContext(ctx, slot, channel, command)
}

// SendAPDUContext transmits a raw APDU using the caller context.
func (m *Manager) SendAPDUContext(ctx context.Context, slot uint8, channel uint8, command []byte) ([]byte, error) {
	if m.sendAPDUHook != nil {
		return m.sendAPDUHook(ctx, slot, channel, command)
	}
	return withUIMRecoveryValue(m, "SendAPDU", func(uim *qmi.UIMService) ([]byte, error) {
		return uim.SendAPDU(ctx, slot, channel, command)
	})
}

// GetNativeMCCMNC 获取原生归属地 MCC 和 MNC
func (m *Manager) GetNativeSPN(ctx context.Context) (string, error) {
	return withUIMRecoveryValue(m, "GetNativeSPN", func(uim *qmi.UIMService) (string, error) {
		return uim.GetNativeSPN(ctx)
	})
}

func (m *Manager) GetSIMMetadata(ctx context.Context) (*qmi.SIMMetadata, error) {
	return withUIMRecoveryValue(m, "GetSIMMetadata", func(uim *qmi.UIMService) (*qmi.SIMMetadata, error) {
		return uim.GetSIMMetadata(ctx)
	})
}

func (m *Manager) GetUSIMAID(ctx context.Context) ([]byte, error) {
	return withUIMRecoveryValue(m, "GetUSIMAID", func(uim *qmi.UIMService) ([]byte, error) {
		return uim.GetUSIMAID(ctx)
	})
}

func (m *Manager) GetISIMAID(ctx context.Context) ([]byte, error) {
	return withUIMRecoveryValue(m, "GetISIMAID", func(uim *qmi.UIMService) ([]byte, error) {
		return uim.GetISIMAID(ctx)
	})
}

func (m *Manager) GetNativeMCCMNC(ctx context.Context) (mcc, mnc string, err error) {
	type nativeLocation struct {
		mcc string
		mnc string
	}
	location, err := withUIMRecoveryValue(m, "GetNativeMCCMNC", func(uim *qmi.UIMService) (nativeLocation, error) {
		localMCC, localMNC, callErr := uim.GetNativeMCCMNC(ctx)
		return nativeLocation{
			mcc: localMCC,
			mnc: localMNC,
		}, callErr
	})
	if err != nil {
		return "", "", err
	}
	return location.mcc, location.mnc, nil
}

func (m *Manager) rotationSessionCurrent(token coreSessionToken, wds *qmi.WDSService) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coreSessionCurrentLocked(token) && m.wds == wds
}

func (m *Manager) startDataCall(ctx context.Context, wds *qmi.WDSService, family uint8) (uint32, error) {
	if m.startDataCallHook != nil {
		return m.startDataCallHook(ctx, wds, family)
	}
	return wds.StartNetworkInterface(ctx,
		m.cfg.APN, m.cfg.Username, m.cfg.Password,
		m.cfg.AuthType, family)
}

func (m *Manager) stopDataCall(ctx context.Context, wds *qmi.WDSService, handle uint32) error {
	if wds == nil || handle == 0 {
		return nil
	}
	var err error
	if m.stopDataCallHook != nil {
		err = m.stopDataCallHook(ctx, wds, handle)
	} else {
		err = wds.StopNetworkInterface(ctx, handle)
	}
	if isExistingDataStopNoopError(err) {
		return nil
	}
	return err
}

func (m *Manager) getRotationRuntimeSettings(ctx context.Context, wds *qmi.WDSService) (*qmi.RuntimeSettings, error) {
	if m.getRotationRuntimeSettingsHook != nil {
		return m.getRotationRuntimeSettingsHook(ctx, wds)
	}
	return wds.GetRuntimeSettings(ctx, qmi.IpFamilyV4)
}

func (m *Manager) dataInterfaceName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.muxIface != "" {
		return m.muxIface
	}
	return m.cfg.Device.NetInterface
}

func (m *Manager) flushRotationAddresses(ifname string) error {
	if m.flushRotationAddressesHook != nil {
		return m.flushRotationAddressesHook(ifname)
	}
	if strings.TrimSpace(ifname) == "" {
		return nil
	}
	addressErr := netcfg.FlushAddresses(ifname)
	routeErr := netcfg.FlushRoutes(ifname)
	if addressErr != nil {
		return addressErr
	}
	return routeErr
}

func (m *Manager) flushRadioDataPlane(ifname string) error {
	if m.flushRadioDataPlaneHook != nil {
		return m.flushRadioDataPlaneHook(ifname)
	}
	if strings.TrimSpace(ifname) == "" {
		return nil
	}
	addressErr := netcfg.FlushAddresses(ifname)
	routeErr := netcfg.FlushRoutes(ifname)
	return errors.Join(addressErr, routeErr)
}

func pendingDataCallEqual(call pendingDataCall, generation uint64, wds *qmi.WDSService, family uint8, handle uint32) bool {
	return call.generation == generation && call.wds == wds && call.family == family && call.handle == handle
}

// trackPendingDataCallLocked records modem ownership that could not be
// released. m.mu must be held; no QMI operation is performed under the lock.
func (m *Manager) trackPendingDataCallLocked(call pendingDataCall) {
	if call.generation == 0 || call.wds == nil || call.handle == 0 {
		return
	}
	for _, current := range m.pendingDataCalls {
		if pendingDataCallEqual(current, call.generation, call.wds, call.family, call.handle) {
			return
		}
	}
	m.pendingDataCalls = append(m.pendingDataCalls, call)
}

func (m *Manager) removePendingDataCallLocked(generation uint64, wds *qmi.WDSService, family uint8, handle uint32) {
	kept := m.pendingDataCalls[:0]
	for _, call := range m.pendingDataCalls {
		if pendingDataCallEqual(call, generation, wds, family, handle) {
			continue
		}
		kept = append(kept, call)
	}
	m.pendingDataCalls = kept
}

func (m *Manager) pendingDataCallsForFamily(token coreSessionToken, family uint8) []pendingDataCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.coreSessionCurrentLocked(token) {
		return nil
	}
	var calls []pendingDataCall
	for _, call := range m.pendingDataCalls {
		if call.generation == token.generation && call.family == family {
			calls = append(calls, call)
		}
	}
	return calls
}

// retryPendingDataCallsForFamily discharges old ownership before any new call
// for the same family is started, preventing duplicate modem sessions.
func (m *Manager) retryPendingDataCallsForFamily(ctx context.Context, token coreSessionToken, family uint8) error {
	for _, call := range m.pendingDataCallsForFamily(token, family) {
		if err := m.stopDataCall(ctx, call.wds, call.handle); err != nil {
			return fmt.Errorf("release pending data call 0x%08x: %w", call.handle, err)
		}
		m.mu.Lock()
		m.removePendingDataCallLocked(call.generation, call.wds, call.family, call.handle)
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) moveActiveDataCallToPendingLocked(generation uint64, wds *qmi.WDSService, family uint8, handle uint32) {
	if m.coreGeneration.Load() != generation {
		return
	}
	switch family {
	case qmi.IpFamilyV4:
		if m.wds == wds && m.handleV4 == handle {
			m.handleV4 = 0
		}
	case qmi.IpFamilyV6:
		if m.wdsV6 == wds && m.handleV6 == handle {
			m.handleV6 = 0
		}
	}
	m.trackPendingDataCallLocked(pendingDataCall{generation: generation, wds: wds, family: family, handle: handle})
}

// rollbackDataCallCandidate owns cleanup for a handle returned by a QMI start
// that cannot be committed to the exact client generation. It also clears an
// already-published matching handle after a later rotation step fails.
func (m *Manager) rollbackDataCallCandidate(token coreSessionToken, wds *qmi.WDSService, family uint8, handle uint32) {
	if wds == nil || handle == 0 {
		return
	}
	timeout := m.cfg.Timeouts.Stop
	if timeout <= 0 {
		timeout = defaultTimeouts.Stop
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), timeout)
	stopErr := m.stopDataCall(cleanupCtx, wds, handle)
	cleanupCancel()
	if stopErr != nil {
		m.mu.Lock()
		m.moveActiveDataCallToPendingLocked(token.generation, wds, family, handle)
		m.mu.Unlock()
		m.log.WithError(stopErr).Warn("Failed to roll back uncommitted data call")
		return
	}

	m.mu.Lock()
	m.removePendingDataCallLocked(token.generation, wds, family, handle)
	if m.coreGeneration.Load() == token.generation {
		switch family {
		case qmi.IpFamilyV4:
			if m.wds == wds && m.handleV4 == handle {
				m.handleV4 = 0
			}
		case qmi.IpFamilyV6:
			if m.wdsV6 == wds && m.handleV6 == handle {
				m.handleV6 = 0
			}
		}
	}
	m.mu.Unlock()
}

func (m *Manager) commitDataCallHandle(token coreSessionToken, wds *qmi.WDSService, family uint8, handle uint32) bool {
	if wds == nil || handle == 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.coreSessionCurrentLocked(token) || !m.desiredConnection {
		return false
	}
	switch family {
	case qmi.IpFamilyV4:
		if m.wds != wds || m.handleV4 != 0 {
			return false
		}
		m.handleV4 = handle
	case qmi.IpFamilyV6:
		if m.wdsV6 != wds || m.handleV6 != 0 {
			return false
		}
		m.handleV6 = handle
	default:
		return false
	}
	m.removePendingDataCallLocked(token.generation, wds, family, handle)
	return true
}

// dialMissingDataFamilies starts only absent legs for an exact session. This
// lets a failed IPv4 rotation preserve a live IPv6 call without duplicating it.
func (m *Manager) dialMissingDataFamilies(ctx context.Context, token coreSessionToken) error {
	if ctx == nil || ctx.Err() != nil || !m.coreSessionCurrent(token) {
		return ErrServiceNotReady("qmi-core")
	}
	if !m.cfg.EnableIPv4 && !m.cfg.EnableIPv6 {
		return fmt.Errorf("no IP family enabled")
	}

	if m.cfg.EnableIPv4 {
		m.mu.RLock()
		wds, missing := m.wds, m.handleV4 == 0
		m.mu.RUnlock()
		if wds == nil {
			return ErrServiceNotReady("WDS")
		}
		if err := m.retryPendingDataCallsForFamily(ctx, token, qmi.IpFamilyV4); err != nil {
			return err
		}
		if missing {
			m.log.Info("Starting IPv4 data call...")
			handle, err := m.startDataCall(ctx, wds, qmi.IpFamilyV4)
			if err != nil {
				return fmt.Errorf("IPv4 dial failed: %w", err)
			}
			if !m.commitDataCallHandle(token, wds, qmi.IpFamilyV4, handle) {
				m.rollbackDataCallCandidate(token, wds, qmi.IpFamilyV4, handle)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrServiceNotReady("qmi-core")
			}
			m.log.Infof("IPv4 connected, handle=0x%08x", handle)
		}
	}

	if m.cfg.EnableIPv6 {
		m.mu.RLock()
		wdsV6, missing := m.wdsV6, m.handleV6 == 0
		m.mu.RUnlock()
		if wdsV6 != nil {
			if err := m.retryPendingDataCallsForFamily(ctx, token, qmi.IpFamilyV6); err != nil {
				return err
			}
		}
		if wdsV6 == nil {
			m.mu.RLock()
			hasV4 := m.handleV4 != 0
			m.mu.RUnlock()
			if !hasV4 {
				return ErrServiceNotReady("WDS IPv6")
			}
		} else if missing {
			m.log.Info("Starting IPv6 data call...")
			handle, err := m.startDataCall(ctx, wdsV6, qmi.IpFamilyV6)
			if err != nil {
				m.mu.RLock()
				hasV4 := m.handleV4 != 0
				m.mu.RUnlock()
				if !hasV4 {
					return fmt.Errorf("IPv6 dial failed: %w", err)
				}
				m.log.WithError(err).Warn("IPv6 dial failed; continuing with IPv4")
			} else if !m.commitDataCallHandle(token, wdsV6, qmi.IpFamilyV6, handle) {
				m.rollbackDataCallCandidate(token, wdsV6, qmi.IpFamilyV6, handle)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrServiceNotReady("qmi-core")
			} else {
				m.log.Infof("IPv6 connected, handle=0x%08x", handle)
			}
		}
	}

	m.mu.RLock()
	current := m.coreSessionCurrentLocked(token)
	hasV4, hasV6 := m.handleV4 != 0, m.handleV6 != 0
	m.mu.RUnlock()
	if !current || (m.cfg.EnableIPv4 && !hasV4) || (!m.cfg.EnableIPv4 && m.cfg.EnableIPv6 && !hasV6) {
		return ErrServiceNotReady("data-plane")
	}
	return nil
}

func (m *Manager) finishIPRotation(token coreSessionToken, wds *qmi.WDSService, disrupted bool, rotationErr error) {
	disconnected := false
	m.mu.Lock()
	m.isRotating = false
	if rotationErr != nil &&
		disrupted &&
		m.coreSessionCurrentLocked(token) &&
		m.wds == wds {
		m.settings = nil
		if m.state != StateDisconnected {
			m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
			m.state = StateDisconnected
			m.publishCoreStatusLocked()
		}
		disconnected = true
	}
	m.mu.Unlock()
	if !disconnected {
		return
	}
	if flushErr := m.flushRotationAddresses(m.dataInterfaceName()); flushErr != nil {
		m.log.WithError(flushErr).Warn("Failed to flush host data plane after IP rotation failure")
	}
	m.emitEventForGeneration(Event{Type: EventDisconnected, State: StateDisconnected, Error: rotationErr}, token.generation)
	if m.emitReconnectingForSessionIfDesired(token, rotationErr) {
		_ = m.enqueueReconnectEvent(token.generation)
	}
}

func (m *Manager) emitReconnectingForSessionIfDesired(token coreSessionToken, eventErr error) bool {
	m.mu.RLock()
	if !m.coreSessionCurrentLocked(token) ||
		!m.cfg.AutoReconnect ||
		!m.desiredConnection ||
		m.state != StateDisconnected {
		m.mu.RUnlock()
		return false
	}
	// External callbacks are best-effort. Reconnect eligibility and the
	// internal durable intent must not depend on callback queue availability.
	if m.events != nil {
		_ = m.events.Emit(Event{
			Type:       EventReconnecting,
			Generation: token.generation,
			State:      StateDisconnected,
			Error:      eventErr,
		})
	}
	m.mu.RUnlock()
	return true
}

func (m *Manager) rollbackRotationHandle(token coreSessionToken, wds *qmi.WDSService, handle uint32) {
	m.rollbackDataCallCandidate(token, wds, qmi.IpFamilyV4, handle)
}

// RotateIP disconnects and reconnects to get a new IP address / RotateIP 断开并重新连接以获取新 IP 地址
func (m *Manager) RotateIP() (err error) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.connectMu.Lock()
	defer m.connectMu.Unlock()

	m.mu.Lock()
	if !m.cfg.EnableIPv4 {
		m.mu.Unlock()
		return fmt.Errorf("IPv4 is disabled, cannot rotate IPv4 address")
	}
	if m.state != StateConnected {
		m.mu.Unlock()
		return fmt.Errorf("not connected, cannot rotate IP")
	}
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	if !m.coreSessionCurrentLocked(token) {
		m.mu.Unlock()
		return ErrServiceNotReady("qmi-core")
	}
	wds := m.wds
	if wds == nil {
		m.mu.Unlock()
		return ErrServiceNotReady("WDS")
	}
	oldSettings := m.settings
	handleV4 := m.handleV4
	m.isRotating = true // Suppress status checks / 抑制状态检查
	m.mu.Unlock()
	disrupted := false

	defer func() {
		m.finishIPRotation(token, wds, disrupted, err)
	}()

	oldIP := ""
	if oldSettings != nil && oldSettings.IPv4Address != nil {
		oldIP = oldSettings.IPv4Address.String()
	}
	m.log.Infof("Rotating IP (current: %s)...", oldIP)

	ctx, cancel := contextWithMaxTimeout(token.runCtx, m.cfg.Timeouts.Dial)
	defer cancel()

	// 1. Disconnect data call / 1. 断开数据呼叫
	if handleV4 != 0 {
		if stopErr := m.stopDataCall(ctx, wds, handleV4); stopErr != nil {
			return fmt.Errorf("stop IPv4 data call for rotation: %w", stopErr)
		}
		disrupted = true
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		m.mu.Lock()
		if m.coreSessionCurrentLocked(token) && m.wds == wds && m.handleV4 == handleV4 {
			m.handleV4 = 0
		}
		m.mu.Unlock()
	}

	// Flush old addresses to avoid duplicates / 清除旧地址以避免重复
	_ = m.flushRotationAddresses(m.dataInterfaceName())
	disrupted = true

	// 3. Reconnect / 3. 重新连接
	handle, err := m.startDataCall(ctx, wds, qmi.IpFamilyV4)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		return m.rotateViaRadioReset(token, wds)
	}
	if !m.rotationSessionCurrent(token, wds) {
		m.rollbackRotationHandle(token, wds, handle)
		return ErrServiceNotReady("qmi-core")
	}

	// CHECK BEFORE CONFIG: Quickly check if IP actually changed / 配置前检查: 快速检查 IP 是否真的变了
	settings, err := m.getRotationRuntimeSettings(ctx, wds)
	if ctx.Err() != nil || !m.rotationSessionCurrent(token, wds) {
		m.rollbackRotationHandle(token, wds, handle)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrServiceNotReady("qmi-core")
	}
	if err == nil && settings != nil && settings.IPv4Address != nil && settings.IPv4Address.String() == oldIP {
		m.log.Warn("IP same after redial, forcing radio reset...")
		m.rollbackRotationHandle(token, wds, handle)
		return m.rotateViaRadioReset(token, wds)
	}
	if !m.commitDataCallHandle(token, wds, qmi.IpFamilyV4, handle) {
		m.rollbackRotationHandle(token, wds, handle)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrServiceNotReady("qmi-core")
	}

	// 4. Reconfigure network / 4. 重新配置网络
	if err := m.configureNetwork(); err != nil {
		m.rollbackRotationHandle(token, wds, handle)
		return err
	}

	// Restore connected state / 恢复已连接状态
	m.mu.Lock()
	if !m.coreSessionCurrentLocked(token) || m.wds != wds || !m.desiredConnection {
		m.mu.Unlock()
		m.rollbackRotationHandle(token, wds, handle)
		return ErrServiceNotReady("qmi-core")
	}
	if m.state != StateConnected {
		m.log.Infof("State: %s -> %s", m.state, StateConnected)
		m.state = StateConnected
		m.publishCoreStatusLocked()
	}
	m.retryCount = 0
	newSettings := m.settings
	m.mu.Unlock()

	newIP := ""
	if newSettings != nil && newSettings.IPv4Address != nil {
		newIP = newSettings.IPv4Address.String()
	}

	if oldIP == newIP {
		m.log.Warn("IP unchanged, trying radio reset...")
		m.rollbackRotationHandle(token, wds, handle)
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		return m.rotateViaRadioReset(token, wds)
	}

	m.log.Infof("IP rotated: %s -> %s", oldIP, newIP)

	// Emit IP change event / 5. 发送 IP 变化事件
	m.emitEventForGeneration(Event{
		Type:     EventIPChanged,
		State:    StateConnected,
		Settings: newSettings,
	}, token.generation)

	return nil
}

// rotateViaRadioReset performs IP rotation by resetting the radio / rotateViaRadioReset 通过重置射频执行 IP 轮换
// lifecycleMu and connectMu must be held by the caller.
func (m *Manager) rotateViaRadioReset(token coreSessionToken, wds *qmi.WDSService) error {
	ctx, cancel := contextWithMaxTimeout(token.runCtx, m.cfg.Timeouts.Dial)
	defer cancel()

	if !m.rotationSessionCurrent(token, wds) {
		return ErrServiceNotReady("qmi-core")
	}
	m.mu.RLock()
	oldSettings := m.settings
	handleV4 := m.handleV4
	m.mu.RUnlock()
	oldIP := ""
	if oldSettings != nil && oldSettings.IPv4Address != nil {
		oldIP = oldSettings.IPv4Address.String()
	}

	// 1. Disconnect current call / 1. 断开当前呼叫
	if handleV4 != 0 {
		if stopErr := m.stopDataCall(ctx, wds, handleV4); stopErr != nil {
			return fmt.Errorf("stop IPv4 data call before radio reset: %w", stopErr)
		}
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		m.mu.Lock()
		if m.coreSessionCurrentLocked(token) && m.wds == wds && m.handleV4 == handleV4 {
			m.handleV4 = 0
		}
		m.mu.Unlock()
	}

	// Flush old addresses / 2. 清除旧地址
	_ = m.flushRotationAddresses(m.dataInterfaceName())

	if _, err := m.radioPowerCycleForSession(ctx, token, "rotateViaRadioReset.RadioPowerCycle"); err != nil {
		return fmt.Errorf("radio power cycle during IP rotation: %w", err)
	}

	// Holding lifecycleMu deliberately blocks indication side effects. Poll NAS
	// against the exact core session instead of waiting on regNotify, which is
	// serviced by the indication path and would deadlock behind lifecycleMu.
	m.log.Info("Waiting for network registration (session-bound polling)...")
	registrationDeadline := time.NewTimer(10 * time.Second)
	registrationPoll := time.NewTicker(250 * time.Millisecond)
	defer registrationDeadline.Stop()
	defer registrationPoll.Stop()
registrationLoop:
	for {
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		registered, err := m.queryNASRegisteredWithContext(ctx)
		if !m.rotationSessionCurrent(token, wds) {
			return ErrServiceNotReady("qmi-core")
		}
		if registered {
			break registrationLoop
		}
		if err != nil {
			m.log.WithError(err).Debug("Registration poll failed during IP rotation")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-registrationDeadline.C:
			m.log.Warn("Registration timeout, trying to connect anyway")
			break registrationLoop
		case <-registrationPoll.C:
		}
	}

	// 5. Reconnect / 6. 重新连接
	if err := m.dialMissingDataFamilies(ctx, token); err != nil {
		return fmt.Errorf("redial after radio reset failed: %w", err)
	}

	// 6. Reconfigure network / 7. 重新配置网络
	if err := m.configureNetwork(); err != nil {
		return err
	}

	// Restore connected state / 恢复已连接状态
	m.mu.Lock()
	if !m.coreSessionCurrentLocked(token) || m.wds != wds || !m.desiredConnection {
		m.mu.Unlock()
		return ErrServiceNotReady("qmi-core")
	}
	if m.state != StateConnected {
		m.log.Infof("State: %s -> %s", m.state, StateConnected)
		m.state = StateConnected
		m.publishCoreStatusLocked()
	}
	m.retryCount = 0
	newSettings := m.settings
	m.mu.Unlock()

	newIP := ""
	if newSettings != nil && newSettings.IPv4Address != nil {
		newIP = newSettings.IPv4Address.String()
	}

	m.log.Infof("IP rotated via radio reset: %s -> %s", oldIP, newIP)

	// Emit IP change event / 8. 发送 IP 变化事件
	m.emitEventForGeneration(Event{
		Type:     EventIPChanged,
		State:    StateConnected,
		Settings: newSettings,
	}, token.generation)

	return nil
}

// ============================================================================
// Internal methods / 内部方法
// ============================================================================

func (m *Manager) setState(s State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// StateStopping is terminal for the current manager lifetime. Work already
	// in flight may complete after Stop cancels the context, but it must not
	// publish Connected/Disconnected over the shutdown state.
	if m.stopped || (m.state == StateStopping && s != StateStopping) {
		return
	}
	if m.state != s {
		m.log.Infof("State: %s -> %s", m.state, s)
		m.state = s
		m.publishCoreStatusLocked()
	}
}

func (m *Manager) finishStopState() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
	}
	m.setCorePhaseLocked(CorePhaseStopped, "stop_complete", "", nil)
	m.publishCoreStatusLocked()
}

type startupServiceTask struct {
	run func(context.Context) error
}

func (m *Manager) runStartupServiceTasks(ctx context.Context, fatal bool, tasks []startupServiceTask) error {
	if ctx == nil {
		ctx = context.Background()
	}

	taskCtx := ctx
	cancel := func() {}
	if fatal {
		taskCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			if fatal {
				cancel()
			}
		}
		mu.Unlock()
	}

	wg.Add(len(tasks))
	for _, task := range tasks {
		task := task
		go func() {
			defer wg.Done()
			if err := taskCtx.Err(); err != nil {
				recordErr(err)
				return
			}
			if err := task.run(taskCtx); err != nil {
				recordErr(err)
			}
		}()
	}
	wg.Wait()
	if fatal {
		if firstErr != nil {
			return firstErr
		}
		return taskCtx.Err()
	}
	return nil
}

// hasQMIService 检查底层 Client 是否声明支持该服务。
// 仅在 client 初始化完成后调用有效。
func (m *Manager) hasQMIService(service uint8) bool {
	if m.client == nil {
		return false
	}
	return m.client.HasService(service)
}

func (m *Manager) allocateServices(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if m.shouldAllocateDataPlaneAtStart() {
		if err := m.ensureDataPlaneServices(ctx); err != nil {
			return err
		}
	} else {
		m.log.Debug("Skipping data-plane service allocation at startup")
	}

	coreTasks := []startupServiceTask{
		{
			run: func(taskCtx context.Context) error {
				m.log.Debug("Allocating NAS client...")
				nas, err := m.createNASService(taskCtx)
				if err != nil {
					m.log.WithError(err).Warn("Failed to allocate NAS client")
					return fmt.Errorf("failed to allocate NAS client: %w", err)
				}
				m.log.Debug("Allocated NAS client")
				m.mu.Lock()
				m.nas = nas
				m.mu.Unlock()

				indCtx, cancel := contextWithMaxTimeout(taskCtx, m.cfg.Timeouts.IndicationRegister)
				defer cancel()
				if err := m.registerNASIndicationsWithContext(indCtx, qmi.NASIndicationRegistration{
					ServingSystemChanged:        true,
					SystemInfo:                  true,
					NetworkTime:                 true,
					SignalInfo:                  true,
					OperatorName:                true,
					NetworkReject:               true,
					IncrementalNetworkScan:      true,
					EventReportSignalThresholds: []int8{-60, -85},
				}); err != nil {
					m.log.WithError(err).Warn("Failed to register NAS indications")
					return fmt.Errorf("failed to register NAS indications: %w", err)
				}
				return nil
			},
		},
		{
			run: func(taskCtx context.Context) error {
				m.log.Debug("Allocating DMS client...")
				dms, err := m.createDMSService(taskCtx)
				if err != nil {
					m.log.WithError(err).Warn("Failed to allocate DMS client")
					return fmt.Errorf("failed to allocate DMS client: %w", err)
				}
				m.log.Debug("Allocated DMS client")
				m.mu.Lock()
				m.dms = dms
				m.mu.Unlock()
				return nil
			},
		},
		{
			run: func(taskCtx context.Context) error {
				m.log.Debug("Allocating UIM client...")
				uim, err := m.createUIMService(taskCtx)
				if err != nil {
					m.log.WithError(err).Warn("Failed to allocate UIM client")
					return fmt.Errorf("failed to allocate UIM client: %w", err)
				}
				m.log.Debug("Allocated UIM client")
				m.mu.Lock()
				m.uim = uim
				m.mu.Unlock()

				indCtx, cancel := contextWithMaxTimeout(taskCtx, m.cfg.Timeouts.IndicationRegister)
				defer cancel()
				acceptedMask, registerErr := m.registerUIMIndicationsWithContext(indCtx, uim)
				if registerErr != nil {
					m.log.WithError(registerErr).Warn("Failed to register UIM indications")
					return fmt.Errorf("failed to register UIM indications: %w", registerErr)
				}
				m.log.WithField("requested_mask", m.uimIndicationRegistrationMask()).WithField("accepted_mask", acceptedMask).Info("UIM indications registered")
				return nil
			},
		},
	}
	if err := m.runStartupServiceTasks(ctx, true, coreTasks); err != nil {
		return err
	}

	auxTasks := []startupServiceTask{
		{
			run: func(taskCtx context.Context) error {
				if m.cfg.DisableWMSInd {
					m.log.Debug("Skipping WMS client allocation")
					return nil
				}
				if !m.hasQMIService(qmi.ServiceWMS) {
					m.log.Debug("Skipping WMS client allocation (modem not supported)")
					return nil
				}
				m.log.Debug("Allocating WMS client...")
				wms, err := m.createWMSService(taskCtx)
				if err != nil {
					m.log.WithError(err).Warn("Failed to allocate WMS client")
					return err
				}
				m.log.Debug("Allocated WMS client")
				m.mu.Lock()
				m.wms = wms
				m.mu.Unlock()
				m.recoverWMSStateWithContext(taskCtx)
				return nil
			},
		},
		{
			run: func(taskCtx context.Context) error {
				if !m.hasQMIService(qmi.ServiceVOICE) {
					m.log.Debug("Skipping VOICE client allocation (modem not supported)")
					return nil
				}
				m.log.Debug("Allocating VOICE client...")
				voice, err := m.createVOICEService(taskCtx)
				if err != nil {
					m.log.WithError(err).Warn("Failed to allocate VOICE client")
					return err
				}
				m.log.Debug("Allocated VOICE client")
				m.mu.Lock()
				m.voice = voice
				m.mu.Unlock()

				if cfg, ok := m.voiceIndicationRegistration(); ok {
					indCtx, cancel := contextWithMaxTimeout(taskCtx, m.cfg.Timeouts.IndicationRegister)
					defer cancel()
					if err := m.registerVOICEIndicationsWithContext(indCtx, cfg); err != nil {
						m.log.WithError(err).Warn("Failed to register VOICE indications")
						return err
					}
				} else {
					m.log.Debug("VOICE indications disabled by config")
				}
				return nil
			},
		},
	}
	_ = m.runStartupServiceTasks(ctx, false, auxTasks)

	// IMS/IMSA/IMSP
	// 当前设备族在这些服务上经常返回 CTL client-id 分配失败（如 0x001f），
	// 且主链路不依赖它们，默认跳过分配以减少启动噪声和恢复抖动。
	m.ims = nil
	m.imsa = nil
	m.imsp = nil
	m.log.Debug("Skipping IMS/IMSA/IMSP client allocation")

	return nil
}

func (m *Manager) imsaIndicationRegistration() (qmi.IMSAIndicationRegistration, bool) {
	if m.cfg.DisableIMSAInd {
		return qmi.IMSAIndicationRegistration{}, false
	}
	return qmi.IMSAIndicationRegistration{
		RegistrationStatusChanged: true,
		ServicesStatusChanged:     true,
	}, true
}

func (m *Manager) voiceIndicationRegistration() (qmi.VoiceIndicationRegistration, bool) {
	if m.cfg.DisableVOICEInd {
		return qmi.VoiceIndicationRegistration{}, false
	}
	return qmi.VoiceIndicationRegistration{
		CallNotificationEvents:                 true,
		SupplementaryServiceNotificationEvents: true,
		USSDNotificationEvents:                 true,
	}, true
}

type rawIPKernelOps struct {
	readFile  func(string) ([]byte, error)
	writeFile func(string, []byte, os.FileMode) error
	bringDown func(string) error
}

func rawIPStateEnabled(content []byte) bool {
	return strings.EqualFold(strings.TrimSpace(string(content)), "Y")
}

func ensureKernelRawIP(ifname, sysfsPath string, ops rawIPKernelOps) (bool, error) {
	content, err := ops.readFile(sysfsPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("read raw_ip state for interface %s: %w", ifname, err)
	}
	if rawIPStateEnabled(content) {
		return true, nil
	}

	if err := ops.bringDown(ifname); err != nil {
		return true, fmt.Errorf("bring interface %s down before enabling raw_ip: %w", ifname, err)
	}
	if err := ops.writeFile(sysfsPath, []byte("Y"), 0o644); err != nil {
		return true, fmt.Errorf("write raw_ip=Y for interface %s after link down: %w", ifname, err)
	}

	content, err = ops.readFile(sysfsPath)
	if err != nil {
		return true, fmt.Errorf("read back raw_ip state for interface %s: %w", ifname, err)
	}
	if !rawIPStateEnabled(content) {
		return true, fmt.Errorf("verify raw_ip for interface %s: got %q, want %q", ifname, strings.TrimSpace(string(content)), "Y")
	}
	return true, nil
}

// enableRawIP enables RawIP mode on both the modem (WDA) and the kernel interface / 启用RawIP模式：同时在Modem(WDA)和内核接口上启用
func (m *Manager) enableRawIP(parent context.Context) error {
	if m.enableRawIPHook != nil {
		return m.enableRawIPHook(parent)
	}
	wda := m.wda
	if wda == nil {
		return fmt.Errorf("WDA service not available")
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return err
	}

	ifname := m.cfg.Device.NetInterface
	if runtime.GOOS == "linux" {
		sysfsPath := filepath.Join("/sys/class/net", ifname, "qmi/raw_ip")
		supported, err := ensureKernelRawIP(ifname, sysfsPath, rawIPKernelOps{
			readFile:  os.ReadFile,
			writeFile: os.WriteFile,
			bringDown: netcfg.BringDown,
		})
		if err != nil {
			return fmt.Errorf("configure kernel raw_ip: %w", err)
		}
		if !supported {
			m.log.Warn("Kernel driver does not support raw_ip (sysfs entry missing), skipping kernel config")
		} else {
			m.log.Info("Kernel raw_ip is enabled and verified")
		}
	}

	statusCtx, statusCancel := contextWithMaxTimeout(parent, m.cfg.Timeouts.StatusCheck)
	defer statusCancel()
	if currentFormat, err := wda.GetDataFormat(statusCtx); err == nil {
		if currentFormat.LinkProtocol == qmi.LinkProtocolIP {
			m.log.Info("Modem Raw IP mode already enabled")
			return nil
		}
	} else {
		m.log.WithError(err).Debug("Failed to get current data format, configuring Raw IP")
	}

	m.log.Info("Setting modem data format to Raw IP...")
	format := qmi.DataFormat{
		LinkProtocol:      qmi.LinkProtocolIP,
		UlDataAggregation: uint32(qmi.DataFormatUlDataAggDisabled),
		DlDataAggregation: uint32(qmi.DataFormatDlDataAggDisabled),
	}
	setCtx, setCancel := contextWithMaxTimeout(parent, m.cfg.Timeouts.StatusCheck)
	defer setCancel()
	if err := wda.SetDataFormat(setCtx, format); err != nil {
		return fmt.Errorf("set modem data format to Raw IP: %w", err)
	}
	m.log.Info("Modem data format set to Raw IP")
	return nil
}

func (m *Manager) checkSIM() error {
	ctx, cancel := m.opContext(m.cfg.Timeouts.SIMCheck)
	defer cancel()
	return m.checkSIMContext(ctx)
}

func (m *Manager) checkSIMContext(parent context.Context) error {
	if m != nil && m.checkSIMContextHook != nil {
		return m.checkSIMContextHook(parent)
	}
	if m != nil && m.checkSIMHook != nil {
		return m.checkSIMHook()
	}
	status := qmi.SIMAbsent
	var err error
	ctx, cancel := contextWithMaxTimeout(parent, m.cfg.Timeouts.SIMCheck)
	defer cancel()

	// Try UIM service first (modern modems) / 优先尝试UIM服务 (现代modem)
	if m.uim != nil {
		status, err = m.uim.GetCardStatus(ctx)
		if err == nil {
			m.log.Infof("SIM status (UIM): %s", status)
		}
	}

	// Fallback to DMS if UIM failed or not ready / 如果UIM失败或未就绪，回退到DMS
	if err != nil || status != qmi.SIMReady {
		dmsStatus, dmsErr := withDMSRecoveryValue(m, "checkSIM.GetSIMStatus", func(dms *qmi.DMSService) (qmi.SIMStatus, error) {
			return dms.GetSIMStatus(ctx)
		})
		if dmsErr == nil {
			status = dmsStatus
			m.log.Infof("SIM status (DMS): %s", status)
		} else if err == nil {
			err = dmsErr
		}
	}

	if err != nil {
		return err
	}

	if status == qmi.SIMPINRequired && m.cfg.PINCode != "" {
		m.log.Info("Verifying PIN...")
		if err := m.withDMSRecovery("checkSIM.VerifyPIN", func(dms *qmi.DMSService) error {
			return dms.VerifyPIN(ctx, m.cfg.PINCode)
		}); err != nil {
			return fmt.Errorf("PIN verification failed: %w", err)
		}
		m.log.Info("PIN verified successfully")
	}

	return nil
}

func (m *Manager) cleanup() {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	cleanupCtx, cancel := context.WithTimeout(context.Background(), m.cfg.Timeouts.Stop)
	defer cancel()
	m.cleanupLocked(cleanupCtx)
}

func (m *Manager) cleanupLocked(cleanupCtx context.Context) {
	// Use timeout context for cleanup operations / 使用超时上下文进行清理操作
	m.stopScheduledTimers()
	if cleanupCtx == nil {
		cleanupCtx = context.Background()
	}

	m.mu.Lock()
	wds := m.wds
	wdsV6 := m.wdsV6
	nas := m.nas
	dms := m.dms
	uim := m.uim
	wda := m.wda
	wms := m.wms
	ims := m.ims
	imsa := m.imsa
	imsp := m.imsp
	voice := m.voice
	client := m.client
	handleV4 := m.handleV4
	handleV6 := m.handleV6
	pendingDataCalls := append([]pendingDataCall(nil), m.pendingDataCalls...)
	m.pendingDataCalls = nil
	ifname := m.cfg.Device.NetInterface

	muxIface := m.muxIface
	muxID := m.cfg.MuxID
	masterIface := m.cfg.Device.NetInterface

	m.wds = nil
	m.wdsV6 = nil
	m.nas = nil
	m.dms = nil
	m.uim = nil
	m.wda = nil
	m.rawIPConfigured.Store(false)
	m.wms = nil
	m.ims = nil
	m.imsa = nil
	m.imsp = nil
	m.voice = nil
	m.retireListenerBindingLocked(client)
	m.client = nil
	m.handleV4 = 0
	m.handleV6 = 0
	m.settings = nil
	m.muxIface = ""
	m.markControlNotReadyLocked("cleanup")
	m.markCoreNotReadyLocked("cleanup", nil)
	m.setCorePhaseLocked(m.corePhase, "cleanup", m.coreStatusReason, nil)
	m.publishCoreStatusLocked()
	m.wmsTransportStatus = 0
	m.wmsTransportKnown = false
	m.wmsTransportUnsupported = false
	m.wmsTransportQueryError = ""
	m.wmsLastTransportWarn = ""
	m.wmsLastTransportWarnAt = time.Time{}
	m.wmsSMSCValue = ""
	m.wmsSMSCAvailable = false
	m.wmsSMSCKnown = false
	m.wmsSMSCStale = false
	m.wmsSMSCUpdatedAt = time.Time{}
	m.wmsSMSCLastCheckAt = time.Time{}
	m.wmsSMSCRefreshPending = false
	m.wmsRoutesKnown = false
	m.wmsLastNASRegistered = false
	m.wmsLastNASRegisteredKnown = false
	m.wmsReadinessRefreshTicket++
	m.wmsReadinessRefreshPending = false
	m.mu.Unlock()

	cleanupTasks := make([]cleanupTask, 0, 4+len(pendingDataCalls))

	if muxIface != "" && muxID > 0 {
		cleanupTasks = append(cleanupTasks, cleanupTask{
			name: "qmap",
			run: func(context.Context) error {
				return errors.Join(
					netcfg.FlushAddresses(muxIface),
					netcfg.BringDown(muxIface),
					netcfg.DelQMAPMux(masterIface, muxID),
				)
			},
		})
	}

	// Disconnect data call with timeout / 带超时断开数据呼叫
	if handleV4 != 0 && wds != nil {
		cleanupTasks = append(cleanupTasks, cleanupTask{
			name: "stop-wds-v4",
			run: func(ctx context.Context) error {
				if m.stopWDSForCleanup != nil {
					return m.stopWDSForCleanup(ctx, wds, handleV4)
				}
				return wds.StopNetworkInterface(ctx, handleV4)
			},
		})
	}
	if handleV6 != 0 && wdsV6 != nil {
		cleanupTasks = append(cleanupTasks, cleanupTask{
			name: "stop-wds-v6",
			run: func(ctx context.Context) error {
				if m.stopWDSForCleanup != nil {
					return m.stopWDSForCleanup(ctx, wdsV6, handleV6)
				}
				return wdsV6.StopNetworkInterface(ctx, handleV6)
			},
		})
	}
	for i, pending := range pendingDataCalls {
		pending := pending
		if pending.wds == nil || pending.handle == 0 {
			continue
		}
		if (pending.family == qmi.IpFamilyV4 && pending.wds == wds && pending.handle == handleV4) ||
			(pending.family == qmi.IpFamilyV6 && pending.wds == wdsV6 && pending.handle == handleV6) {
			continue
		}
		cleanupTasks = append(cleanupTasks, cleanupTask{
			name: fmt.Sprintf("stop-pending-wds-%d-%08x-%d", pending.family, pending.handle, i),
			run: func(ctx context.Context) error {
				if m.stopWDSForCleanup != nil {
					return m.stopWDSForCleanup(ctx, pending.wds, pending.handle)
				}
				return pending.wds.StopNetworkInterface(ctx, pending.handle)
			},
		})
	}

	if ifname != "" {
		cleanupTasks = append(cleanupTasks, cleanupTask{
			name: "netcfg",
			run: func(context.Context) error {
				return errors.Join(
					netcfg.FlushAddresses(ifname),
					netcfg.BringDown(ifname),
				)
			},
		})
	}

	runCleanupTasks(cleanupCtx, m.log, cleanupTasks)

	// Release clients / 释放客户端
	if wds != nil {
		if m.closeWDSService != nil {
			_ = m.closeWDSService(wds)
		} else {
			_ = wds.Close()
		}
	}
	if wdsV6 != nil {
		if m.closeWDSService != nil {
			_ = m.closeWDSService(wdsV6)
		} else {
			_ = wdsV6.Close()
		}
	}
	if nas != nil {
		nas.Close()
	}
	if dms != nil {
		dms.Close()
	}
	if uim != nil {
		uim.Close()
	}
	if wda != nil {
		wda.Close()
	}
	if wms != nil {
		wms.Close()
	}
	if ims != nil {
		ims.Close()
	}
	if imsa != nil {
		imsa.Close()
	}
	if imsp != nil {
		imsp.Close()
	}
	if voice != nil {
		voice.Close()
	}

	if client != nil {
		_ = m.closeQMIClient(client)
	}
}

type cleanupTask struct {
	name string
	run  func(context.Context) error
}

type cleanupTaskResult struct {
	name string
	err  error
}

func runCleanupTasks(ctx context.Context, log Logger, tasks []cleanupTask) []cleanupTaskResult {
	if len(tasks) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if log == nil {
		log = NewNopLogger()
	}

	resultCh := make(chan cleanupTaskResult, len(tasks))
	pending := make(map[string]struct{}, len(tasks))
	var workers sync.WaitGroup
	workers.Add(len(tasks))
	for i, task := range tasks {
		if task.name == "" {
			task.name = fmt.Sprintf("task-%d", i)
		}
		pending[task.name] = struct{}{}
		go func(task cleanupTask) {
			defer workers.Done()
			if task.run == nil {
				resultCh <- cleanupTaskResult{name: task.name}
				return
			}
			resultCh <- cleanupTaskResult{name: task.name, err: task.run(ctx)}
		}(task)
	}

	results := make([]cleanupTaskResult, 0, len(tasks))
	timedOut := make(map[string]error, len(tasks))
	ctxDone := ctx.Done()
	for len(pending) > 0 {
		select {
		case result := <-resultCh:
			if _, ok := pending[result.name]; !ok {
				continue
			}
			delete(pending, result.name)
			if timeoutErr, ok := timedOut[result.name]; ok {
				result.err = timeoutErr
			}
			results = append(results, result)
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				log.Warnf("Cleanup task %s failed: %v", result.name, result.err)
			}
		case <-ctxDone:
			err := ctx.Err()
			for name := range pending {
				timedOut[name] = err
				log.Warnf("Cleanup task %s timed out: %v", name, err)
			}
			// Cancellation is a request for each task to unwind, not permission to
			// orphan it. Keep draining and join every worker before transport Close.
			ctxDone = nil
		}
	}
	workers.Wait()
	return results
}

// ============================================================================
// Event Loop / 事件循环
// ============================================================================

func (m *Manager) eventLoop(runCtx context.Context) {
	defer m.wg.Done()

	checkTimer := time.NewTimer(jitteredFullCheckInterval(m.cfg.HealthPolicy.FullCheckInterval))
	defer checkTimer.Stop()

	for {
		select {
		case <-runCtx.Done():
			return

		case envelope := <-m.eventCh:
			m.consumeQueuedReconnect(envelope)
			m.handleInternalEventEnvelope(envelope)
			m.promotePendingReconnect()

		case <-checkTimer.C:
			if m.enqueueCurrentInternalEvent(eventCheckFull) == internalEventQueueFull {
				m.log.Warn("Internal event queue is full while scheduling full status check")
			}
			checkTimer.Reset(jitteredFullCheckInterval(m.cfg.HealthPolicy.FullCheckInterval))
		}
	}
}

func (m *Manager) handleEvent(evt internalEvent) {
	if m == nil {
		return
	}
	m.mu.RLock()
	generation := m.coreGeneration.Load()
	m.mu.RUnlock()
	m.handleEventForGeneration(evt, generation)
}

func (m *Manager) handleInternalEventEnvelope(envelope internalEventEnvelope) {
	if m == nil || envelope.generation == 0 {
		return
	}
	m.mu.RLock()
	runCtx := m.ctx
	active := !m.stopped &&
		m.state != StateStopping &&
		m.coreGeneration.Load() == envelope.generation &&
		runCtx != nil &&
		runCtx.Err() == nil
	m.mu.RUnlock()
	if !active {
		return
	}
	m.handleEventForGeneration(envelope.kind, envelope.generation)
}

func (m *Manager) handleEventForGeneration(evt internalEvent, generation uint64) {
	switch evt {
	case eventStart:
		_ = m.doConnectForGeneration(generation)

	case eventStop:
		if err := m.doDisconnect(); err != nil {
			m.log.WithError(err).Warn("Data disconnect event completed with cleanup errors")
		}

	case eventCheckFull:
		m.doStatusCheck(true)

	case eventCheckTargeted:
		m.doStatusCheck(false)

	case eventPacketStatusChanged, eventServingSystemChanged:
		m.log.Debug("Received indication - scheduling status check")
		m.scheduleTargetedCheck()

	case eventModemReset, eventCoreRecovery:
		m.handleRecoveryEventForGeneration(evt, generation)
	}
}

func (m *Manager) handleModemResetEvent() {
	m.handleRecoveryEvent(eventModemReset)
}

func (m *Manager) handleRecoveryEvent(event internalEvent) {
	if m == nil {
		return
	}
	m.handleRecoveryEventForGeneration(event, m.coreGeneration.Load())
}

func (m *Manager) handleRecoveryEventForGeneration(event internalEvent, eventGeneration uint64) {
	if m == nil {
		return
	}

	request, ok := m.beginRecoveryForGeneration(event, eventGeneration)
	if !ok {
		return
	}
	m.clearTimeoutStormCandidates()

	entry := m.coreRecoveryLogger().
		WithField("recovery_reason", request.reason).
		WithField("recovery_detail", request.detail)
	if request.retry {
		entry.Warn("Retrying core recovery")
	} else if request.kind == recoveryKindModemReset {
		entry.Warn("Modem reset detected")
	} else {
		entry.Warn("Core recovery requested")
	}

	result := m.doRecoverCoreAttempt(request)
	emitTerminal := false
	if result.terminalReason != "" && !result.finished {
		emitTerminal = m.commitHardRecoveryTerminal(&result)
	} else if !result.finished {
		// Finish/promote is committed before publishing the per-attempt failure,
		// so observers never see a terminal ordering followed by hidden work.
		m.finishRecovery()
	}
	if !result.recovered && result.generation != 0 {
		generation := result.generation
		if m.emitEventForGeneration(Event{
			Type:   EventCoreRecoveryFailed,
			State:  StateDisconnected,
			Reason: string(request.reason),
		}, generation) {
			m.coreRecoveryFailures.Add(1)
		}
	}
	// A terminal event is the final recovery event for this attempt. Consumers
	// may safely tear down their recovery state as soon as they observe it.
	if emitTerminal {
		m.emitRecoveryTerminal(result)
	}
}

func (m *Manager) runOpenGuard(ctx context.Context, attempt OpenAttempt) error {
	if m.cfg.OpenGuard == nil {
		return nil
	}
	if err := m.cfg.OpenGuard(ctx, attempt); err != nil {
		return fmt.Errorf(
			"qmi open guard rejected %s phase for %s open attempt %d: %w",
			attempt.Phase, attempt.Reason, attempt.Attempt, err,
		)
	}
	return nil
}

func (m *Manager) openQMIClient(ctx context.Context) (*qmi.Client, error) {
	if m.openQMIClientHook != nil {
		return m.openQMIClientHook(ctx, m.cfg.Device.ControlPath, m.cfg.ClientOptions)
	}
	return qmi.NewClientWithOptions(ctx, m.cfg.Device.ControlPath, m.cfg.ClientOptions)
}

func (m *Manager) closeQMIClient(client *qmi.Client) error {
	if client == nil {
		return nil
	}
	if m.closeQMIClientHook != nil {
		return m.closeQMIClientHook(client)
	}
	return client.Close()
}

// rollbackClientAllocationAttempt detaches every service that could have been
// published by allocateServices for client, then releases service client IDs
// before closing the transport. The caller owns lifecycle serialization and
// allocateServices has already joined its startup tasks.
func (m *Manager) rollbackClientAllocationAttempt(client *qmi.Client) {
	if client == nil {
		return
	}

	m.mu.Lock()
	if m.client != client {
		m.mu.Unlock()
		_ = m.closeQMIClient(client)
		return
	}
	wds := m.wds
	wdsV6 := m.wdsV6
	nas := m.nas
	dms := m.dms
	uim := m.uim
	wda := m.wda
	wms := m.wms
	ims := m.ims
	imsa := m.imsa
	imsp := m.imsp
	voice := m.voice
	m.wds = nil
	m.wdsV6 = nil
	m.nas = nil
	m.dms = nil
	m.uim = nil
	m.wda = nil
	m.wms = nil
	m.ims = nil
	m.imsa = nil
	m.imsp = nil
	m.voice = nil
	m.retireListenerBindingLocked(client)
	m.client = nil
	m.handleV4 = 0
	m.handleV6 = 0
	m.settings = nil
	m.rawIPConfigured.Store(false)
	m.mu.Unlock()

	timeout := m.cfg.Timeouts.Stop
	if timeout <= 0 {
		timeout = defaultTimeouts.Stop
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), timeout)
	defer cleanupCancel()
	if wds != nil {
		_ = m.closeTemporaryWDSService(cleanupCtx, wds)
	}
	if wdsV6 != nil {
		_ = m.closeTemporaryWDSService(cleanupCtx, wdsV6)
	}
	if nas != nil {
		_ = nas.Close()
	}
	if dms != nil {
		_ = dms.Close()
	}
	if uim != nil {
		_ = uim.Close()
	}
	if wda != nil {
		_ = wda.Close()
	}
	if wms != nil {
		_ = wms.Close()
	}
	if ims != nil {
		_ = ims.Close()
	}
	if imsa != nil {
		_ = imsa.Close()
	}
	if imsp != nil {
		_ = imsp.Close()
	}
	if voice != nil {
		_ = voice.Close()
	}
	_ = m.closeQMIClient(client)
}

func (m *Manager) openClientAndAllocateServices(ctx context.Context, reason OpenReason) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if reason == "" {
		reason = OpenReasonInitial
	}
	const maxAttempts = 4
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		initCtx, cancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Init)
		openAttempt := OpenAttempt{
			Phase:      OpenPhaseBefore,
			Reason:     reason,
			Attempt:    attempt,
			DevicePath: m.cfg.Device.ControlPath,
			UseProxy:   m.cfg.ClientOptions.UseProxy,
		}
		if err := m.runOpenGuard(initCtx, openAttempt); err != nil {
			cancel()
			return err
		}

		client, err := m.openQMIClient(initCtx)
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("failed to open QMI device: %w", err)
		} else {
			openAttempt.UseProxy = client.UsesProxy()
			openAttempt.Phase = OpenPhaseAfter
			if guardErr := m.runOpenGuard(initCtx, openAttempt); guardErr != nil {
				_ = m.closeQMIClient(client)
				cancel()
				return guardErr
			}
			if err := initCtx.Err(); err != nil {
				_ = m.closeQMIClient(client)
				cancel()
				return err
			}

			m.mu.Lock()
			m.client = client
			m.publishListenerBindingLocked(client, m.coreGeneration.Load(), m.ctx)
			m.mu.Unlock()

			err = m.allocateServices(initCtx)
			if err == nil {
				err = initCtx.Err()
			}
			if err == nil {
				cancel()
				return nil
			}
			lastErr = err

			m.rollbackClientAllocationAttempt(client)
			cancel()
		}

		var timeoutErr *qmi.TimeoutError
		shouldRetry := errors.As(lastErr, &timeoutErr) || errors.Is(lastErr, context.DeadlineExceeded)
		if !shouldRetry || attempt == maxAttempts {
			return lastErr
		}

		delay := time.Duration(attempt) * 2 * time.Second
		m.log.WithError(lastErr).Warnf("QMI init failed, retrying in %v (%d/%d)", delay, attempt, maxAttempts)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}

	return lastErr
}

func (m *Manager) doRecoverFromModemReset() bool {
	request := modemResetRecoveryRequest("direct")
	m.mu.RLock()
	request.generation = m.coreGeneration.Load()
	m.mu.RUnlock()
	result := m.doRecoverCoreAttempt(request)
	if result.terminalReason != "" {
		committed := result.finished
		if !committed {
			committed = m.commitScheduledRecoveryTerminal(
				result.generation, result.terminalReason, result.terminalErr,
			)
		}
		if committed {
			m.emitRecoveryTerminal(result)
		}
	}
	return result.recovered
}

type recoveryAttemptResult struct {
	recovered      bool
	finished       bool
	generation     uint64
	terminalReason string
	terminalErr    error
}

// commitHardRecoveryTerminal atomically retires all pending recovery work for
// an exhausted/device-removed attempt. A hard terminal event is emitted only
// after this commit, so no pending reset or preserved retry can be promoted
// after observers have been told recovery is terminal.
func (m *Manager) commitHardRecoveryTerminal(result *recoveryAttemptResult) bool {
	if m == nil || result == nil || result.finished || result.terminalReason == "" || result.generation == 0 {
		return false
	}
	m.mu.Lock()
	runCtx := m.ctx
	active := !m.stopped &&
		m.state != StateStopping &&
		m.coreGeneration.Load() == result.generation &&
		runCtx != nil &&
		runCtx.Err() == nil
	if !active {
		m.mu.Unlock()
		return false
	}
	m.modemResetMu.Lock()
	if !m.modemResetRecovering || m.recoveryGeneration != result.generation {
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		return false
	}
	m.clearRecoveryStateLocked()
	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
	}
	m.setCorePhaseLocked(CorePhaseTerminal, "recovery_terminal", result.terminalReason, result.terminalErr)
	m.publishCoreStatusLocked()
	result.finished = true
	m.modemResetMu.Unlock()
	m.mu.Unlock()
	return true
}

func (m *Manager) emitRecoveryTerminal(result recoveryAttemptResult) bool {
	if result.terminalReason == "" || result.generation == 0 {
		return false
	}
	return m.emitEventForGeneration(Event{
		Type:   EventRecoveryExhausted,
		State:  StateDisconnected,
		Error:  result.terminalErr,
		Reason: result.terminalReason,
	}, result.generation)
}

func (m *Manager) doRecoverCore(request recoveryRequest) bool {
	return m.doRecoverCoreAttempt(request).recovered
}

func (m *Manager) doRecoverCoreAttempt(request recoveryRequest) recoveryAttemptResult {
	recoveryCtx, recoveryCancel := m.opContext(0)
	defer recoveryCancel()

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	return m.doRecoverCoreLocked(recoveryCtx, request)
}

func (m *Manager) doRecoverCoreLocked(ctx context.Context, request recoveryRequest) (result recoveryAttemptResult) {
	if ctx == nil || ctx.Err() != nil {
		return result
	}

	// Avoid retiring live timers for an already stale request, then drain the
	// exact active epoch before advancing core ownership. The second validation
	// closes the race with Stop while the drain was in progress.
	m.mu.RLock()
	currentGeneration := m.coreGeneration.Load()
	preflight := ctx.Err() == nil &&
		!m.stopped &&
		m.state != StateStopping &&
		request.generation != 0 &&
		currentGeneration != 0 &&
		request.generation == currentGeneration
	m.mu.RUnlock()
	if !preflight {
		return result
	}

	m.pauseAndDrainScheduledTimers()
	m.mu.Lock()
	currentGeneration = m.coreGeneration.Load()
	if ctx.Err() != nil ||
		m.stopped ||
		m.state == StateStopping ||
		request.generation == 0 ||
		currentGeneration == 0 ||
		request.generation != currentGeneration {
		m.mu.Unlock()
		m.resumeScheduledTimersLocked()
		return result
	}
	desiredConnection := m.desiredConnection
	m.reconnectPending = false
	m.reconnectGeneration = 0
	m.retireListenerBindingLocked(m.client)
	generation := m.coreGeneration.Add(1)
	request.generation = generation
	result.generation = generation
	// Advancing the authoritative generation and withdrawing readiness are one
	// state commit. Observers can never see a fresh generation advertised as
	// ready while it still owns the retired client and services.
	m.markControlNotReadyLocked("recover_generation_advanced")
	m.markCoreNotReadyLocked("recover_generation_advanced", nil)
	// A reset can arrive after beginRecovery marks the current recovery active
	// but before this session generation is advanced. Move the active recovery
	// bookkeeping to the new generation while m.mu excludes new enqueues, so a
	// real pending reset is not discarded as stale by finishRecovery.
	m.modemResetMu.Lock()
	if m.modemResetRecovering && m.recoveryGeneration == currentGeneration {
		m.recoveryGeneration = generation
		m.currentRecoveryRequest = request
	}
	m.modemResetMu.Unlock()
	m.coreStatusLastErr = ""
	m.setCorePhaseLocked(CorePhaseRecovering, "recover_generation_advanced", string(request.reason), nil)
	m.publishCoreStatusLocked()

	m.mu.Unlock()
	m.resumeScheduledTimersLocked()

	m.recoverAttempts.Add(1)

	disconnectCtx, disconnectCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Stop)
	if disconnectErr := m.doDisconnectLocked(disconnectCtx); disconnectErr != nil {
		m.log.WithError(disconnectErr).Warn("Data disconnect was incomplete before core recovery cleanup")
	}
	disconnectCancel()

	cleanupCtx, cleanupCancel := contextWithMaxTimeout(ctx, m.cfg.Timeouts.Stop)
	m.cleanupLocked(cleanupCtx)
	cleanupCancel()
	m.snapshot.Reset()

	openErr := error(nil)
	if m.openClientAndAllocateServicesHook != nil {
		openErr = m.openClientAndAllocateServicesHook(ctx)
	} else {
		openErr = m.openClientAndAllocateServices(ctx, OpenReasonRecovery)
	}
	if openErr != nil {
		m.log.WithError(openErr).Warn("Failed to reinitialize QMI core during recovery")
		m.mu.Lock()
		m.markControlNotReadyLocked("recover_reinit_services")
		m.markCoreNotReadyLocked("recover_reinit_services", openErr)
		isStopping := m.state == StateStopping
		if !isStopping && m.state != StateDisconnected {
			m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
			m.state = StateDisconnected
		}
		m.setCorePhaseLocked(CorePhaseRecovering, "recover_reinit_services", string(request.reason), openErr)
		m.publishCoreStatusLocked()
		m.mu.Unlock()
		if ctx.Err() != nil || isStopping {
			return result
		}
		if m.isControlDeviceGone() {
			m.log.WithError(openErr).Warn("Control device node missing; terminating recovery retries")
			m.mu.Lock()
			m.recoverCount = 0
			m.recoverFirstFailAt = time.Time{}
			m.mu.Unlock()
			result.terminalReason, result.terminalErr = "device_removed", openErr
			return result
		}
		retryResult := m.scheduleRecoverRetryFor(request, "reinit_failed")
		if retryResult.terminalReason != "" {
			result.terminalReason = retryResult.terminalReason
		}
		return result
	}
	m.mu.RLock()
	isStopping := m.state == StateStopping
	m.mu.RUnlock()
	if ctx.Err() != nil || isStopping {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), m.cfg.Timeouts.Stop)
		m.cleanupLocked(cleanupCtx)
		cleanupCancel()
		m.setState(StateDisconnected)
		return result
	}

	m.mu.Lock()
	m.markControlReadyLocked("recover_control_ready")
	m.setCorePhaseLocked(CorePhaseRecovering, "recover_control_ready", string(request.reason), nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()

	checkSIMErr := m.checkSIMContext(ctx)
	if checkSIMErr != nil {
		m.log.WithError(checkSIMErr).Warn("SIM check failed after core recovery")
	}

	m.mu.Lock()
	m.markCoreNotReadyLocked("recover_wait_reset_quiet", nil)
	m.setCorePhaseLocked(CorePhaseRecovering, "recover_wait_reset_quiet", string(request.reason), nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	quietCtx, quietCancel := contextWithMaxTimeout(ctx, m.modemResetQuietWindow+time.Second)
	quietErr := m.waitResetQuietWindow(quietCtx)
	quietCancel()
	if quietErr != nil {
		m.mu.Lock()
		m.markCoreNotReadyLocked("recover_wait_reset_quiet", quietErr)
		m.setCorePhaseLocked(CorePhaseRecovering, "recover_wait_reset_quiet", string(request.reason), quietErr)
		m.publishCoreStatusLocked()
		m.mu.Unlock()
		m.log.WithError(quietErr).Warn("QMI reset quiet-window gate not satisfied")
		m.mu.RLock()
		stopping := m.state == StateStopping
		m.mu.RUnlock()
		if ctx.Err() != nil || stopping {
			return result
		}
		if !m.hasPendingModemReset() {
			retryResult := m.scheduleRecoverRetryFor(request, "quiet_window")
			if retryResult.terminalReason != "" {
				result.terminalReason = retryResult.terminalReason
			}
		}
		return result
	}

	m.mu.Lock()
	m.markCoreNotReadyLocked("recover_wait_identity", nil)
	m.setCorePhaseLocked(CorePhaseRecovering, "recover_wait_identity", string(request.reason), nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	identityTimeout := m.cfg.Timeouts.SIMCheck
	if identityTimeout <= 0 {
		identityTimeout = defaultTimeouts.SIMCheck
	}
	identityCtx, identityCancel := contextWithMaxTimeout(ctx, identityTimeout)
	identityErr := m.waitIdentityReadable(identityCtx)
	identityCancel()
	if identityErr != nil {
		m.log.WithError(identityErr).Warn("QMI identity gate not satisfied after reset recovery (core recovery proceeds anyway)")
	}

	if m.beforeRecoveryCommitHook != nil {
		m.beforeRecoveryCommitHook()
	}

	m.mu.Lock()
	m.modemResetMu.Lock()
	pendingReset := m.modemResetRecovering &&
		m.recoveryGeneration == generation &&
		m.modemResetPending
	staleRecovery := m.modemResetRecovering &&
		m.recoveryGeneration != generation
	if ctx.Err() != nil ||
		m.stopped ||
		m.state == StateStopping ||
		m.coreGeneration.Load() != generation ||
		staleRecovery {
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		return result
	}
	if m.currentListenerTerminalLocked(generation) {
		terminalBinding := m.listenerBinding
		terminalErr := terminalBinding.err()
		if terminalErr == nil {
			terminalErr = errQMIClientEventStreamClosed
		}
		// Retire the failed stream in the same state transaction that rejects
		// readiness. The blocked listener can no longer enqueue transport_down
		// after this attempt schedules backoff or commits Terminal.
		m.retireListenerBindingLocked(terminalBinding.client)

		m.markControlNotReadyLocked("recover_transport_terminal")
		m.markCoreNotReadyLocked("recover_transport_terminal", terminalErr)
		m.setCorePhaseLocked(CorePhaseRecovering, "recover_transport_terminal", string(request.reason), terminalErr)
		m.publishCoreStatusLocked()
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		retryResult := m.scheduleRecoverRetryFor(request, "transport_terminal")
		if retryResult.terminalReason != "" {
			result.terminalReason = retryResult.terminalReason
			result.terminalErr = terminalErr
		}
		return result
	}
	if pendingReset {
		event, queued := m.finishRecoveryStateLocked(generation)
		if queued {
			m.setCorePhaseLocked(CorePhaseRecovering, "recovery_follow_up_queued", string(request.reason), nil)
		} else {
			m.setCorePhaseLocked(CorePhaseDegraded, "recovery_pending_reset", string(request.reason), nil)
		}
		m.publishCoreStatusLocked()
		result.finished = true
		m.modemResetMu.Unlock()
		m.mu.Unlock()
		if queued {
			m.signalRecoveryEvent(event, generation)
		}
		return result
	}
	m.recoverCount = 0
	m.recoverFirstFailAt = time.Time{}
	m.markCoreReadyLocked("recover_converged")
	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
	}
	m.recoverBackoffMs.Store(0)
	m.recoverSuccess.Add(1)
	m.coreRecoverySuccess.Add(1)
	nextEvent, followUpQueued := m.finishRecoveryStateLocked(generation)
	m.coreStatusLastErr = ""
	if followUpQueued {
		m.setCorePhaseLocked(CorePhaseRecovering, "recovery_follow_up_queued", string(request.reason), nil)
	} else {
		m.setCorePhaseLocked(CorePhaseReady, "recover_converged", string(request.reason), nil)
	}
	m.publishCoreStatusLocked()
	result.finished = true
	m.modemResetMu.Unlock()
	m.mu.Unlock()
	if m.events != nil {
		m.events.Emit(Event{
			Type: EventCoreRecoverySucceeded, Generation: generation,
			State: StateDisconnected, Reason: string(request.reason),
		})
	}

	if followUpQueued {
		m.signalRecoveryEvent(nextEvent, generation)
	}
	if desiredConnection && m.cfg.AutoReconnect {
		m.scheduleAfter(2*time.Second, func() {
			if m.enqueueReconnectEvent(generation) == internalEventQueueFull {
				m.log.Warn("Internal event queue is full while scheduling recovery reconnect")
			}
		})
	} else {
		m.log.Info("QMI core recovered without reconnecting data plane")
	}

	result.recovered = true
	return result
}

func (m *Manager) hasPendingModemReset() bool {
	m.modemResetMu.Lock()
	defer m.modemResetMu.Unlock()
	return m.modemResetPending
}

func (m *Manager) waitResetQuietWindow(ctx context.Context) error {
	window := m.modemResetQuietWindow
	if window <= 0 {
		window = defaultModemResetQuietWindow
	}
	deadline := time.Now().Add(window)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if m.hasPendingModemReset() {
			return fmt.Errorf("detected coalesced modem reset during quiet window")
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("quiet window wait timed out: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m *Manager) waitIdentityReadable(ctx context.Context) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	tryProbe := func() bool {
		probeCtx, cancel := contextWithMaxTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := probeCtx.Err(); err != nil {
			lastErr = err
			return false
		}

		// Bound the first probe to half of the available attempt so a slow
		// ICCID path cannot consume IMSI's entire deadline. The IMSI probe
		// receives all remaining time and is skipped after ICCID succeeds,
		// avoiding duplicate service-recovery side effects.
		iccidBudget := time.Second
		if deadline, ok := probeCtx.Deadline(); ok {
			iccidBudget = time.Until(deadline) / 2
		}
		if iccidBudget <= 0 {
			lastErr = probeCtx.Err()
			return false
		}
		iccidCtx, iccidCancel := contextWithMaxTimeout(probeCtx, iccidBudget)
		iccid, iccidErr := m.GetICCIDStrictLive(iccidCtx)
		iccidCancel()
		if iccidErr == nil && strings.TrimSpace(iccid) != "" {
			return true
		}
		if iccidErr != nil {
			lastErr = fmt.Errorf("read ICCID failed: %w", iccidErr)
		} else {
			lastErr = fmt.Errorf("read ICCID returned an empty identity")
		}
		if err := probeCtx.Err(); err != nil {
			lastErr = err
			return false
		}

		imsiCtx, imsiCancel := contextWithMaxTimeout(probeCtx, 2*time.Second)
		imsi, imsiErr := m.GetIMSIStrictLive(imsiCtx)
		imsiCancel()
		if imsiErr == nil && strings.TrimSpace(imsi) != "" {
			return true
		}
		if imsiErr != nil {
			lastErr = fmt.Errorf("read IMSI failed: %w", imsiErr)
		} else {
			lastErr = fmt.Errorf("read IMSI returned an empty identity")
		}
		return false
	}
	if tryProbe() {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return fmt.Errorf("identity not readable before deadline: %w", lastErr)
		case <-ticker.C:
			if tryProbe() {
				return nil
			}
		}
	}
}

// isControlDeviceGone 仅在配置了控制口路径且该节点确实不存在时返回 true。
func (m *Manager) isControlDeviceGone() bool {
	path := m.cfg.Device.ControlPath
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true
	}
	return false
}

// recoveryExhausted 判断核心恢复是否应放弃重试（仅在策略启用时返回 true）。
func (m *Manager) recoveryExhausted() bool {
	p := m.cfg.RecoveryPolicy
	if p.MaxRecoverAttempts > 0 && m.recoverCount > p.MaxRecoverAttempts {
		return true
	}
	if p.MaxRecoverElapsed > 0 && !m.recoverFirstFailAt.IsZero() &&
		time.Since(m.recoverFirstFailAt) >= p.MaxRecoverElapsed {
		return true
	}
	return false
}

type recoveryRetryResult struct {
	terminalReason string
	generation     uint64
	terminalCommit bool
}

func (m *Manager) scheduleRecoverRetry(reason string) {
	request := m.currentRecovery()
	if request.reason == "" {
		request = explicitRecoveryRequest("recover_retry")
	}
	if request.generation == 0 {
		m.mu.RLock()
		request.generation = m.coreGeneration.Load()
		m.mu.RUnlock()
	}
	result := m.scheduleRecoverRetryForOptions(request, reason, true)
	if result.terminalReason != "" && result.terminalCommit {
		if result.generation != 0 {
			m.emitEventForGeneration(Event{
				Type:   EventRecoveryExhausted,
				State:  StateDisconnected,
				Reason: result.terminalReason,
			}, result.generation)
		}
	}
}

func (m *Manager) commitScheduledRecoveryTerminal(generation uint64, reason string, terminalErr error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitScheduledRecoveryTerminalLocked(generation, reason, terminalErr)
}

// commitScheduledRecoveryTerminalLocked requires m.mu.
func (m *Manager) commitScheduledRecoveryTerminalLocked(generation uint64, reason string, terminalErr error) bool {
	runCtx := m.ctx
	if generation == 0 || m.stopped || m.state == StateStopping ||
		m.coreGeneration.Load() != generation || runCtx == nil || runCtx.Err() != nil {
		return false
	}
	m.modemResetMu.Lock()
	m.clearRecoveryStateLocked()
	m.modemResetMu.Unlock()
	m.markCoreNotReadyLocked("recovery_terminal", nil)
	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
	}
	m.setCorePhaseLocked(CorePhaseTerminal, "recovery_terminal", reason, terminalErr)
	m.publishCoreStatusLocked()
	return true
}

func (m *Manager) scheduleRecoverRetryFor(request recoveryRequest, reason string) recoveryRetryResult {
	return m.scheduleRecoverRetryForOptions(request, reason, false)
}

func (m *Manager) scheduleRecoverRetryForOptions(request recoveryRequest, reason string, commitTerminal bool) recoveryRetryResult {
	m.mu.Lock()
	generation := m.coreGeneration.Load()
	runCtx := m.ctx
	active := !m.stopped && m.state != StateStopping && generation != 0 && runCtx != nil && runCtx.Err() == nil
	strictGeneration := request.generation == generation
	if !active || !strictGeneration {
		m.mu.Unlock()
		return recoveryRetryResult{generation: generation}
	}

	m.recoverCount++
	if m.recoverFirstFailAt.IsZero() {
		m.recoverFirstFailAt = time.Now()
	}
	if m.recoveryExhausted() {
		attempts := m.recoverCount
		m.recoverCount = 0
		m.recoverFirstFailAt = time.Time{}
		terminalCommitted := false
		if commitTerminal {
			terminalCommitted = m.commitScheduledRecoveryTerminalLocked(generation, "recovery_exhausted", nil)
		}
		m.mu.Unlock()
		m.log.WithField("reason", reason).
			WithField("recovery_reason", request.reason).
			WithField("attempts", attempts).
			Warn("Core recovery exhausted; stopping retries")
		return recoveryRetryResult{
			terminalReason: "recovery_exhausted",
			generation:     generation,
			terminalCommit: terminalCommitted,
		}
	}
	delay := m.getRecoverDelay()
	attempt := m.recoverCount
	m.setCorePhaseLocked(CorePhaseRecovering, "recovery_backoff", reason, nil)
	m.publishCoreStatusLocked()
	m.mu.Unlock()
	m.log.
		WithField("reason", reason).
		WithField("recovery_reason", request.reason).
		Infof("Will retry core recovery with backoff in %v (attempt=%d)", delay, attempt)
	m.scheduleAfter(delay, func() {
		m.enqueueCoreRecoveryRetry(request, reason)
	})
	return recoveryRetryResult{generation: generation}
}

func (m *Manager) getRecoverDelay() time.Duration {
	attempt := m.recoverCount
	if attempt <= 0 {
		attempt = 1
	}

	delay := recoverBackoffBase
	for i := 1; i < attempt; i++ {
		if delay >= recoverBackoffMax {
			delay = recoverBackoffMax
			break
		}
		delay *= 2
		if delay > recoverBackoffMax {
			delay = recoverBackoffMax
			break
		}
	}

	// add jitter in range [-20%, +20%]
	jitterRange := time.Duration(float64(delay) * recoverBackoffJitterRatio)
	if jitterRange > 0 {
		jitter := time.Duration(rand.Int63n(int64(jitterRange)*2+1)) - jitterRange
		delay += jitter
	}
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}

	m.recoverBackoffMs.Store(uint64(delay / time.Millisecond))
	return delay
}

// jitteredFullCheckInterval 在 base 基础上加 ±fullCheckJitterRatio 的随机抖动。
// base<=0 时回退到 defaultHealthPolicy.FullCheckInterval。
func jitteredFullCheckInterval(base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultHealthPolicy.FullCheckInterval
	}
	jitterRange := time.Duration(float64(base) * fullCheckJitterRatio)
	if jitterRange <= 0 {
		return base
	}
	jitter := time.Duration(rand.Int63n(int64(jitterRange)*2+1)) - jitterRange
	result := base + jitter
	if result <= 0 {
		return base
	}
	return result
}

func (m *Manager) doConnect() error {
	return m.doConnectForGeneration(m.coreGeneration.Load())
}

func (m *Manager) doConnectForGeneration(generation uint64) error {
	dialCtx, cancelDial := m.opContext(m.cfg.Timeouts.Dial)
	defer cancelDial()

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.RLock()
	stopped := m.stopped
	m.mu.RUnlock()
	if stopped {
		return ErrManagerStopped
	}
	return m.doConnectLocked(dialCtx, generation)
}

func (m *Manager) doConnectLocked(dialCtx context.Context, generation uint64) error {
	if dialCtx == nil || dialCtx.Err() != nil {
		return context.Canceled
	}
	m.connectMu.Lock()
	defer m.connectMu.Unlock()

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrManagerStopped
	}
	if m.state == StateStopping || dialCtx.Err() != nil {
		m.mu.Unlock()
		return context.Canceled
	}
	token := coreSessionToken{generation: generation, client: m.client, runCtx: m.ctx}
	if !m.coreSessionCurrentLocked(token) {
		m.mu.Unlock()
		return ErrServiceNotReady("qmi-core")
	}
	if !m.desiredConnection {
		m.mu.Unlock()
		return nil
	}
	dataReady := (m.cfg.EnableIPv4 && m.handleV4 != 0) || (!m.cfg.EnableIPv4 && m.cfg.EnableIPv6 && m.handleV6 != 0)
	if m.state == StateConnected && dataReady {
		m.mu.Unlock()
		return nil
	}
	if m.state == StateConnecting && dataReady {
		m.mu.Unlock()
		return fmt.Errorf("connection already in progress")
	}
	if m.state != StateConnecting {
		m.log.Infof("State: %s -> %s", m.state, StateConnecting)
		m.state = StateConnecting
		m.publishCoreStatusLocked()
	}
	m.mu.Unlock()

	if m.useSIMCOMNDIS() {
		return m.doSIMCOMNDISConnect(dialCtx, generation)
	}

	if err := m.ensureDataPlaneServices(dialCtx); err != nil {
		if dialCtx.Err() != nil || !m.coreSessionCurrent(token) {
			return ErrServiceNotReady("qmi-core")
		}
		m.handleDialFailure(err, generation)
		return err
	}
	if !m.coreSessionCurrent(token) {
		return ErrServiceNotReady("qmi-core")
	}

	if m.wds == nil && m.wdsV6 == nil {
		err := fmt.Errorf("wds service not available")
		m.log.Error("WDS service not available")
		m.handleDialFailure(err, generation)
		return err
	}

	// ========== 多路拨号 (QMAP) 准备 ==========
	if m.cfg.MuxID > 0 {
		masterIface := m.cfg.Device.NetInterface
		m.log.Infof("多路拨号模式: MuxID=%d, ProfileIndex=%d, 物理网卡=%s",
			m.cfg.MuxID, m.cfg.ProfileIndex, masterIface)

		// 1. 创建 QMAP 虚拟网卡 (如果不存在)
		muxIfname, err := netcfg.AddQMAPMux(masterIface, m.cfg.MuxID)
		if err != nil {
			m.log.WithError(err).Errorf("创建 MUX ID=%d 虚拟网卡失败", m.cfg.MuxID)
			// 继续尝试，也许用户已手动创建
		} else {
			m.log.Infof("QMAP 虚拟网卡: %s (MuxID=%d)", muxIfname, m.cfg.MuxID)
			m.mu.Lock()
			m.muxIface = muxIfname
			m.mu.Unlock()
		}

		// 2. 绑定 WDS Client 到 Mux Data Port
		binding := qmi.MuxBinding{
			EpType:     0x02, // HSUSB
			EpIfID:     0x04, // 默认 Interface ID
			MuxID:      m.cfg.MuxID,
			ClientType: 1, // Tethered
		}
		if m.wds != nil {
			ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
			if err := m.wds.BindMuxDataPort(ctx, binding); err != nil {
				m.log.WithError(err).Error("WDS IPv4 BindMuxDataPort 失败")
				// 非致命，继续
			} else {
				m.log.Infof("WDS IPv4 已绑定 MuxID=%d", m.cfg.MuxID)
			}
			cancel()
		}

		// 如果有 IPv6 WDS，也需要绑定
		if m.wdsV6 != nil {
			ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
			if err := m.wdsV6.BindMuxDataPort(ctx, binding); err != nil {
				m.log.WithError(err).Warn("WDS IPv6 BindMuxDataPort 失败")
			} else {
				m.log.Infof("WDS IPv6 已绑定 MuxID=%d", m.cfg.MuxID)
			}
			cancel()
		}
	}

	// 设置 ProfileIndex (多路模式和非多路模式都可用)
	if m.cfg.ProfileIndex > 0 {
		if m.wds != nil {
			m.wds.ProfileIndex = m.cfg.ProfileIndex
		}
		if m.wdsV6 != nil {
			m.wdsV6.ProfileIndex = m.cfg.ProfileIndex
		}
		m.log.Infof("使用 Profile Index=%d", m.cfg.ProfileIndex)
	}

	// Log current signal and registration for context / 记录当前信号和注册状态以便调试
	if sig, err := m.getSignalStrength(dialCtx); err == nil {
		m.emitSignalUpdate(sig)
		if sig != nil {
			m.log.Infof("Signal: RSSI=%d, RSRP=%d, RSRQ=%d", sig.RSSI, sig.RSRP, sig.RSRQ)
		}
	}
	if ss, err := m.getServingSystem(dialCtx); err == nil {
		if ss != nil {
			m.log.Infof("Network: %s (%d-%d) Tech:%d", ss.RegistrationState, ss.MCC, ss.MNC, ss.RadioInterface)
		}
	}

	// Check registration / 检查注册状态
	if registered, regErr := m.queryNASRegisteredWithContext(dialCtx); regErr == nil {
		if !registered {
			m.log.Info("Waiting for network registration...")
			// Don't fail - continue and let the dial fail if not registered / 不报错 - 继续执行，让拨号过程去处理未注册的情况
		}
	} else {
		m.log.WithError(regErr).Debug("Failed to query registration state before dialing")
	}

	if err := m.dialMissingDataFamilies(dialCtx, token); err != nil {
		m.log.WithError(err).Error("Data call dial failed")
		m.handleDialFailure(err, generation)
		return err
	}

	// Get IP settings and configure interface / 获取IP设置并配置接口
	if err := m.configureNetwork(); err != nil {
		m.log.WithError(err).Error("Network configuration failed")
		m.handleDialFailure(err, generation)
		return err
	}

	if err := m.commitConnected(dialCtx, generation, nil, 0, false); err != nil {
		return err
	}
	m.log.Info("Connection established successfully!")
	return nil
}

func (m *Manager) commitConnected(
	ctx context.Context,
	generation uint64,
	settings *qmi.RuntimeSettings,
	handleV4 uint32,
	setSession bool,
) error {
	if m.beforeConnectCommitHook != nil {
		m.beforeConnectCommitHook()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.state == StateStopping {
		return ErrManagerStopped
	}
	if ctx == nil ||
		ctx.Err() != nil ||
		generation == 0 ||
		m.coreGeneration.Load() != generation ||
		!m.desiredConnection {
		return context.Canceled
	}
	if setSession {
		m.handleV4 = handleV4
		m.settings = settings
	}
	if settings == nil {
		settings = m.settings
	}
	if m.state != StateConnected {
		m.log.Infof("State: %s -> %s", m.state, StateConnected)
		m.state = StateConnected
		m.publishCoreStatusLocked()
	}
	m.retryCount = 0
	if m.events != nil {
		m.events.Emit(Event{
			Type:       EventConnected,
			Generation: generation,
			State:      StateConnected,
			Settings:   settings,
		})
	}
	return nil
}

func (m *Manager) configureNetwork() error {
	if m.configureNetworkHook != nil {
		return m.configureNetworkHook()
	}
	// 多路拨号模式下，IP/DNS/Route 配置在虚拟网卡上
	ifname := m.cfg.Device.NetInterface
	m.mu.RLock()
	if m.muxIface != "" {
		ifname = m.muxIface
	}
	m.mu.RUnlock()
	m.log.Infof("Configuring network interface %s...", ifname)

	// 多路拨号时也要确保物理网卡是 up 的
	if m.muxIface != "" && ifname != m.cfg.Device.NetInterface {
		if err := netcfg.BringUp(m.cfg.Device.NetInterface); err != nil {
			m.log.WithError(err).Warn("Failed to bring master interface up")
		}
	}

	// Bring interface up / 启动接口
	if err := netcfg.BringUp(ifname); err != nil {
		m.log.WithError(err).Warn("Failed to bring interface up")
	}

	// 1. IPv4 Configuration / 1. IPv4配置
	if m.wds != nil {
		m.log.Debug("Querying IPv4 runtime settings...")
		ctx, cancel := m.opContext(m.cfg.Timeouts.StatusCheck)
		settings, err := m.wds.GetRuntimeSettings(ctx, qmi.IpFamilyV4)
		cancel()
		if err != nil {
			m.log.WithError(err).Warn("Failed to get IPv4 settings")
		} else {
			m.mu.Lock()
			m.settings = settings
			m.mu.Unlock()

			if settings.IPv4Address != nil {
				prefix, _ := settings.IPv4Subnet.Size()
				if prefix == 0 {
					prefix = 32
				}
				m.log.Infof("Configuring IPv4: %s/%d via %s (DNS: %v, %v)",
					settings.IPv4Address, prefix, settings.IPv4Gateway,
					settings.IPv4DNS1, settings.IPv4DNS2)

				if err := netcfg.SetIPAddress(ifname, settings.IPv4Address, prefix); err != nil {
					m.log.WithError(err).Error("Failed to set IPv4 address")
				}

				// Add default route (unless disabled) / 添加默认路由 (除非被禁用)
				if !m.cfg.NoRoute {
					if settings.IPv4Gateway != nil && !settings.IPv4Gateway.Equal(net.IPv4zero) {
						m.log.Infof("Adding IPv4 route via %s", settings.IPv4Gateway)
						if err := netcfg.AddDefaultRoute(ifname, settings.IPv4Gateway); err != nil {
							m.log.WithError(err).Error("Failed to add IPv4 default route")
						}
					} else {
						m.log.Info("Adding direct IPv4 default route")
						netcfg.AddDefaultRouteDirect(ifname, false)
					}
				} else {
					m.log.Info("Skipping default route (--no-route)")
				}

				if !m.cfg.NoDNS {
					dns1 := ""
					dns2 := ""
					if settings.IPv4DNS1 != nil {
						dns1 = settings.IPv4DNS1.String()
					}
					if settings.IPv4DNS2 != nil {
						dns2 = settings.IPv4DNS2.String()
					}
					if dns1 != "" {
						m.log.Infof("Configuring DNS: %s, %s", dns1, dns2)
						netcfg.UpdateResolvConf(dns1, dns2)
					}
				} else {
					m.log.Info("Skipping DNS configuration (--no-dns)")
				}

				// Set MTU
				if settings.MTU > 0 {
					m.log.Infof("Setting MTU: %d", settings.MTU)
					netcfg.SetMTU(ifname, int(settings.MTU))
				}
			}
		}
	}

	// 2. IPv6 Configuration / 2. IPv6配置
	if m.wdsV6 != nil {
		m.log.Debug("Querying IPv6 runtime settings...")
		ctx, cancel := m.opContext(m.cfg.Timeouts.StatusCheck)
		settingsV6, err := m.wdsV6.GetRuntimeSettings(ctx, qmi.IpFamilyV6)
		cancel()
		if err != nil {
			m.log.WithError(err).Warn("Failed to get IPv6 settings")
		} else {
			// Merge IPv6 fields into m.settings so Settings() exposes both
			// families regardless of whether the IPv4 leg is active.
			// IPv6需要合并进m.settings，这样无论IPv4是否启用，Settings()都能返回双栈信息。
			m.mu.Lock()
			if m.settings == nil {
				m.settings = &qmi.RuntimeSettings{}
			}
			m.settings.IPv6Address = settingsV6.IPv6Address
			m.settings.IPv6Prefix = settingsV6.IPv6Prefix
			m.settings.IPv6Gateway = settingsV6.IPv6Gateway
			m.settings.IPv6DNS1 = settingsV6.IPv6DNS1
			m.settings.IPv6DNS2 = settingsV6.IPv6DNS2
			if m.settings.MTU == 0 {
				m.settings.MTU = settingsV6.MTU
			}
			m.mu.Unlock()

			if settingsV6.IPv6Address != nil {
				m.log.Infof("Configuring IPv6: %s/%d", settingsV6.IPv6Address, settingsV6.IPv6Prefix)
				if err := netcfg.SetIPv6Address(ifname, settingsV6.IPv6Address, int(settingsV6.IPv6Prefix)); err != nil {
					m.log.WithError(err).Error("Failed to set IPv6 address")
				}
				if !m.cfg.NoRoute {
					if settingsV6.IPv6Gateway != nil {
						m.log.Infof("Adding IPv6 route via %s", settingsV6.IPv6Gateway)
						netcfg.AddDefaultRoute(ifname, settingsV6.IPv6Gateway)
					} else {
						m.log.Info("Adding direct IPv6 default route")
						netcfg.AddDefaultRouteDirect(ifname, true)
					}
				}
			}
		}
	}

	// Final check: ensure up / 最后检查: 确保接口已启动
	netcfg.BringUp(ifname)
	m.log.Info("Network configuration completed")
	return nil
}

func (m *Manager) doDisconnect() error {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Stop)
	defer cancel()

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	return m.doDisconnectLocked(ctx)
}

func (m *Manager) doDisconnectLocked(ctx context.Context) error {
	m.log.Info("Disconnecting...")
	var disconnectErrs []error

	m.mu.RLock()
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	m.mu.RUnlock()
	commitDisconnected := func() {
		publish := false
		m.mu.Lock()
		sameCore := token.generation != 0 &&
			m.coreGeneration.Load() == token.generation &&
			m.client == token.client &&
			m.ctx == token.runCtx &&
			!m.stopped &&
			m.state != StateStopping
		if sameCore {
			m.settings = nil
			if m.state != StateDisconnected {
				m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
				m.state = StateDisconnected
				m.publishCoreStatusLocked()
			}
			publish = m.coreSessionCurrentLocked(token)
		}
		m.mu.Unlock()
		if publish {
			m.emitEventForGeneration(Event{Type: EventDisconnected, State: StateDisconnected}, token.generation)
		}
	}

	if m.useSIMCOMNDIS() {
		m.doSIMCOMNDISDisconnect(ctx)
		commitDisconnected()
		return nil
	}

	m.mu.RLock()
	wds, wdsV6 := m.wds, m.wdsV6
	handleV4, handleV6 := m.handleV4, m.handleV6
	ifname := strings.TrimSpace(m.muxIface)
	if ifname == "" {
		ifname = strings.TrimSpace(m.cfg.Device.NetInterface)
	}
	pending := make([]pendingDataCall, 0, len(m.pendingDataCalls))
	for _, call := range m.pendingDataCalls {
		if call.generation == token.generation {
			pending = append(pending, call)
		}
	}
	m.mu.RUnlock()

	active := make([]pendingDataCall, 0, 2)
	if handleV4 != 0 && wds != nil {
		active = append(active, pendingDataCall{generation: token.generation, wds: wds, family: qmi.IpFamilyV4, handle: handleV4})
	}
	if handleV6 != 0 && wdsV6 != nil {
		active = append(active, pendingDataCall{generation: token.generation, wds: wdsV6, family: qmi.IpFamilyV6, handle: handleV6})
	}

	for _, call := range active {
		stopErr := m.stopDataCall(ctx, call.wds, call.handle)
		m.mu.Lock()
		if m.coreGeneration.Load() == call.generation {
			switch call.family {
			case qmi.IpFamilyV4:
				if m.wds == call.wds && m.handleV4 == call.handle {
					if stopErr != nil {
						m.moveActiveDataCallToPendingLocked(call.generation, call.wds, call.family, call.handle)
					} else {
						m.handleV4 = 0
						m.removePendingDataCallLocked(call.generation, call.wds, call.family, call.handle)
					}
				}
			case qmi.IpFamilyV6:
				if m.wdsV6 == call.wds && m.handleV6 == call.handle {
					if stopErr != nil {
						m.moveActiveDataCallToPendingLocked(call.generation, call.wds, call.family, call.handle)
					} else {
						m.handleV6 = 0
						m.removePendingDataCallLocked(call.generation, call.wds, call.family, call.handle)
					}
				}
			}
		}
		m.mu.Unlock()
		if stopErr != nil {
			m.log.WithError(stopErr).Warnf("Failed to stop active data call 0x%08x", call.handle)
			disconnectErrs = append(disconnectErrs, fmt.Errorf("stop active data call 0x%08x: %w", call.handle, stopErr))
		}
	}

	for _, call := range pending {
		duplicateActive := false
		for _, current := range active {
			if pendingDataCallEqual(call, current.generation, current.wds, current.family, current.handle) {
				duplicateActive = true
				break
			}
		}
		if duplicateActive {
			continue
		}
		stopErr := m.stopDataCall(ctx, call.wds, call.handle)
		if stopErr == nil {
			m.mu.Lock()
			m.removePendingDataCallLocked(call.generation, call.wds, call.family, call.handle)
			m.mu.Unlock()
		} else {
			m.log.WithError(stopErr).Warnf("Failed to release pending data call 0x%08x", call.handle)
			disconnectErrs = append(disconnectErrs, fmt.Errorf("release pending data call 0x%08x: %w", call.handle, stopErr))
		}
	}

	if ifname != "" {
		var hostErr error
		if m.disconnectHostCleanupHook != nil {
			hostErr = m.disconnectHostCleanupHook(ifname)
		} else {
			hostErr = errors.Join(
				netcfg.FlushAddresses(ifname),
				netcfg.FlushRoutes(ifname),
				netcfg.BringDown(ifname),
			)
		}
		if hostErr != nil {
			m.log.WithError(hostErr).Warn("Failed to clean disconnected host data plane")
			disconnectErrs = append(disconnectErrs, fmt.Errorf("clean disconnected host data plane: %w", hostErr))
		}
	}

	commitDisconnected()
	return errors.Join(disconnectErrs...)
}

func (m *Manager) doStatusCheck(full bool) {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	m.mu.RLock()
	if m.isRotating {
		m.mu.RUnlock()
		return // Skip check during rotation / 轮换期间跳过检查
	}
	currentState := m.state
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	generation := token.generation
	m.mu.RUnlock()

	if currentState == StateStopping || currentState == StateDisconnected {
		return
	}

	if token.client == nil {
		return
	}

	m.statusChecks.Add(1)
	ctx, cancel := m.opContext(m.cfg.Timeouts.StatusCheck)
	defer cancel()

	// 1. Log Signal Strength & Registration / 1. 记录信号强度和注册状态
	if full {
		sig, err := m.getSignalStrength(ctx)
		if err == nil {
			m.emitSignalUpdate(sig)
			if sig != nil {
				m.log.Infof("Signal: RSSI=%d, RSRP=%d, RSRQ=%d", sig.RSSI, sig.RSRP, sig.RSRQ)
			}
		}
		ss, err := m.getServingSystem(ctx)
		if err == nil {
			if ss != nil {
				// 周期主动查询也回填快照，避免仅依赖 indication 导致状态陈旧。
				m.snapshot.updateServingFromQuery(ss)
				m.log.Infof("Network: %s (MCC:%d MNC:%d) Tech:%d", ss.RegistrationState, ss.MCC, ss.MNC, ss.RadioInterface)
			}
		}
	}

	if m.useSIMCOMNDIS() {
		m.doSIMCOMNDISStatusCheck(ctx, currentState, generation)
		return
	}

	// 2. Query connection status / 2. 查询连接状态
	status, statusFamily, err := m.getPacketServiceState(ctx)
	if err != nil {
		m.log.WithError(err).Debug("Status query failed")
		return
	}

	if status == qmi.StatusConnected {
		if currentState != StateConnected {
			m.log.Info("Connection restored")
			m.configureNetwork()
			_ = m.commitConnected(ctx, generation, nil, 0, false)
		} else {
			// Smart Check: Verify IP consistency (match C version) / 智能检查: 验证IP一致性 (匹配C版本逻辑)
			shouldVerifyIP := full
			if !shouldVerifyIP && m.cfg.HealthPolicy.IPConsistencyInterval > 0 {
				m.mu.RLock()
				lastIPCheck := m.lastIPCheck
				m.mu.RUnlock()
				shouldVerifyIP = time.Since(lastIPCheck) >= m.cfg.HealthPolicy.IPConsistencyInterval
			}
			if shouldVerifyIP {
				if err := m.verifyIPConsistency(); err != nil {
					m.log.WithError(err).Warn("IP consistency check failed - triggering redial")
					disconnectCtx, disconnectCancel := m.opContext(m.cfg.Timeouts.Stop)
					if disconnectErr := m.doDisconnectLocked(disconnectCtx); disconnectErr != nil {
						m.log.WithError(disconnectErr).Warn("Data disconnect was incomplete after IP consistency failure")
					}
					disconnectCancel()
					if m.emitReconnectingForSessionIfDesired(token, err) {
						_ = m.enqueueReconnectEvent(generation)
					}
				} else {
					m.mu.Lock()
					m.lastIPCheck = time.Now()
					m.mu.Unlock()
				}
			}
		}
	} else if status == qmi.StatusDisconnected {
		if currentState == StateConnected {
			m.log.Warn("Connection lost!")
			m.mu.Lock()
			if !m.coreSessionCurrentLocked(token) {
				m.mu.Unlock()
				return
			}
			switch statusFamily {
			case qmi.IpFamilyV6:
				m.handleV6 = 0
			default:
				m.handleV4 = 0
			}
			m.settings = nil
			m.state = StateDisconnected
			m.publishCoreStatusLocked()
			m.mu.Unlock()

			if flushErr := m.flushRotationAddresses(m.dataInterfaceName()); flushErr != nil {
				m.log.WithError(flushErr).Warn("Failed to flush host data plane after packet service disconnect")
			}
			m.emitEventForGeneration(Event{Type: EventDisconnected, State: StateDisconnected}, generation)
			if m.emitReconnectingForSessionIfDesired(token, nil) {
				_ = m.enqueueReconnectEvent(generation)
			}
		}
	}
}

func (m *Manager) verifyIPConsistency() error {
	if m.wds == nil || m.settings == nil {
		return nil
	}

	// Get fresh settings from modem / 从 modem获取最新设置
	ctx, cancel := m.opContext(m.cfg.Timeouts.StatusCheck)
	defer cancel()
	newSettings, err := m.wds.GetRuntimeSettings(ctx, qmi.IpFamilyV4)
	if err != nil {
		return err
	}

	// Compare with recorded IP / 与记录的IP进行比较
	if !newSettings.IPv4Address.Equal(m.settings.IPv4Address) {
		return fmt.Errorf("local IP %s != modem IP %s", m.settings.IPv4Address, newSettings.IPv4Address)
	}

	return nil
}

func (m *Manager) handleDialFailure(err error, generation uint64) {
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
	if m.state != StateDisconnected {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
		m.publishCoreStatusLocked()
	}
	desiredConnection := m.desiredConnection
	if m.events != nil {
		m.events.Emit(Event{
			Type:       EventDialFailed,
			Generation: generation,
			State:      StateDisconnected,
			Error:      err,
		})
	}
	if !m.cfg.AutoReconnect || !desiredConnection {
		m.mu.Unlock()
		return
	}
	token := coreSessionToken{generation: generation, client: m.client, runCtx: runCtx}

	delay := m.getRetryDelay()
	m.retryCount++
	radioReset := m.retryCount == m.cfg.RetryPolicy.RadioResetAfter
	if m.events != nil {
		m.events.Emit(Event{
			Type:       EventReconnecting,
			Generation: generation,
			State:      StateDisconnected,
			Error:      err,
		})
	}
	m.log.Infof("Will retry in %v (%d/%d)", delay, m.retryCount, len(m.retryDelays))
	m.reconnectScheduled.Add(1)
	m.mu.Unlock()

	if radioReset {
		// Dial failure handling runs inside doConnectLocked, which already owns
		// lifecycleMu and connectMu. Perform the reset synchronously against the
		// exact failed session instead of spawning an unowned lifecycle bypass.
		resetCtx, resetCancel := contextWithMaxTimeout(token.runCtx, m.cfg.Timeouts.Stop)
		if _, resetErr := m.radioPowerCycleForSession(resetCtx, token, "handleDialFailure.RadioPowerCycle"); resetErr != nil {
			m.log.WithError(resetErr).Warn("Radio reset after dial failures did not complete")
		}
		resetCancel()
	}
	m.scheduleAfter(delay, func() {
		if m.enqueueReconnectEvent(generation) == internalEventQueueFull {
			m.log.Warn("Internal event queue is full while scheduling reconnect")
		}
	})
}

func (m *Manager) getRetryDelay() time.Duration {
	if m.retryCount < len(m.retryDelays) {
		return m.retryDelays[m.retryCount]
	}
	return m.retryDelays[len(m.retryDelays)-1]
}

// ============================================================================
// Indication Handler
// ============================================================================

func (m *Manager) indicationHandler(runCtx context.Context) {
	defer m.wg.Done()

	for {
		binding, changed := m.listenerBindingSnapshot()
		if binding == nil {
			if !waitForListenerChange(runCtx, nil, changed) {
				return
			}
			continue
		}

		if binding.terminal() {
			m.reportListenerTerminal(binding)
			if !waitForListenerChange(runCtx, binding, changed) {
				return
			}
			continue
		}

		select {
		case <-runCtx.Done():
			return
		case <-binding.runCtx.Done():
			return
		case <-binding.retired:
		case <-changed:
		case <-binding.done:
			m.reportListenerTerminal(binding)
			if !waitForListenerChange(runCtx, binding, changed) {
				return
			}
		case evt, ok := <-binding.events:
			if !ok {
				m.reportListenerTerminal(binding)
				if !waitForListenerChange(runCtx, binding, changed) {
					return
				}
				continue
			}
			if evt.Type == qmi.EventModemReset {
				m.handleModemResetForBinding(binding, evt)
				continue
			}
			m.queueListenerIndication(runCtx, binding, evt)
		}
	}
}

func (m *Manager) handleIndicationForBinding(binding *listenerBinding, evt qmi.Event) {
	if m == nil || binding == nil {
		return
	}

	// A modem reset is a recovery input, not an ordinary indication side
	// effect. It must remain observable while recovery owns lifecycleMu and is
	// waiting for its reset quiet window. Exact binding validation still makes
	// stale or replaced streams harmless.
	if evt.Type == qmi.EventModemReset {
		m.handleModemResetForBinding(binding, evt)
		return
	}

	// Core replacement is serialized by lifecycleMu. Holding it across the
	// handler makes validation and all snapshot/event side effects one exact
	// listener-binding operation. The receive loop dispatches ordinary events
	// through a bounded worker, so waiting here never hides a later modem reset.
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.indicationDispatchMu.RLock()
	defer m.indicationDispatchMu.RUnlock()
	m.mu.RLock()
	active := m.listenerBindingUsableLocked(binding)
	m.mu.RUnlock()
	if active {
		m.handleIndication(evt)
	}
}

func (m *Manager) handleModemResetForBinding(binding *listenerBinding, evt qmi.Event) {
	if shouldLogRawIndication(evt) {
		m.log.Debugf("Indication: type=%d service=0x%02x msg=0x%04x", evt.Type, evt.ServiceID, evt.MessageID)
	}
	event := m.qmiIndicationEvent(EventModemReset, evt)
	m.enqueueModemResetEventForBinding("qmi_indication", binding, &event)
}

func (m *Manager) handleIndication(evt qmi.Event) {
	if shouldLogRawIndication(evt) {
		m.log.Debugf("Indication: type=%d service=0x%02x msg=0x%04x", evt.Type, evt.ServiceID, evt.MessageID)
	}
	generation := m.coreGeneration.Load()

	switch evt.Type {
	case qmi.EventPacketServiceStatusChanged:
		event := m.qmiIndicationEvent(EventPacketServiceStatusChanged, evt)
		if evt.Packet != nil {
			status, err := qmi.ParsePacketServiceStatusIndication(evt.Packet)
			if err != nil {
				m.log.WithError(err).Warn("Failed to parse WDS packet service status indication")
			} else {
				event.PacketServiceStatus = status
			}
		}
		m.emitEvent(event)
		_ = m.enqueueInternalEvent(eventPacketStatusChanged, generation)

	case qmi.EventServingSystemChanged:
		event := m.qmiIndicationEvent(EventServingSystemChanged, evt)
		var previousServing *qmi.ServingSystem
		if evt.Packet != nil {
			// SysInfoInd 独立拆出补充网络动态快照
			if evt.MessageID == qmi.NASSysInfoInd {
				if sysInfo, err := qmi.ParseSysInfoIndication(evt.Packet); err == nil {
					m.snapshot.updateSysInfo(sysInfo)
				}
			}

			hasServingTLV := qmi.FindTLV(evt.Packet.TLVs, 0x01) != nil
			hasPLMNTLV := qmi.FindTLV(evt.Packet.TLVs, 0x12) != nil
			if hasServingTLV || hasPLMNTLV {
				info, err := qmi.ParseServingSystemIndication(evt.Packet)
				if err != nil {
					m.log.WithError(err).Warn("Failed to parse NAS serving system indication")
				} else {
					if hasPLMNTLV && info.MCC == 0 && info.MNC == 0 {
						// 过滤基带偶发的无效 PLMN，防止缓存被冲刷
						hasPLMNTLV = false
					}
					current, _ := m.snapshot.ServingSystem()
					previousServing = current

					isChanged := false
					if current != nil {
						if hasServingTLV {
							if info.RegistrationState != current.RegistrationState {
								isReg1 := info.RegistrationState == qmi.RegStateRegistered || info.RegistrationState == qmi.RegStateRoaming
								isReg2 := current.RegistrationState == qmi.RegStateRegistered || current.RegistrationState == qmi.RegStateRoaming
								if !(isReg1 && isReg2) {
									isChanged = true
								}
							}
							if info.PSAttached != current.PSAttached ||
								info.RadioInterface != current.RadioInterface {
								isChanged = true
							}
							if !hasPLMNTLV {
								info.MCC = current.MCC
								info.MNC = current.MNC
							}
						}
						if hasPLMNTLV {
							// 过滤基带偶尔抽风上报的无效(全0) PLMN，避免误判和冲刷掉真实缓存
							if (info.MCC != 0 || info.MNC != 0) && (info.MCC != current.MCC || info.MNC != current.MNC) {
								isChanged = true
							}
							if !hasServingTLV {
								info.RegistrationState = current.RegistrationState
								info.PSAttached = current.PSAttached
								info.RadioInterface = current.RadioInterface
							}
						}
					} else {
						isChanged = true
						if !hasServingTLV {
							info.RegistrationState = qmi.RegStateUnknown
						}
					}

					if !isChanged {
						return
					}

					m.log.Debugf("NAS serving indication TLVs: has_0x01=%v has_0x12=%v", hasServingTLV, hasPLMNTLV)
					event.ServingSystem = info
					switch {
					case hasServingTLV && hasPLMNTLV:
						m.log.Debug("NAS serving indication merge: apply registration+plmn")
						m.snapshot.updateServingRegistration(info)
						m.snapshot.updateServingPLMN(info.MCC, info.MNC)
					case hasServingTLV:
						m.log.Debug("NAS serving indication merge: apply registration only")
						m.snapshot.updateServingRegistration(info)
					case hasPLMNTLV:
						m.log.Debug("NAS serving indication merge: apply plmn only")
						m.snapshot.updateServingPLMN(info.MCC, info.MNC)
					}
				}
			}
		}
		if event.ServingSystem != nil {
			m.mu.Lock()
			notify := m.regNotify
			m.mu.Unlock()
			if event.ServingSystem.RegistrationState == qmi.RegStateRegistered && notify != nil {
				select {
				case notify <- true:
				default:
				}
			}
			if (event.ServingSystem.RegistrationState == qmi.RegStateRegistered || event.ServingSystem.RegistrationState == qmi.RegStateRoaming) && event.ServingSystem.PSAttached {
				m.maybeRefreshWMSReadiness("serving-system-recovered")
			}
			m.maybeSchedulePostRegRefresh(previousServing, event.ServingSystem, "serving_indication")
		}
		m.emitEvent(event)
		_ = m.enqueueInternalEvent(eventServingSystemChanged, generation)

	case qmi.EventNASOperatorNameChanged:
		event := m.qmiIndicationEvent(EventNASOperatorNameChanged, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			if info, err := qmi.ParseOperatorNameIndication(evt.Packet); err == nil {
				event.NASOperatorName = info
				m.snapshot.updateNASOperatorName(info)
			} else {
				m.log.WithError(err).Warn("Failed to parse NAS operator name indication")
			}
		}
		m.emitEvent(event)

	case qmi.EventNASNetworkTimeChanged:
		event := m.qmiIndicationEvent(EventNASNetworkTimeChanged, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			if info, err := qmi.ParseNetworkTimeIndication(evt.Packet); err == nil {
				event.NASNetworkTime = info
				m.snapshot.updateNASNetworkTime(info)
			} else {
				m.log.WithError(err).Warn("Failed to parse NAS network time indication")
			}
		}
		m.emitEvent(event)

	case qmi.EventNASSignalInfoChanged:
		event := m.qmiIndicationEvent(EventNASSignalInfoChanged, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			if info, err := qmi.ParseSignalInfoIndication(evt.Packet); err == nil {
				event.NASSignalInfo = info
				m.snapshot.updateNASSignalInfo(info)
			} else {
				m.log.WithError(err).Warn("Failed to parse NAS signal info indication")
			}
		}
		m.emitEvent(event)

	case qmi.EventNASNetworkReject:
		event := m.qmiIndicationEvent(EventNASNetworkReject, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			if info, err := qmi.ParseNetworkRejectIndication(evt.Packet); err == nil {
				event.NASNetworkReject = info
				m.snapshot.updateNASNetworkReject(info)
			} else {
				m.log.WithError(err).Warn("Failed to parse NAS network reject indication")
			}
		}
		m.emitEvent(event)

	case qmi.EventNASIncrementalNetworkScan:
		event := m.qmiIndicationEvent(EventNASIncrementalNetworkScan, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			if info, err := qmi.ParseIncrementalNetworkScanIndication(evt.Packet); err == nil {
				event.NASIncrementalNetwork = info
				m.snapshot.updateNASIncrementalScan(info)
			} else {
				m.log.WithError(err).Warn("Failed to parse NAS incremental network scan indication")
			}
		}
		m.emitEvent(event)

	case qmi.EventNASEventReport:
		if isEmptyNASEventReport(evt) {
			return
		}
		event := m.qmiIndicationEvent(EventNASEventReport, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		m.emitEvent(event)

	case qmi.EventModemReset:
		m.emitQMIIndicationEvent(EventModemReset, evt)
		m.enqueueModemResetEvent("qmi_indication")

	case qmi.EventNewMessage:
		m.log.Info("New SMS Indication received")
		if tlv := qmi.FindTLV(evt.Packet.TLVs, 0x10); tlv != nil && len(tlv.Value) >= 5 {
			// 0x10 = MT Message: StorageType (1) + MemoryIndex (4) / 0x10 = MT 消息: 存储类型(1) + 内存索引(4)
			storage := tlv.Value[0]
			index := binary.LittleEndian.Uint32(tlv.Value[1:5])
			m.emitEvent(Event{
				Type:        EventNewSMS,
				State:       m.State(),
				SMSIndex:    index,
				StorageType: storage,
				RawQMIType:  evt.Type,
				ServiceID:   evt.ServiceID,
				MessageID:   evt.MessageID,
			})
		} else if tlv := qmi.FindTLV(evt.Packet.TLVs, 0x11); tlv != nil && len(tlv.Value) >= 6 {
			// 0x11 = Transfer Route MT Message:
			// AckIndicator(1) + TransactionID(4) + Format(1) + RawDataLen(2) + RawData(...)
			// AckIndicator: 0 = send ACK, 1 = do not send ACK.
			ackRequired := tlv.Value[0] == 0
			transactionID := binary.LittleEndian.Uint32(tlv.Value[1:5])
			format := tlv.Value[5]
			pdu := tlv.Value[6:]
			if len(tlv.Value) >= 8 {
				rawLen := int(binary.LittleEndian.Uint16(tlv.Value[6:8]))
				if rawLen <= len(tlv.Value)-8 {
					pdu = tlv.Value[8 : 8+rawLen]
				}
			}
			// Copy PDU to prevent underlying buffer corruption / 拷贝 PDU 防止底层缓冲区损坏
			pduCopy := make([]byte, len(pdu))
			copy(pduCopy, pdu)
			m.emitEvent(Event{
				Type:             EventNewSMSRaw,
				State:            m.State(),
				Pdu:              pduCopy,
				SMSAckRequired:   ackRequired,
				SMSTransactionID: transactionID,
				SMSFormat:        format,
				RawQMIType:       evt.Type,
				ServiceID:        evt.ServiceID,
				MessageID:        evt.MessageID,
			})
		} else {
			// Just emit event without index if TLV missing
			m.emitEvent(Event{
				Type:       EventNewSMS,
				State:      m.State(),
				SMSIndex:   0xFFFFFFFF,
				RawQMIType: evt.Type,
				ServiceID:  evt.ServiceID,
				MessageID:  evt.MessageID,
			})
		}

	case qmi.EventIMSRegistrationStatus:
		info, err := qmi.ParseIMSARegistrationStatusChanged(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse IMSA registration status indication")
			return
		}
		m.emitEvent(Event{
			Type:            EventIMSRegistrationStatus,
			State:           m.State(),
			IMSRegistration: info,
			RawQMIType:      evt.Type,
			ServiceID:       evt.ServiceID,
			MessageID:       evt.MessageID,
		})

	case qmi.EventIMSServicesStatus:
		info, err := qmi.ParseIMSAServicesStatusChanged(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse IMSA services status indication")
			return
		}
		m.emitEvent(Event{
			Type:        EventIMSServicesStatus,
			State:       m.State(),
			IMSServices: info,
			RawQMIType:  evt.Type,
			ServiceID:   evt.ServiceID,
			MessageID:   evt.MessageID,
		})

	case qmi.EventIMSSettingsChanged:
		info, err := qmi.ParseIMSServicesEnabledSetting(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse IMS settings changed indication")
			return
		}
		m.emitEvent(Event{
			Type:        EventIMSSettingsChanged,
			State:       m.State(),
			IMSSettings: info,
			RawQMIType:  evt.Type,
			ServiceID:   evt.ServiceID,
			MessageID:   evt.MessageID,
		})

	case qmi.EventVoiceCallStatus:
		info, err := qmi.ParseVoiceAllCallStatus(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse VOICE call status indication")
			return
		}
		m.emitEvent(Event{
			Type:       EventVoiceCallStatus,
			State:      m.State(),
			VoiceCalls: info,
			RawQMIType: evt.Type,
			ServiceID:  evt.ServiceID,
			MessageID:  evt.MessageID,
		})

	case qmi.EventVoiceSupplementaryService:
		info, err := qmi.ParseVoiceSupplementaryServiceIndication(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse VOICE supplementary service indication")
			return
		}
		m.emitEvent(Event{
			Type:               EventVoiceSupplementaryService,
			State:              m.State(),
			VoiceSupplementary: info,
			RawQMIType:         evt.Type,
			ServiceID:          evt.ServiceID,
			MessageID:          evt.MessageID,
		})

	case qmi.EventVoiceSupplementaryServiceRequest:
		info, err := qmi.ParseVoiceSupplementaryServiceRequestIndication(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse VOICE supplementary service request indication")
			return
		}
		m.emitEvent(Event{
			Type:                      EventVoiceSupplementaryServiceRequest,
			State:                     m.State(),
			VoiceSupplementaryRequest: info,
			RawQMIType:                evt.Type,
			ServiceID:                 evt.ServiceID,
			MessageID:                 evt.MessageID,
		})

	case qmi.EventUSSD:
		info, err := qmi.ParseVoiceUSSDIndication(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse VOICE USSD indication")
			return
		}
		m.emitEvent(Event{
			Type:       EventVoiceUSSD,
			State:      m.State(),
			VoiceUSSD:  info,
			RawQMIType: evt.Type,
			ServiceID:  evt.ServiceID,
			MessageID:  evt.MessageID,
		})

	case qmi.EventVoiceUSSDReleased:
		m.emitEvent(Event{
			Type:       EventVoiceUSSDReleased,
			State:      m.State(),
			RawQMIType: evt.Type,
			ServiceID:  evt.ServiceID,
			MessageID:  evt.MessageID,
		})

	case qmi.EventVoiceUSSDNoWaitResult:
		info, err := qmi.ParseVoiceUSSDNoWaitIndication(evt.Packet)
		if err != nil {
			m.log.WithError(err).Warn("Failed to parse VOICE USSD no-wait indication")
			return
		}
		m.emitEvent(Event{
			Type:            EventVoiceUSSDNoWaitResult,
			State:           m.State(),
			VoiceUSSDNoWait: info,
			RawQMIType:      evt.Type,
			ServiceID:       evt.ServiceID,
			MessageID:       evt.MessageID,
		})

	case qmi.EventSimStatusChanged:
		m.snapshot.ResetIdentities(false)
		m.PreWarmIdentities(false)
		m.emitQMIIndicationEvent(EventSimStatusChanged, evt)

	case qmi.EventUIMSessionClosed:
		m.markWMSReadinessStale()
		event := m.qmiIndicationEvent(EventUIMSessionClosed, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		m.emitEvent(event)

	case qmi.EventUIMRefresh:
		m.snapshot.ResetIdentities(false)
		m.PreWarmIdentities(false)
		event := m.qmiIndicationEvent(EventUIMRefresh, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			info, err := qmi.ParseUIMRefreshIndication(evt.Packet)
			if err != nil {
				m.log.WithError(err).Warn("Failed to parse UIM refresh indication")
			} else {
				m.snapshot.updateUIMRefresh(info)
				event.UIMRefresh = info
			}
		}
		m.emitEvent(event)

	case qmi.EventUIMSlotStatus:
		event := m.qmiIndicationEvent(EventUIMSlotStatus, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		if evt.Packet != nil {
			info, err := qmi.ParseUIMSlotStatusIndication(evt.Packet)
			if err != nil {
				m.log.WithError(err).Warn("Failed to parse UIM slot status indication")
			} else {
				m.snapshot.updateUIMSlotStatus(info)
				event.UIMSlotStatus = info
			}
		}
		m.emitEvent(event)

	case qmi.EventWMSSMSCAddress:
		event := m.qmiIndicationEvent(EventWMSSMSCAddress, evt)
		if evt.Packet != nil {
			info, err := qmi.ParseWMSSMSCAddressIndication(evt.Packet)
			if err != nil {
				m.log.WithError(err).Warn("Failed to parse WMS SMSC address indication")
			} else {
				event.WMSSMSCAddress = info
				digits := strings.TrimSpace(info.Digits)
				known := digits != ""
				now := time.Now()
				m.setWMSSMSCState(digits, known, known, false, now, now)
			}
		}
		m.emitEvent(event)

	case qmi.EventWMSTransportNetworkRegistrationStatus:
		event := m.qmiIndicationEvent(EventWMSTransportNetworkRegistrationStatus, evt)
		if evt.Packet != nil {
			status, err := qmi.ParseWMSTransportNetworkRegistrationStatusIndication(evt.Packet)
			if err != nil {
				m.log.WithError(err).Warn("Failed to parse WMS transport registration status indication")
			} else {
				event.WMSTransportRegistration = status
				m.setWMSTransportState(status, true, false)
			}
		}
		m.emitEvent(event)

	default:
		event := m.qmiIndicationEvent(EventUnknownIndication, evt)
		event.TLVMeta = packetTLVMeta(evt.Packet)
		m.emitEvent(event)
	}
}

func isEmptyNASEventReport(evt qmi.Event) bool {
	return evt.Type == qmi.EventNASEventReport && len(packetTLVMeta(evt.Packet)) == 0
}

func shouldLogRawIndication(evt qmi.Event) bool {
	switch evt.Type {
	case qmi.EventUIMSessionClosed:
		return false
	case qmi.EventNASEventReport:
		return !isEmptyNASEventReport(evt)
	case qmi.EventServingSystemChanged, qmi.EventNASSignalInfoChanged:
		return false
	default:
		return true
	}
}

// ============================================================================
// Radio Reset Recovery
// ============================================================================

// radioPowerCycleForSession performs the QMI operation for a caller that
// already owns lifecycleMu (and, for data paths, connectMu).
type radioPowerCycleResult struct {
	dataInvalidated bool
	hadData         bool
}

func (m *Manager) runRadioPowerCommand(ctx context.Context, token coreSessionToken, on bool, op string) error {
	if m.radioPowerCommandHook != nil {
		return m.radioPowerCommandHook(ctx, token, on, op)
	}
	return m.withDMSRecovery(op, func(dms *qmi.DMSService) error {
		if ctx.Err() != nil || !m.coreSessionCurrent(token) {
			return ErrServiceNotReady("qmi-core")
		}
		return dms.RadioPower(ctx, on)
	})
}

// invalidateDataPlaneAfterRadioOff is the linearization point after a
// successful RadioPower(false). Both modem data calls are gone at this point,
// so old handles and settings must never survive even if power-on later fails.
func (m *Manager) invalidateDataPlaneAfterRadioOff(token coreSessionToken) (hadData bool, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.coreSessionCurrentLocked(token) {
		return false, false
	}
	pendingTerminated := false
	kept := m.pendingDataCalls[:0]
	for _, call := range m.pendingDataCalls {
		if call.generation == token.generation {
			pendingTerminated = true
			continue
		}
		kept = append(kept, call)
	}
	m.pendingDataCalls = kept
	hadData = m.handleV4 != 0 || m.handleV6 != 0 || pendingTerminated || m.settings != nil || m.state == StateConnected || m.state == StateConnecting
	m.handleV4 = 0
	m.handleV6 = 0
	m.settings = nil
	if m.state == StateConnected || m.state == StateConnecting {
		m.log.Infof("State: %s -> %s", m.state, StateDisconnected)
		m.state = StateDisconnected
		m.publishCoreStatusLocked()
	}
	return hadData, true
}

func (m *Manager) radioPowerCycleForSession(ctx context.Context, token coreSessionToken, op string) (result radioPowerCycleResult, err error) {
	if ctx == nil || ctx.Err() != nil || !m.coreSessionCurrent(token) {
		return result, ErrServiceNotReady("qmi-core")
	}

	m.log.Info("Turning radio off...")
	if powerErr := m.runRadioPowerCommand(ctx, token, false, op+".Off"); powerErr != nil {
		return result, fmt.Errorf("failed to turn radio off: %w", powerErr)
	}
	result.hadData, result.dataInvalidated = m.invalidateDataPlaneAfterRadioOff(token)
	if !result.dataInvalidated {
		return result, ErrServiceNotReady("qmi-core")
	}
	if result.hadData {
		if flushErr := m.flushRadioDataPlane(m.dataInterfaceName()); flushErr != nil {
			// RadioPower(false) already invalidated the modem calls. Host cleanup
			// failure must not strand the radio in the off state.
			m.log.WithError(flushErr).Warn("Failed to flush host data plane after radio power-off")
		}
	}
	if !m.coreSessionCurrent(token) {
		return result, ErrServiceNotReady("qmi-core")
	}

	// RadioPower(false) creates a compensation obligation. A caller-specific
	// timeout after that point must not leave an otherwise-current modem powered
	// off, so power-on gets one fresh bounded attempt while the exact core token
	// remains valid. Stop/recovery invalidates the token and forbids that retry.
	runPowerOn := func(onCtx context.Context) error {
		m.log.Info("Turning radio on...")
		if powerErr := m.runRadioPowerCommand(onCtx, token, true, op+".On"); powerErr != nil {
			return fmt.Errorf("failed to turn radio on: %w", powerErr)
		}
		return nil
	}
	freshPowerOn := func() error {
		timeout := m.cfg.Timeouts.Stop
		if timeout <= 0 {
			timeout = defaultTimeouts.Stop
		}
		onCtx, onCancel := context.WithTimeout(context.Background(), timeout)
		defer onCancel()
		if !m.coreSessionCurrent(token) {
			return ErrServiceNotReady("qmi-core")
		}
		return runPowerOn(onCtx)
	}

	if originalErr := ctx.Err(); originalErr != nil {
		onErr := freshPowerOn()
		return result, errors.Join(originalErr, onErr)
	}

	if onErr := runPowerOn(ctx); onErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && m.coreSessionCurrent(token) {
			compensationErr := freshPowerOn()
			return result, errors.Join(ctxErr, onErr, compensationErr)
		}
		return result, onErr
	}
	if !m.coreSessionCurrent(token) {
		return result, ErrServiceNotReady("qmi-core")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	return result, nil
}

func (m *Manager) publishRadioResetInvalidation(token coreSessionToken, result radioPowerCycleResult, resetErr error) {
	if !result.dataInvalidated || !result.hadData {
		return
	}
	m.mu.RLock()
	active := m.coreSessionCurrentLocked(token) && m.handleV4 == 0 && m.handleV6 == 0 && m.state == StateDisconnected
	m.mu.RUnlock()
	if !active {
		return
	}
	m.emitEventForGeneration(Event{Type: EventDisconnected, State: StateDisconnected, Error: resetErr}, token.generation)
	if m.emitReconnectingForSessionIfDesired(token, resetErr) {
		_ = m.enqueueReconnectEvent(token.generation)
	}
}

// RadioReset performs a radio power cycle to recover from stuck states / 射频重置: 执行射频电源循环以从卡死状态恢复
func (m *Manager) RadioReset() error {
	if m == nil {
		return ErrServiceNotReady("qmi-core")
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.connectMu.Lock()
	defer m.connectMu.Unlock()

	m.mu.RLock()
	token := coreSessionToken{generation: m.coreGeneration.Load(), client: m.client, runCtx: m.ctx}
	current := m.coreSessionCurrentLocked(token)
	m.mu.RUnlock()
	if !current {
		return ErrServiceNotReady("qmi-core")
	}

	m.log.Info("Performing radio reset...")
	ctx, cancel := contextWithMaxTimeout(token.runCtx, m.cfg.Timeouts.Stop)
	defer cancel()
	result, resetErr := m.radioPowerCycleForSession(ctx, token, "RadioReset.RadioPowerCycle")
	m.publishRadioResetInvalidation(token, result, resetErr)
	if resetErr != nil {
		return resetErr
	}
	m.log.Info("Radio reset completed")
	return nil
}

// ============================================================================
// SMS Methods / 短信方法
// ============================================================================

// ListSMS lists SMS messages from the specified storage (0=UIM, 1=NV) / ListSMS 从指定的存储中列出短信 (0=UIM, 1=NV)
func (m *Manager) ListSMS(storageType uint8, tag qmi.MessageTagType) ([]struct {
	Index uint32
	Tag   qmi.MessageTagType
}, error) {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
	defer cancel()
	return withWMSRecoveryValue(m, "ListSMS", func(wms *qmi.WMSService) ([]struct {
		Index uint32
		Tag   qmi.MessageTagType
	}, error) {
		return wms.ListMessages(ctx, storageType, tag)
	})
}

// ReadRawSMS reads a raw SMS message PDU / ReadRawSMS 读取原始短信 PDU
func (m *Manager) ReadRawSMS(storageType uint8, index uint32) ([]byte, error) {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
	defer cancel()
	return withWMSRecoveryValue(m, "ReadRawSMS", func(wms *qmi.WMSService) ([]byte, error) {
		return wms.RawReadMessage(ctx, storageType, index)
	})
}

// DecodedSMS represents a decoded SMS message / DecodedSMS 代表解码后的短信
type DecodedSMS struct {
	Index     uint32
	Storage   uint8
	Sender    string
	Message   string
	Timestamp time.Time

	// Concat info
	IsConcat    bool
	ConcatRef   int
	ConcatTotal int
	ConcatSeq   int
}

func DecodeIncomingSMSPDU(raw []byte, storageType uint8, index uint32) (*DecodedSMS, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("PDU too short")
	}

	candidates := incomingSMSPDUCandidates(raw)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("PDU decode failed: no supported SMS PDU candidate")
	}

	errs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		resp, err := decodeIncomingTPDU(candidate.tpdu, storageType, index)
		if err == nil {
			return resp, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", candidate.name, err))
	}
	return nil, fmt.Errorf("PDU decode failed: %s", strings.Join(errs, "; "))
}

type incomingSMSPDUCandidate struct {
	name string
	tpdu []byte
}

func incomingSMSPDUCandidates(raw []byte) []incomingSMSPDUCandidate {
	candidates := make([]incomingSMSPDUCandidate, 0, 3)
	appendCandidate := func(name string, tpduBytes []byte) {
		if !looksLikeDeliverTPDU(tpduBytes) {
			return
		}
		for _, candidate := range candidates {
			if string(candidate.tpdu) == string(tpduBytes) {
				return
			}
		}
		candidates = append(candidates, incomingSMSPDUCandidate{name: name, tpdu: append([]byte(nil), tpduBytes...)})
	}

	appendCandidate("direct_tpdu", raw)

	if tpduBytes, ok := extractFullPDUWithSMSC(raw); ok {
		appendCandidate("full_pdu_smsc", tpduBytes)
	}

	if tpduBytes, ok := extractIncomingRPDataTPDU(raw); ok {
		appendCandidate("rp_data", tpduBytes)
	}

	return candidates
}

func looksLikeDeliverTPDU(tpduBytes []byte) bool {
	return len(tpduBytes) > 0 && tpduBytes[0]&0x03 == 0
}

func extractFullPDUWithSMSC(raw []byte) ([]byte, bool) {
	if len(raw) < 2 {
		return nil, false
	}
	smscLen := int(raw[0])
	if smscLen == 0 {
		if len(raw) <= 1 {
			return nil, false
		}
		return raw[1:], true
	}
	if smscLen < 2 || 1+smscLen >= len(raw) {
		return nil, false
	}
	if raw[1]&0x80 == 0 {
		return nil, false
	}
	return raw[1+smscLen:], true
}

func extractIncomingRPDataTPDU(raw []byte) ([]byte, bool) {
	if len(raw) < 5 || raw[0] != 0x01 {
		return nil, false
	}
	i := 2 // RP-MTI + RP-MR
	if !skipRPAddress(raw, &i) {
		return nil, false
	}
	if !skipRPAddress(raw, &i) {
		return nil, false
	}
	if i >= len(raw) {
		return nil, false
	}
	udLen := int(raw[i])
	i++
	if udLen <= 0 || i+udLen > len(raw) {
		return nil, false
	}
	return raw[i : i+udLen], true
}

func skipRPAddress(raw []byte, i *int) bool {
	if i == nil || *i >= len(raw) {
		return false
	}
	n := int(raw[*i])
	*i = *i + 1
	if *i+n > len(raw) {
		return false
	}
	*i += n
	return true
}

func decodeIncomingTPDU(tpduBytes []byte, storageType uint8, index uint32) (*DecodedSMS, error) {
	if trimmed, ok := trimDeliverTPDUToDeclaredLength(tpduBytes); ok {
		tpduBytes = trimmed
	}

	pd := &tpdu.TPDU{}
	if err := pd.UnmarshalBinary(tpduBytes); err != nil {
		return nil, err
	}

	textBytes, err := sms.Decode([]*tpdu.TPDU{pd})
	if err != nil {
		return nil, err
	}

	resp := &DecodedSMS{
		Index:     index,
		Storage:   storageType,
		Sender:    pd.OA.Number(),
		Message:   string(textBytes),
		Timestamp: pd.SCTS.Time,
	}

	populateConcatInfo(resp, pd)
	return resp, nil
}

func populateConcatInfo(resp *DecodedSMS, pd *tpdu.TPDU) {
	if resp == nil || pd == nil {
		return
	}
	for _, ie := range pd.UDH {
		if ie.ID == 0x00 && len(ie.Data) >= 3 {
			resp.IsConcat = true
			resp.ConcatRef = int(ie.Data[0])
			resp.ConcatTotal = int(ie.Data[1])
			resp.ConcatSeq = int(ie.Data[2])
			return
		}
		if ie.ID == 0x08 && len(ie.Data) >= 4 {
			resp.IsConcat = true
			resp.ConcatRef = int(ie.Data[0])<<8 | int(ie.Data[1])
			resp.ConcatTotal = int(ie.Data[2])
			resp.ConcatSeq = int(ie.Data[3])
			return
		}
	}
}

// ReadSMS reads and decodes an SMS message / ReadSMS 读取并解码短信
func (m *Manager) ReadSMS(storageType uint8, index uint32) (*DecodedSMS, error) {
	raw, err := m.ReadRawSMS(storageType, index)
	if err != nil {
		return nil, err
	}
	return DecodeIncomingSMSPDU(raw, storageType, index)
}

// SendRawSMS sends a raw SMS PDU / SendRawSMS 发送原始短信 PDU
func (m *Manager) SendRawSMS(format uint8, pdu []byte) error {
	ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
	defer cancel()
	return m.withWMSRecovery("SendRawSMS", func(wms *qmi.WMSService) error {
		return wms.SendRawMessage(ctx, format, pdu)
	})
}

// SendSMS sends a text message / SendSMS 发送文本短信
func (m *Manager) SendSMS(number, text string) error {
	return m.SendSMSWithOptions(number, text, SendSMSOptions{})
}

// SendSMSWithOptions sends a text message with explicit submit encoding options.
func (m *Manager) SendSMSWithOptions(number, text string, opts SendSMSOptions) error {
	pdu, err := m.encodeSMSWithOptions(number, text, opts)
	if err != nil {
		return err
	}

	ctx, cancel := m.opContext(m.cfg.Timeouts.Dial)
	defer cancel()
	return m.withWMSRecovery("SendSMS", func(wms *qmi.WMSService) error {
		return wms.SendRawMessage(ctx, 0x06, pdu)
	})
}

type SMSEncoding string

const (
	SMSEncodingAuto SMSEncoding = "auto"
	SMSEncodingUCS2 SMSEncoding = "ucs2"
)

type SendSMSOptions struct {
	Encoding SMSEncoding
}

func NormalizeSMSEncoding(raw string) (SMSEncoding, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(SMSEncodingAuto):
		return SMSEncodingAuto, nil
	case string(SMSEncodingUCS2):
		return SMSEncodingUCS2, nil
	default:
		return "", fmt.Errorf("unsupported SMS encoding: %s", raw)
	}
}

// encodeSMS encodes a text message into a 7-bit PDU format using warthog618/sms / encodeSMS 使用 warthog618/sms 将文本消息编码为 7-bit PDU 格式
func (m *Manager) encodeSMS(number, text string) ([]byte, error) {
	return m.encodeSMSWithOptions(number, text, SendSMSOptions{})
}

func (m *Manager) encodeSMSWithOptions(number, text string, opts SendSMSOptions) ([]byte, error) {
	normalizedNumber := strings.TrimSpace(number)
	encoding, err := NormalizeSMSEncoding(string(opts.Encoding))
	if err != nil {
		return nil, err
	}

	msg := []byte(text)
	options := []sms.EncoderOption{sms.AsSubmit, sms.To(normalizedNumber)}
	if encoding == SMSEncodingUCS2 {
		msg = ucs2.Encode([]rune(text))
		options = append(options, sms.AsUCS2)
	}

	pdus, err := sms.Encode(msg, options...)
	if err != nil {
		return nil, err
	}
	if len(pdus) == 0 {
		return nil, fmt.Errorf("no PDUs generated")
	}
	if isLikelyShortCode(normalizedNumber) {
		da := pdus[0].DA
		da.SetTypeOfNumber(tpdu.TonUnknown)
		da.SetNumberingPlan(tpdu.NpISDN)
		pdus[0].DA = da
	}

	// Marshal the first PDU segment back to binary for QMI
	binaryTPDU, err := pdus[0].MarshalBinary()
	if err != nil {
		return nil, err
	}

	// QMI WMSRawSend expects: [SMSC_Len(1)] + [TPDU]
	// 0x00 means use the default SMSC stored in the SIM/modem
	pduWithSMSC := append([]byte{0x00}, binaryTPDU...)
	return pduWithSMSC, nil
}

func isLikelyShortCode(phone string) bool {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return false
	}
	if strings.HasPrefix(phone, "+") {
		return false
	}
	digits := strings.TrimLeft(phone, "0123456789")
	return digits == "" && len(phone) <= 6
}
