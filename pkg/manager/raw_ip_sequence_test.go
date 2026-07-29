package manager

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/zanescope/quectel-qmi-go/pkg/qmi"
)

const testRawIPPath = "/sys/class/net/wwan0/qmi/raw_ip"

func TestEnsureKernelRawIPDownWriteReadbackOrder(t *testing.T) {
	var events []string
	reads := 0
	ops := rawIPKernelOps{
		readFile: func(path string) ([]byte, error) {
			if path != testRawIPPath {
				t.Fatalf("read path=%q want %q", path, testRawIPPath)
			}
			events = append(events, "read")
			reads++
			if reads == 1 {
				return []byte("N\n"), nil
			}
			return []byte("Y\n"), nil
		},
		bringDown: func(ifname string) error {
			if ifname != "wwan0" {
				t.Fatalf("bringDown interface=%q want wwan0", ifname)
			}
			events = append(events, "down")
			return nil
		},
		writeFile: func(path string, data []byte, mode os.FileMode) error {
			if path != testRawIPPath {
				t.Fatalf("write path=%q want %q", path, testRawIPPath)
			}
			if string(data) != "Y" {
				t.Fatalf("write data=%q want Y", data)
			}
			if mode != 0o644 {
				t.Fatalf("write mode=%#o want 0644", mode)
			}
			events = append(events, "write")
			return nil
		},
	}

	supported, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if err != nil {
		t.Fatalf("ensureKernelRawIP() error=%v", err)
	}
	if !supported {
		t.Fatal("ensureKernelRawIP() supported=false want true")
	}
	if got := strings.Join(events, ","); got != "read,down,write,read" {
		t.Fatalf("operation order=%q want read,down,write,read", got)
	}
}

func TestEnsureKernelRawIPAlreadyEnabledDoesNotTouchLink(t *testing.T) {
	ops := rawIPKernelOps{
		readFile: func(string) ([]byte, error) { return []byte("Y\n"), nil },
		bringDown: func(string) error {
			t.Fatal("bringDown called for already enabled raw_ip")
			return nil
		},
		writeFile: func(string, []byte, os.FileMode) error {
			t.Fatal("writeFile called for already enabled raw_ip")
			return nil
		},
	}

	supported, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if err != nil {
		t.Fatalf("ensureKernelRawIP() error=%v", err)
	}
	if !supported {
		t.Fatal("ensureKernelRawIP() supported=false want true")
	}
}

func TestEnsureKernelRawIPRequiresLinkDown(t *testing.T) {
	wantErr := errors.New("link down failed")
	ops := rawIPKernelOps{
		readFile:  func(string) ([]byte, error) { return []byte("N\n"), nil },
		bringDown: func(string) error { return wantErr },
		writeFile: func(string, []byte, os.FileMode) error {
			t.Fatal("writeFile called after bringDown failure")
			return nil
		},
	}

	_, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ensureKernelRawIP() error=%v want wrapped %v", err, wantErr)
	}
	if !strings.Contains(err.Error(), "bring interface wwan0 down before enabling raw_ip") {
		t.Fatalf("ensureKernelRawIP() error=%q missing link-down context", err)
	}
}

func TestEnsureKernelRawIPPreservesWriteBusyError(t *testing.T) {
	reads := 0
	ops := rawIPKernelOps{
		readFile: func(string) ([]byte, error) {
			reads++
			return []byte("N\n"), nil
		},
		bringDown: func(string) error { return nil },
		writeFile: func(path string, _ []byte, _ os.FileMode) error {
			return &os.PathError{Op: "write", Path: path, Err: syscall.EBUSY}
		},
	}

	_, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("ensureKernelRawIP() error=%v want wrapped EBUSY", err)
	}
	if !strings.Contains(err.Error(), "write raw_ip=Y for interface wwan0 after link down") {
		t.Fatalf("ensureKernelRawIP() error=%q missing write context", err)
	}
	if reads != 1 {
		t.Fatalf("read calls=%d want 1 after failed write", reads)
	}
}

func TestEnsureKernelRawIPRejectsReadbackMismatch(t *testing.T) {
	ops := rawIPKernelOps{
		readFile:  func(string) ([]byte, error) { return []byte("N\n"), nil },
		bringDown: func(string) error { return nil },
		writeFile: func(string, []byte, os.FileMode) error { return nil },
	}

	_, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if err == nil {
		t.Fatal("ensureKernelRawIP() error=nil want readback mismatch")
	}
	if !strings.Contains(err.Error(), `got "N", want "Y"`) {
		t.Fatalf("ensureKernelRawIP() error=%q missing readback values", err)
	}
}

func TestEnsureKernelRawIPMissingAttributeIsUnsupported(t *testing.T) {
	ops := rawIPKernelOps{
		readFile: func(string) ([]byte, error) { return nil, os.ErrNotExist },
		bringDown: func(string) error {
			t.Fatal("bringDown called for missing raw_ip attribute")
			return nil
		},
		writeFile: func(string, []byte, os.FileMode) error {
			t.Fatal("writeFile called for missing raw_ip attribute")
			return nil
		},
	}

	supported, err := ensureKernelRawIP("wwan0", testRawIPPath, ops)
	if err != nil {
		t.Fatalf("ensureKernelRawIP() error=%v", err)
	}
	if supported {
		t.Fatal("ensureKernelRawIP() supported=true want false")
	}
}

func TestEnsureDataPlaneServicesPropagatesAndRetriesRawIPFailure(t *testing.T) {
	m := newRecoveryTestManager()
	m.cfg = Config{
		Device:          ModemDevice{NetInterface: "wwan0"},
		EnableIPv4:      true,
		DataPlanePolicy: DataPlanePolicyLazy,
	}
	m.client = &qmi.Client{}

	wdaAllocations := 0
	rawIPAttempts := 0
	m.newWDSService = func(context.Context, *qmi.Client) (*qmi.WDSService, error) {
		return &qmi.WDSService{}, nil
	}
	m.newWDAService = func(context.Context, *qmi.Client) (*qmi.WDAService, error) {
		wdaAllocations++
		return &qmi.WDAService{}, nil
	}
	m.enableRawIPHook = func(context.Context) error {
		rawIPAttempts++
		if rawIPAttempts == 1 {
			return syscall.EBUSY
		}
		return nil
	}

	err := m.ensureDataPlaneServices(context.Background())
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("first ensureDataPlaneServices() error=%v want wrapped EBUSY", err)
	}
	if !strings.Contains(err.Error(), "failed to enable RawIP mode") {
		t.Fatalf("first ensureDataPlaneServices() error=%q missing RawIP context", err)
	}
	if m.rawIPConfigured.Load() {
		t.Fatal("rawIPConfigured=true after failed configuration")
	}

	if err := m.ensureDataPlaneServices(context.Background()); err != nil {
		t.Fatalf("second ensureDataPlaneServices() error=%v", err)
	}
	if wdaAllocations != 1 {
		t.Fatalf("WDA allocations=%d want 1", wdaAllocations)
	}
	if rawIPAttempts != 2 {
		t.Fatalf("RawIP attempts=%d want 2", rawIPAttempts)
	}
	if !m.rawIPConfigured.Load() {
		t.Fatal("rawIPConfigured=false after successful retry")
	}
}
