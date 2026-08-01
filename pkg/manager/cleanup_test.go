package manager

import (
	"context"
	"testing"
	"time"
)

func TestCleanupDoesNotUseFixedSleepWhenNoTasks(t *testing.T) {
	m := newRecoveryTestManager()
	m.cfg.Timeouts.Stop = time.Second

	start := time.Now()
	m.cleanup()
	elapsed := time.Since(start)

	if elapsed >= 90*time.Millisecond {
		t.Fatalf("cleanup() elapsed = %s, want no fixed 100ms sleep", elapsed)
	}
}

func TestRunCleanupTasksWaitsForCompletion(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan []cleanupTaskResult, 1)

	go func() {
		done <- runCleanupTasks(context.Background(), NewNopLogger(), []cleanupTask{{
			name: "slow",
			run: func(context.Context) error {
				close(started)
				<-release
				return nil
			},
		}})
	}()

	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cleanup task did not start")
	}

	select {
	case <-done:
		t.Fatal("runCleanupTasks returned before task completed")
	default:
	}

	close(release)

	select {
	case results := <-done:
		if len(results) != 1 || results[0].name != "slow" || results[0].err != nil {
			t.Fatalf("cleanup task results = %#v, want one successful slow task", results)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("runCleanupTasks did not return after task completed")
	}
}

func TestRunCleanupTasksCancelsButJoinsAtContextDeadline(t *testing.T) {
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	canceled := make(chan struct{})
	done := make(chan []cleanupTaskResult, 1)
	go func() {
		done <- runCleanupTasks(ctx, NewNopLogger(), []cleanupTask{{
			name: "blocked",
			run: func(taskCtx context.Context) error {
				<-taskCtx.Done()
				close(canceled)
				<-release
				return taskCtx.Err()
			},
		}})
	}()

	select {
	case <-canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cleanup task did not receive deadline cancellation")
	}
	select {
	case results := <-done:
		t.Fatalf("runCleanupTasks orphaned blocked worker: %#v", results)
	default:
	}

	close(release)
	select {
	case results := <-done:
		if len(results) != 1 || results[0].name != "blocked" || results[0].err == nil {
			t.Fatalf("cleanup task results = %#v, want joined task with deadline error", results)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("runCleanupTasks did not return after canceled worker exited")
	}
}
