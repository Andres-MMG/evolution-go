package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("lifecycle transition did not complete")
	}
}

func ready(t *testing.T, run *Run) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := run.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentStartReservesBeforeLaunching(t *testing.T) {
	c := New()
	var starts atomic.Int32
	gate := make(chan struct{})
	runner := func(r *Run) (func(bool) error, error) {
		starts.Add(1)
		<-gate
		return nil, nil
	}
	var wg sync.WaitGroup
	runs := make(chan *Run, 32)
	for range 32 {
		wg.Go(func() {
			r, err := c.Start("one", false, runner)
			if err != nil {
				t.Error(err)
			}
			runs <- r
		})
	}
	wg.Wait()
	close(gate)
	first := <-runs
	ready(t, first)
	for range 31 {
		if r := <-runs; r != first {
			t.Fatal("duplicate start reservation")
		}
	}
	if starts.Load() != 1 {
		t.Fatalf("starts = %d", starts.Load())
	}
	await(t, c.Stop("one", nil, false))
}

func TestRestartCoalescesAndWaitsForOldCleanup(t *testing.T) {
	c := New()
	cleanupEntered, cleanupRelease := make(chan struct{}), make(chan struct{})
	old, _ := c.Start("one", false, func(r *Run) (func(bool) error, error) {
		return func(bool) error { close(cleanupEntered); <-cleanupRelease; return nil }, nil
	})
	ready(t, old)
	var starts atomic.Int32
	runner := func(r *Run) (func(bool) error, error) { starts.Add(1); return nil, nil }
	next, _ := c.Restart("one", old, false, runner)
	await(t, cleanupEntered)
	for range 32 {
		duplicate, err := c.Restart("one", old, false, runner)
		if err != nil || duplicate != next {
			t.Fatal("restart was not coalesced")
		}
	}
	if starts.Load() != 0 {
		t.Fatal("successor started before old cleanup completed")
	}
	close(cleanupRelease)
	ready(t, next)
	if starts.Load() != 1 {
		t.Fatalf("starts = %d", starts.Load())
	}
	if c.Active("one", old) {
		t.Fatal("old callback still owns instance")
	}
	stale, err := c.Restart("one", old, false, runner)
	if err != nil || stale != next {
		t.Fatal("stale callback altered successor")
	}
	await(t, c.Stop("one", old, true))
	if !c.Active("one", next) {
		t.Fatal("stale stop killed successor")
	}
	await(t, c.Stop("one", next, false))
}

func TestTerminalStopCancelsRestartAndRequiresExplicitStart(t *testing.T) {
	c := New()
	gate := make(chan struct{})
	runner := func(r *Run) (func(bool) error, error) { <-gate; return nil, nil }
	old, _ := c.Start("one", false, runner)
	next, _ := c.Restart("one", old, false, runner)
	done := c.Stop("one", old, false)
	close(gate)
	await(t, done)
	if err := next.Wait(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("queued result = %v", err)
	}
	if _, err := c.Start("one", false, runner); !errors.Is(err, ErrStopped) {
		t.Fatal("passive start revived terminal session")
	}
	if _, err := c.Restart("one", nil, false, runner); !errors.Is(err, ErrStopped) {
		t.Fatal("passive retry revived terminal session")
	}
	fresh, err := c.Start("one", true, runner)
	if err != nil {
		t.Fatal(err)
	}
	ready(t, fresh)
	await(t, c.Stop("one", nil, false))
}

func TestStartupFailureReleasesWaitersAndAllowsRetry(t *testing.T) {
	for _, stage := range []string{"creation", "connection"} {
		t.Run(stage, func(t *testing.T) {
			c := New()
			failure := errors.New(stage)
			var cleanups atomic.Int32
			r, _ := c.Start("one", false, func(*Run) (func(bool) error, error) {
				return func(bool) error { cleanups.Add(1); return nil }, failure
			})
			if err := r.Wait(context.Background()); !errors.Is(err, failure) {
				t.Fatalf("result = %v", err)
			}
			await(t, r.Done())
			if cleanups.Load() != 1 {
				t.Fatal("failure skipped cleanup")
			}
			fresh, err := c.Start("one", false, func(*Run) (func(bool) error, error) { return nil, nil })
			if err != nil || fresh == r {
				t.Fatal("failure retained reservation")
			}
			ready(t, fresh)
			await(t, c.Stop("one", nil, false))
		})
	}
}

func TestStopDuringStartupIsNonBlockingAndInstancesAreIndependent(t *testing.T) {
	c := New()
	entered := make(chan struct{})
	first, _ := c.Start("one", false, func(r *Run) (func(bool) error, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	await(t, entered)
	second, _ := c.Start("two", false, func(*Run) (func(bool) error, error) { return nil, nil })
	ready(t, second)
	await(t, c.Stop("one", first, false))
	if !c.Active("two", second) {
		t.Fatal("one instance blocked another")
	}
	await(t, c.Stop("two", nil, false))
}

func TestRegistryStaleCleanupCannotDeleteSuccessor(t *testing.T) {
	r := &Registry[*int]{}
	old, next := new(int), new(int)
	r.Store("one", old)
	r.Store("one", next)
	if r.CompareDelete("one", old) || r.Load("one") != next {
		t.Fatal("stale cleanup deleted successor")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 100 {
				r.Store("other", old)
				_ = r.Load("one")
				r.CompareDelete("other", old)
			}
		})
	}
	wg.Wait()
}

func TestFailedStartupInvalidatesCallbacksBeforeTeardown(t *testing.T) {
	c := New()
	var activeDuringCleanup atomic.Bool
	r, _ := c.Start("one", false, func(r *Run) (func(bool) error, error) {
		return func(bool) error { activeDuringCleanup.Store(c.Active("one", r)); return nil }, errors.New("connect failed")
	})
	await(t, r.Done())
	if activeDuringCleanup.Load() {
		t.Fatal("failed generation can request restart from teardown callback")
	}
}

func TestCallbackMayRequestRestartDuringConnectWithoutWaiting(t *testing.T) {
	c := New()
	successor := make(chan *Run, 1)
	old, _ := c.Start("one", false, func(r *Run) (func(bool) error, error) {
		next, err := c.Restart("one", r, false, func(*Run) (func(bool) error, error) { return nil, nil })
		if err != nil {
			return nil, err
		}
		successor <- next
		return nil, nil
	})
	await(t, old.Done())
	ready(t, <-successor)
	await(t, c.Stop("one", nil, false))
}

func TestConcurrentStartAndRestartNeverOverlapLiveGenerations(t *testing.T) {
	c := New()
	var live, peak atomic.Int32
	runner := func(r *Run) (func(bool) error, error) {
		n := live.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		return func(bool) error { live.Add(-1); return nil }, nil
	}
	first, _ := c.Start("one", false, runner)
	ready(t, first)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { _, _ = c.Start("one", false, runner) })
		wg.Go(func() { _, _ = c.Restart("one", first, false, runner) })
	}
	wg.Wait()
	await(t, c.Stop("one", nil, false))
	if peak.Load() != 1 || live.Load() != 0 {
		t.Fatalf("peak=%d live=%d", peak.Load(), live.Load())
	}
}

func TestStopAndWaitReturnsTeardownErrorOnlyAfterCleanup(t *testing.T) {
	for _, failure := range []error{nil, errors.New("remote logout rejected")} {
		c := New()
		entered, release := make(chan struct{}), make(chan struct{})
		r, _ := c.Start("one", false, func(*Run) (func(bool) error, error) {
			return func(logout bool) error {
				if !logout {
					t.Error("logout intent was lost")
				}
				close(entered)
				<-release
				return failure
			}, nil
		})
		ready(t, r)
		result := make(chan error, 1)
		go func() { result <- c.StopAndWait(context.Background(), "one", true) }()
		await(t, entered)
		select {
		case <-result:
			t.Fatal("stop completed before cleanup")
		default:
		}
		close(release)
		if err := <-result; !errors.Is(err, failure) {
			t.Fatalf("completion result = %v", err)
		}
		await(t, r.Done())
	}
}

func TestStopAndWaitTimeoutDoesNotReleaseGenerationBeforeCleanup(t *testing.T) {
	c := New()
	entered, release := make(chan struct{}), make(chan struct{})
	r, _ := c.Start("one", false, func(*Run) (func(bool) error, error) {
		return func(bool) error { close(entered); <-release; return nil }, nil
	})
	ready(t, r)
	c.Stop("one", r, false)
	await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.StopAndWait(ctx, "one", true); !errors.Is(err, context.Canceled) {
		t.Fatalf("timeout result = %v", err)
	}
	select {
	case <-r.Done():
		t.Fatal("wait timeout released running cleanup")
	default:
	}
	close(release)
	await(t, r.Done())
}
