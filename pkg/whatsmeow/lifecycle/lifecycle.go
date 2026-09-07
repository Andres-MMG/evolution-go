// Package lifecycle serializes instance generations without blocking event callbacks.
package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrStopped = errors.New("instance stopped; explicit connection required")

// Runner creates/connects one client and returns its teardown even on startup error.
// Teardown runs outside callbacks and completes before any successor is launched.
type Runner func(*Run) (teardown func(logout bool) error, err error)

type Run struct {
	ctx         context.Context
	cancel      context.CancelFunc
	ready, done chan struct{}
	err         error
	cleanupErr  error
	runner      Runner
	logout      atomic.Bool
}

func (r *Run) Context() context.Context { return r.ctx }
func (r *Run) Done() <-chan struct{}    { return r.done }
func (r *Run) Wait(ctx context.Context) error {
	select {
	case <-r.ready:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type instance struct {
	current, next *Run
	terminal      bool
}

// Controller must be shared by pointer; service values may safely refer to it.
type Controller struct {
	mu        sync.Mutex
	instances map[string]*instance
}

func New() *Controller { return &Controller{instances: make(map[string]*instance)} }

func newRun(runner Runner) *Run {
	ctx, cancel := context.WithCancel(context.Background())
	return &Run{ctx: ctx, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), runner: runner}
}

func (c *Controller) Start(id string, explicit bool, runner Runner) (*Run, error) {
	return c.request(id, nil, explicit, false, runner)
}

func (c *Controller) Restart(id string, expected *Run, explicit bool, runner Runner) (*Run, error) {
	return c.request(id, expected, explicit, true, runner)
}

func (c *Controller) request(id string, expected *Run, explicit, restart bool, runner Runner) (*Run, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.instances[id]
	if e == nil {
		e = &instance{}
		c.instances[id] = e
	}
	if expected != nil && e.current != expected {
		return e.current, nil
	}
	if e.terminal && !explicit {
		return nil, ErrStopped
	}
	if explicit {
		e.terminal = false
	}
	if e.current == nil {
		e.current = newRun(runner)
		go c.execute(id, e.current)
		return e.current, nil
	}
	if e.next != nil {
		return e.next, nil
	}
	if !restart && e.current.ctx.Err() == nil {
		return e.current, nil
	}
	e.next = newRun(runner)
	e.current.cancel()
	return e.next, nil
}

func (c *Controller) Current(id string) *Run {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.instances[id]; e != nil {
		return e.current
	}
	return nil
}

func (c *Controller) Active(id string, run *Run) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.instances[id]
	return e != nil && e.current == run && run != nil && run.ctx.Err() == nil
}

// Stop is non-blocking, terminal, and generation-scoped when expected is supplied.
// Only explicit connect/pair/manual reconnect can clear the terminal barrier.
func (c *Controller) Stop(id string, expected *Run, logout bool) <-chan struct{} {
	return c.stop(id, expected, logout).done
}

// StopAndWait binds the waiter to the generation selected by the stop request.
// Callback callers continue using non-blocking Stop instead.
func (c *Controller) StopAndWait(ctx context.Context, id string, logout bool) error {
	run := c.stop(id, nil, logout)
	select {
	case <-run.done:
		return run.cleanupErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func completedRun() *Run {
	run := newRun(nil)
	run.cancel()
	close(run.ready)
	close(run.done)
	return run
}

func (c *Controller) stop(id string, expected *Run, logout bool) *Run {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.instances[id]
	if e == nil {
		e = &instance{}
		c.instances[id] = e
	}
	if expected != nil && e.current != expected {
		return completedRun()
	}
	e.terminal = true
	if e.next != nil {
		e.next.cancel()
		e.next.err = ErrStopped
		close(e.next.ready)
		close(e.next.done)
		e.next = nil
	}
	if e.current == nil {
		return completedRun()
	}
	if logout {
		e.current.logout.Store(true)
	}
	e.current.cancel()
	return e.current
}

func (c *Controller) execute(id string, run *Run) {
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		e := c.instances[id]
		close(run.done)
		if e.current != run {
			return
		}
		e.current, e.next = e.next, nil
		if e.current != nil {
			go c.execute(id, e.current)
		}
	}()
	cleanup, err := run.runner(run)
	if err == nil && run.ctx.Err() != nil {
		err = ErrStopped
	}
	run.err = err
	close(run.ready)
	if err == nil {
		<-run.ctx.Done()
	}
	run.cancel()
	if cleanup != nil {
		run.cleanupErr = cleanup(run.logout.Load())
	}
}
