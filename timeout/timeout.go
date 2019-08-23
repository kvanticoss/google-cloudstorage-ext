package timeout

import (
	"context"
	"sync"
	"time"
)

// Timeout allows users to implement keep alive patterns where after a certain amount of inactivty callbacks are triggerd
type Timeout struct {
	ctx   context.Context
	mutex sync.Mutex

	lastPing      time.Time
	callbackOnCtx bool
	maxIdle       time.Duration
	callbacks     []func()

	reset chan bool
}

// NewTimeout returns a Timeout object which will call each callback sequencially after maxIdle time has passed
// withouth Timeout.Ping() being called.
// callbacks will not be invoked on ctx.Done unless callbackOnCtxDone == true
// Please note that due to how golang manages channels, there is a risk that Ping can be called and directly after each of
// the callbacks are invoked (even though the maxIdle hasn't been reached since the last ping).
func NewTimeout(ctx context.Context, maxIdle time.Duration, callbackOnCtxDone bool, callbacks ...func()) *Timeout {
	res := &Timeout{
		ctx:           ctx,
		mutex:         sync.Mutex{},
		lastPing:      time.Now(),
		callbackOnCtx: callbackOnCtxDone,
		maxIdle:       maxIdle,
		callbacks:     callbacks,

		reset: make(chan bool),
	}

	go res.monitorIdle()

	return res
}

func (to *Timeout) finalize() {
	to.mutex.Lock()
	defer to.mutex.Unlock()
	for _, f := range to.callbacks {
		f()
	}
	close(to.reset)
}

func (to *Timeout) monitorIdle() {
	for {
		to.lastPing = time.Now()
		select {
		case <-to.ctx.Done():
			if to.callbackOnCtx {
				to.finalize()
			}
			return
		case <-time.After(to.maxIdle):
			to.finalize()
			return
		case <-to.reset:
			// NOP
		}
	}
}

// LastPing holds the timestamp of the last ping / timer reset
func (to *Timeout) LastPing() time.Time {
	return to.lastPing
}

// TimeRemainging returns the duration until the callbacks will be triggerd if no more Ping() are called
func (to *Timeout) TimeRemainging() time.Duration {
	return to.maxIdle - time.Now().Sub(to.lastPing)
}

// Ping resets the idle timer to zero; non blocking
func (to *Timeout) Ping() {
	to.mutex.Lock()
	defer to.mutex.Unlock()

	if to.reset == nil {
		return
	}

	select {
	case to.reset <- true:
	default:
	}
}
