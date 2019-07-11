package hookedwritecloser

import (
	"io"
	"sync"
	"time"
)

const (
	multiWriterCachePurgesPerTTL = 10 // How many time per ttl duration should we try to check the cache
)

// SelfDestructWriteCloser adds the ability to get automatically close a writer after x time or bytes.
type SelfDestructWriteCloser struct {
	*HookedWriteCloser

	// for WithMaxIdleTime
	lastWrite time.Time

	// for WithMaxBytesWritten
	bytesWriten int
}

// SelfDestructOption represents an option that can be provided the the NewSelfDestructWriteCloser constructor
type SelfDestructOption func(*SelfDestructWriteCloser) *SelfDestructWriteCloser

// NewSelfDestructWriteCloser returns a new SelfDestructWriteCloser
func NewSelfDestructWriteCloser(wc io.WriteCloser, options ...SelfDestructOption) *SelfDestructWriteCloser {
	hwc, ok := wc.(*HookedWriteCloser)
	if !ok {
		hwc = NewHookedWriteCloser(wc)
	}
	res := &SelfDestructWriteCloser{
		HookedWriteCloser: hwc,
		lastWrite:         time.Now(),
	}
	for _, option := range options {
		res = option(res)
	}
	return res
}

// WithMaxIdleTime will add a timeout that when reached the writecloser till automatically
// The resolution of the timer will be 10th of the maxIdle meaning the file will close between
// maxIdleTime AND maxIdleTime*1.1 seconds from the last write.
func WithMaxIdleTime(maxIdleTime time.Duration) SelfDestructOption {
	return func(sdwc *SelfDestructWriteCloser) *SelfDestructWriteCloser {
		mutex := sync.Mutex{}

		mutex.Lock()
		defer mutex.Unlock()
		sdwc.lastWrite = time.Now()

		ticker := time.NewTicker(maxIdleTime / multiWriterCachePurgesPerTTL)
		go func() {
			for tick := range ticker.C {
				mutex.Lock()
				if tick.After(sdwc.lastWrite.Add(maxIdleTime)) {
					sdwc.Close()
					mutex.Unlock()
					return
				}
				mutex.Unlock()
			}
		}()

		// Let's update the last access both before and after so we don't
		// end up with to crazy race conditions race conditions. We could
		// still have a situation where a file that has been written for a long time
		// is closed directly after the write (the mutex lock will stop any
		// cuncurrent closing of an active write)
		sdwc.AddPreWriteHooks(func() {
			mutex.Lock()
			defer mutex.Unlock()
			sdwc.lastWrite = time.Now()
		})
		sdwc.AddPostWriteHooks(func(_ int, _ error) {
			mutex.Lock()
			defer mutex.Unlock()
			sdwc.lastWrite = time.Now()
		})
		sdwc.AddPostCloseHooks(func(_ error) {
			ticker.Stop()
		})
		return sdwc
	}
}

// WithMaxBytesWritten will force a close of the Writer AFTER maxBytes bytes have been written.
func WithMaxBytesWritten(maxBytes int) SelfDestructOption {
	return func(sdwc *SelfDestructWriteCloser) *SelfDestructWriteCloser {
		sdwc.AddPostWriteHooks(func(written int, _ error) {
			sdwc.bytesWriten += written
			if sdwc.bytesWriten >= maxBytes {
				sdwc.Close()
			}
		})
		return sdwc
	}
}
