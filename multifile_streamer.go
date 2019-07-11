package gcsext

import (
	"compress/gzip"
	"context"
	"io"
	"sync"
	"time"
)

const (
	multiWriterCachePurgesPerTTL = 10 // How many time per ttl duration should we try to check the cache
)

// MultiWriterCache is a utillity that keeps an index of multiple writers, indexed by a string (most often path)
// if a writer is requested and doesn't exist it gets created (using the provided factory). Writers that aren't
// used for long enough are automatically closed.
type MultiWriterCache struct {
	cxtCancel         func()
	newSteamer        func(path string) (wc WriteCloser, err error)
	mutex             *sync.Mutex
	lastAccessUpdates chan *lastUpdateMsg
	lastAccess        map[string]time.Time
	writers           map[string]WriteCloser
}

type lastUpdateMsg struct {
	path string
	time time.Time
}

// NewMultiWriterCache will dynamically open files for writing; not thread safe.
func NewMultiWriterCache(opener func(path string) (wc WriteCloser, err error), ttl time.Duration) *MultiWriterCache {
	ctx, cancel := context.WithCancel(context.Background())
	r := &MultiWriterCache{
		cxtCancel:         cancel,
		newSteamer:        opener,
		mutex:             &sync.Mutex{},
		lastAccessUpdates: make(chan *lastUpdateMsg, 100),
		lastAccess:        map[string]time.Time{},
		writers:           map[string]WriteCloser{},
	}
	go r.manageTTLExp(ctx, ttl)
	return r
}

func (mfw *MultiWriterCache) manageTTLExp(ctx context.Context, ttl time.Duration) {
	var ttlMutex sync.Mutex

	// Create a deamon to sync updates.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-mfw.lastAccessUpdates:
				ttlMutex.Lock()
				mfw.lastAccess[msg.path] = msg.time
				ttlMutex.Unlock()
			}
		}
	}()

	ticker := time.NewTicker(ttl / multiWriterCachePurgesPerTTL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ttlMutex.Lock()
			for k, v := range mfw.lastAccess {
				if now.Sub(v) > ttl {
					if w, ok := mfw.writers[k]; ok {
						w.Close()
						delete(mfw.writers, k)
					}
					delete(mfw.lastAccess, k)
				}
			}
			ttlMutex.Unlock()
		}
	}
}

// Close closes all opened files; will continue on error and return all (if any) errors
func (mfw *MultiWriterCache) Close() error {
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()

	me := MultiError{}
	for _, writer := range mfw.writers {
		if err := writer.Close(); err != nil {
			me = append(me, err)
		}
	}
	return me.MaybeError()
}

// GetWriter gets an existing writers for the path or creates one and saves it for later re-use.
func (mfw *MultiWriterCache) GetWriter(path string, gzipStream bool) (io.Writer, error) {
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()

	writer, ok := mfw.writers[path]
	if ok {
		return writer, nil
	}

	writer, err := mfw.newSteamer(path)
	if gzipStream {
		writer = &deepWriteCloser{gzip.NewWriter(writer), writer}
	}
	writer = NewWriterCloserCallback(writer, func() {
		mfw.lastAccessUpdates <- &lastUpdateMsg{
			path: path,
			time: time.Now(),
		}
	})

	if err != nil {
		return nil, err
	}
	mfw.writers[path] = writer
	return writer, err
}

/*
func (mfw *MultiWriterCache) GetGzipWriter(path string) (io.Writer, error) {
	log.Printf("GetGzipWriter: Locking %s", path)
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()
	defer log.Printf("GetGzipWriter: Releasing lock %s", path)

	gzipWriter, ok := mfw.gzipwriters[path]

	log.Printf("GetGzipWriter: setting last access on key %s", path)
	mfw.lastAccess[path] = time.Now()
	if !ok {

	}
	return gzipWriter, nil
}
*/
