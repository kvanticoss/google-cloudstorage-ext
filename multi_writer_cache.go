package gcsext

import (
	"context"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kvanticoss/google-cloudstorage-ext/hookedwritecloser"
)

// MultiWriterCache is a utillity that keeps an index of multiple writers, indexed by a string (most often path)
// if a writer is requested and doesn't exist it gets created (using the provided factory). Writers that aren't
// used for long enough are automatically closed.
type MultiWriterCache struct {
	cxtCancel      func()
	mutex          *sync.Mutex
	newSteamer     func(path string) (wc WriteCloser, err error)
	writers        map[string]WriteCloser
	ttl            time.Duration
	writersCreated int
}

// NewMultiWriterCache will dynamically open files for writing.
func NewMultiWriterCache(ctx context.Context, opener func(path string) (wc WriteCloser, err error), ttl time.Duration) *MultiWriterCache {
	ctx, cancel := context.WithCancel(ctx)
	return &MultiWriterCache{
		cxtCancel:      cancel,
		mutex:          &sync.Mutex{},
		newSteamer:     opener,
		writers:        map[string]WriteCloser{},
		ttl:            ttl,
		writersCreated: 0,
	}
}

// Close closes all opened files; will continue on error and return all (if any) errors
func (mfw *MultiWriterCache) Close() error {
	me := MultiError{}

	// Avoid races since we will be clearing keys form  mfw.writers in the Cloase calls
	writercopy := map[string]WriteCloser{}
	mfw.mutex.Lock()
	for k, v := range mfw.writers {
		writercopy[k] = v
	}
	mfw.mutex.Unlock()

	for path, _ := range writercopy {
		if err := mfw.ClosePath(path); err != nil && err != hookedwritecloser.ErrAlreadyClosed { // since we don't want to have a mutex here, chances are that ErrAlreadyClosed can happen
			me = append(me, err)
		}
	}
	return me.MaybeError()
}

func (mfw *MultiWriterCache) ClosePath(path string) error {
	mfw.mutex.Lock()

	writer, ok := mfw.writers[path]
	if !ok {
		mfw.mutex.Unlock()
		return nil
	}
	mfw.mutex.Unlock()

	return writer.Close()
}

func (mfw *MultiWriterCache) getWriter(path string) (io.Writer, error) {
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()

	writer, ok := mfw.writers[path]
	if ok {
		return writer, nil
	}

	newSuffixedPath := strings.Replace(
		path,
		"{suffix}",
		strconv.Itoa(mfw.writersCreated)+"_"+strconv.Itoa(int(time.Now().Unix())),
		-1)

	writer, err := mfw.newSteamer(newSuffixedPath)
	if err != nil {
		return nil, err
	}
	mfw.writersCreated++

	// Make this writer self destruct
	hwc := hookedwritecloser.NewSelfDestructWriteCloser(
		writer,
		hookedwritecloser.WithMaxIdleTime(mfw.ttl),
	)
	// And when that happens, remove the reference to it.
	hwc.AddPreCloseHooks(func() {
		mfw.mutex.Lock()
		defer mfw.mutex.Unlock()

		delete(mfw.writers, path)
	})

	mfw.writers[path] = hwc
	return hwc, nil
}

// GetWriter gets an existing writers for the path or creates one and saves it for later re-use. If the path contains {suffix}
// it will be replaced by a unique counter + timestamp.
func (mfw *MultiWriterCache) Write(path string, p []byte) (int, error) {
	writer, err := mfw.getWriter(path)
	if err != nil {
		return 0, err
	}

	if n, err := writer.Write(p); err == nil {
		return n, nil
	} else if err == hookedwritecloser.ErrAlreadyClosed { // Make once race condition less likely
		log.Printf("Retrying write as ErrAlreadyClosed")
		return mfw.Write(path, p)
	} else {
		return n, err
	}
}
