package hookedwritecloser

import (
	"errors"
	"io"
	"sync"
)

var ErrAlreadyClosed = errors.New("writer is closed")

// HookedWriteCloser adds the ability to get callbacks priror and after a close call.
type HookedWriteCloser struct {
	io.WriteCloser

	isClosed bool

	mutex sync.Mutex

	preCloseHooks  []func()
	postCloseHooks []func(error)

	preWriteHooks  []func()
	postWriteHooks []func(int, error)
}

// NewHookedWriteCloser returns a new HookedWriteCloser that wraps the closer.
func NewHookedWriteCloser(wc io.WriteCloser) *HookedWriteCloser {
	return &HookedWriteCloser{
		WriteCloser:    wc,
		isClosed:       false,
		mutex:          sync.Mutex{},
		preCloseHooks:  []func(){},
		postCloseHooks: []func(error){},
		preWriteHooks:  []func(){},
		postWriteHooks: []func(int, error){},
	}
}

// Close calls each pre-hook in order then closes the stream and calls each post-hook in order.
// PostClosehooks will have the result of the close operation forward to them.
func (h *HookedWriteCloser) Close() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.isClosed {
		return ErrAlreadyClosed
	}
	for _, closer := range h.preCloseHooks {
		closer()
	}
	res := h.WriteCloser.Close()
	h.isClosed = true

	for _, closer := range h.postCloseHooks {
		closer(res)
	}

	return res
}

// Write calls each pre-hook in order then Writes the stream and calls each post-hook in order.
// PostWritehooks will have the result of the Write operation forward to them.
func (h *HookedWriteCloser) Write(p []byte) (int, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.isClosed {
		return 0, ErrAlreadyClosed
	}
	for _, writer := range h.preWriteHooks {
		writer()
	}
	n, err := h.WriteCloser.Write(p)
	for _, writer := range h.postWriteHooks {
		writer(n, err)
	}
	return n, err
}

// AddPreCloseHooks adds one or more Pre-hooks to a close command. Each will be called in order before a close call
func (h *HookedWriteCloser) AddPreCloseHooks(f ...func()) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.preCloseHooks = append(h.preCloseHooks, f...)
}

// AddPostCloseHooks adds one or more Post-hooks to a close command. Each will be called in order before a close call
func (h *HookedWriteCloser) AddPostCloseHooks(f ...func(error)) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.postCloseHooks = append(h.postCloseHooks, f...)
}

// AddPreWriteHooks adds one or more Pre-hooks to a Write command. Each will be called in order before a Write call
func (h *HookedWriteCloser) AddPreWriteHooks(f ...func()) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.preWriteHooks = append(h.preWriteHooks, f...)
}

// AddPostWriteHooks adds one or more Post-hooks to a Write command. Each will be called in order before a Write call
func (h *HookedWriteCloser) AddPostWriteHooks(f ...func(int, error)) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.postWriteHooks = append(h.postWriteHooks, f...)
}
