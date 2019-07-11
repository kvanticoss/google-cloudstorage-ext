package gcsext

import "io"

type WriterCloserCallback struct {
	WriteCloser
	interceptor func()
}

var _ io.Writer = &WriterCloserCallback{}

// NewWriterCloserCallback wraps the Write() and Close() methods
// of a write closer with and callback that is invoked for each
// write or close call. The callback have no effect on the operation
// except adding a blocking delay.
func NewWriterCloserCallback(wc WriteCloser, interceptor func()) WriteCloser {
	return &WriterCloserCallback{wc, interceptor}
}

func (iwc *WriterCloserCallback) Write(p []byte) (n int, err error) {
	iwc.interceptor()
	return iwc.WriteCloser.Write(p)
}
