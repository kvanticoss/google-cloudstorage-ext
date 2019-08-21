package gcsext

import (
	"io"
)

// WriteCloser is the comination of a writer and closer.
type WriteCloser interface {
	io.Writer
	Close() error
}

// NopWriteCloser ads a NOP Close method to any writer.
type NopWriteCloser struct {
	io.Writer
}

// Close is a NOP
func (nc NopWriteCloser) Close() error {
	return nil
}
