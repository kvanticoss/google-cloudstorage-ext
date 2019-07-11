package gcsext

import (
	"io"
)

// WriteCloser is the comination of a writer and closer.
type WriteCloser interface {
	io.Writer
	Close() error
}

// NopeWriteCloserr ads a NOP Close method to any writer.
type NopeWriteCloserr struct {
	io.Writer
}

// Close is a NOP
func (nc NopeWriteCloserr) Close() error {
	return nil
}
