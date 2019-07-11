// Package gzip provides compress/gzip eqv functionality but with the change that it
// flushes & closes the incoming writer as well.
package gzip

import (
	"compress/gzip"
	"io"
	"log"
	"sync"
)

// Writer creates a gzip file which closes the underlying stream as well as the gzip strip on close
type Writer struct {
	*gzip.Writer
	mutex            sync.Mutex
	underlyingWriter io.Writer
}

// NewWriter acts like a compress/gzip.NewWriter but that Close And Flushes will be cascaded to underlying writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Writer:           gzip.NewWriter(w),
		mutex:            sync.Mutex{},
		underlyingWriter: w,
	}
}

// Write writes data to the gzip stream
func (gz *Writer) Write(p []byte) (int, error) {
	gz.mutex.Lock()
	defer gz.mutex.Unlock()
	return gz.Writer.Write(p)
}

// Flush flushes and flushes the gzip writer AND the underlying writer
func (gz *Writer) Flush() error {
	gz.mutex.Lock()
	defer gz.mutex.Unlock()

	if err := gz.Writer.Flush(); err != nil {
		return err
	}

	if flusher, ok := gz.underlyingWriter.(flusher); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes the gzip writer AND the underlying writer
func (gz *Writer) Close() error {
	if err := gz.Flush(); err != nil {
		log.Print("Err: " + err.Error())
		return err
	}

	gz.mutex.Lock()
	defer gz.mutex.Unlock()

	if err := gz.Writer.Close(); err != nil {
		log.Print("Err: " + err.Error())
		return err
	}

	if closer, ok := gz.underlyingWriter.(closer); ok {
		if err := closer.Close(); err != nil {
			log.Print("Err: " + err.Error())
			return err
		}
	}

	return nil
}

type flusher interface {
	Flush() error
}

type closer interface {
	Close() error
}
