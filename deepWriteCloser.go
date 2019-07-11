package gcsext

import (
	"compress/gzip"
	"io"
)

// deepWriteCloser creates a gzip file which closes the underlying stream as well as the gzip strip on close
type deepWriteCloser struct {
	*gzip.Writer
	underlyingWriter io.WriteCloser
}

func (gz *deepWriteCloser) Close() error {
	if err := gz.Writer.Close(); err != nil {
		return err
	}

	if err := gz.underlyingWriter.Close(); err != nil {
		return err
	}

	return nil
}
