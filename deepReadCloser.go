package gcsext

import (
	"compress/gzip"
	"io"
)

type deepReadCloser struct {
	*gzip.Reader
	underlyingReader io.ReadCloser
}

func (gz *deepReadCloser) Close() error {
	if err := gz.Reader.Close(); err != nil {
		return err
	}

	if err := gz.underlyingReader.Close(); err != nil {
		return err
	}

	return nil
}
