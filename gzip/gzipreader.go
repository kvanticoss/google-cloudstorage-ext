package gzip

import (
	"compress/gzip"
	"io"
)

// Reader is a Gzip reader that once closed will also close the underlying reader (which the standard gz reader does not)
type Reader struct {
	*gzip.Reader
	underlyingReader io.ReadCloser
}

// NewReader treats the r-stream as a gzip stream and returns a Reader
func NewReader(r io.ReadCloser) (*Reader, error) {
	gzReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &Reader{gzReader, r}, nil
}

// Close closes the gzip reader as well as the underlying reader
func (gz *Reader) Close() error {
	if err := gz.Reader.Close(); err != nil {
		return err
	}
	if err := gz.underlyingReader.Close(); err != nil {
		return err
	}
	return nil
}
