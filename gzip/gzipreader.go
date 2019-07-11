package gzip

import (
	"compress/gzip"
	"io"
)

type Reader struct {
	*gzip.Reader
	underlyingReader io.ReadCloser
}

func NewReader(r io.ReadCloser) (*Reader, error) {
	gzReader, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &Reader{gzReader, r}, nil
}

func (gz *Reader) Close() error {
	if err := gz.Reader.Close(); err != nil {
		return err
	}
	if err := gz.underlyingReader.Close(); err != nil {
		return err
	}
	return nil
}
