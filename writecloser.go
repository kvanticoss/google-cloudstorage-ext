package gcsext

import (
	"io"
)

type WriteCloser interface {
	io.Writer
	Close() error
}
