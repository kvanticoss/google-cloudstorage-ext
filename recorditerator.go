package gcsext

import (
	"github.com/kvanticoss/goutils/iterator"
)

var (
	// ErrIteratorStop is returned by RecordIterators where there are not more records to be found.
	ErrIteratorStop = iterator.ErrIteratorStop
)
