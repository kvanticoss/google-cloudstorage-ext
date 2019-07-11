package gcsext

import "fmt"

var (
	// ErrIteratorStop is returned by RecordIterators where there are not more records to be found.
	ErrIteratorStop = fmt.Errorf("iterator stop")
)

// RecordIterator is a function which yeild any golang data struct each time called
// Where there are no more records; ErrIteratorStop should be returned and should not
// be treated as an error (compare to io.EOF)
type RecordIterator func() (interface{}, error)
