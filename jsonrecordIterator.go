package gcsext

import (
	"encoding/json"
	"io"
)

// JSONRecordIterator returns a RecordIterator based on a JSON stream of data. Often NewLine delimited
// @new - creator to allocate a new struct for a record.
// @r - byte stream reader containing new line delimited json data
func JSONRecordIterator(new func() interface{}, r io.Reader) RecordIterator {
	dec := json.NewDecoder(r)
	return func() (interface{}, error) {
		dst := new()
		if !dec.More() {
			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
			return nil, ErrIteratorStop
		}
		return dst, dec.Decode(dst)
	}
}
