package gcsext

import "errors"

// Lesser is implemented by type which can be compared to each other and should answer
// i'm I/this/self less than the other record (argument 1)
type Lesser interface {
	Less(other interface{}) bool
}

// LesserIterator iterators is the function interface
type LesserIterator func() (Lesser, error)

// ErrNotLesser is returned when an interface is expected to implement Lesser but isn't
var ErrNotLesser = errors.New("record does not implement the Lesser interface and can't be sorted")

func toLesserIterator(it RecordIterator) LesserIterator {
	return func() (Lesser, error) {
		record, err := it()
		if err != nil {
			return nil, err
		}
		if l, ok := record.(Lesser); ok {
			return l, nil
		}
		return nil, ErrNotLesser
	}
}
