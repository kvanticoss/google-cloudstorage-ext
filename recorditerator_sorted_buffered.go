package gcsext

import (
	"github.com/google/btree"
)

// NewBufferedRecordIteratorBTree creates a B+Tree of bufferSize from which records are emitted in sorted order.
func NewBufferedRecordIteratorBTree(ri LesserIterator, bufferSize int) RecordIterator {
	tree := btree.New(2)
	var lastErr error

	addToTree := func() {
		val, err := ri()
		if err == nil {
			tree.ReplaceOrInsert(btreeLesser{val})
		} else {
			lastErr = err
		}
	}

	// Bootstap with some inital values
	for i := 0; i <= bufferSize; i++ {
		addToTree()
		if lastErr != nil {
			break
		}
	}

	// Sorted iterator
	return func() (interface{}, error) {
		defer addToTree()
		res := tree.DeleteMin()
		if res == nil {
			return nil, lastErr
		}
		return res.(btreeLesser).Lesser, nil
	}
}

/*


type resTuple struct {
	rec interface{}
	err error
}

// NewBufferedRecordIterator does stuff
func NewBufferedRecordIterator(ri RecordIterator, bufferSize int) RecordIterator {
	unorderd := make(chan *resTuple, bufferSize)
	someOrder := make(chan *resTuple, bufferSize)

	// Load all records
	go func() {
		for {
			res, err := ri()
			res <- &resTuple{res, err}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		tmpList := make([]interface{})
		for {
			if len(unorderd) > 0 {

			}
		}
	}()
	return func() (interface{}, error) {
		if isDoneErr != nil && len(ll) == 0 {
			return nil, isDoneErr
		}
		if len(ll) < bufferSize/2 {
			<-limitor
		} else {

		}

		tmp := ll[0]
		ll = ll[1:]
		return tmp, nil
	}
}

// NewBufferedRecordIterator
func NewBufferedRecordIteratorrrr(ri RecordIterator, bufferSize int) RecordIterator {
	mutex := sync.Mutex{}
	buffer := make(LesserList, 0, bufferSize)
	return func() (interface{}, error) {
		mutex.Lock()
		defer mutex.Unlock()
		if len(buffer) < bufferSize/2 {
			for i := len(buffer); i < bufferSize; i++ {
				rec, err := ri()
				lesser, ok := rec.(Lesser)
				if !ok {
					return rec, err
				}
				if err == ErrIteratorStop {
					break
				} else if err != nil {
					return nil, err
				}
				buffer = append(buffer, lesser)
			}
			sort.Sort(buffer)
		}
		if len(buffer) > 0 {
			res := buffer[0]
			buffer = buffer[1:]
			return res, nil
		}
		return nil, ErrIteratorStop
	}
}

*/
