package gcsext

import (
	"errors"
)

var (
	ErrNotLesser    = errors.New("record does not implement the Lesser interface and can't be sorted")
	ErrGotNilRecord = errors.New("record should not be nil (not comparable), return iterator stop instead")
)

type lesserIterator func() (Lesser, error)

// SortedRecordIterator combines a list of iterators; always yeilding the lowest value
// available from all iterators. To do this it keeps a local "peak cache" of the next
// value for each iterator. This means that iterators that produces data from volatile
// sources (e.g time) might be experience unexpected results.
func SortedRecordIterator(iterators []RecordIterator) (RecordIterator, error) {
	var err error
	lesserIterators := make([]lesserIterator, len(iterators))
	nextCandidates := make([]Lesser, len(iterators))
	for i, ri := range iterators {
		lesserIterators[i] = toLesserIterator(ri)
		nextCandidates[i], err = lesserIterators[i]()
		if err != nil && err != ErrIteratorStop { // Stops are not errors
			return nil, err
		}
	}

	return func() (interface{}, error) {
		bestIndex := -1
		var bestCandidate Lesser

		for i, candidate := range nextCandidates {
			if candidate == nil {
				continue
			}
			if bestIndex == -1 {
				bestIndex = i
				bestCandidate = candidate
				continue
			}

			if !bestCandidate.Less(candidate) {
				bestIndex = i
				bestCandidate = candidate
			}
		}

		if bestIndex == -1 {
			return nil, ErrIteratorStop
		}

		nextRecord, err := lesserIterators[bestIndex]()
		if err == ErrIteratorStop {
			nextCandidates[bestIndex] = nil
		} else if err != nil {
			nextCandidates[bestIndex] = nil
			return nil, err
		} else if l, ok := nextRecord.(Lesser); !ok {
			return nil, ErrNotLesser
		} else {
			nextCandidates[bestIndex] = l
		}

		return bestCandidate, nil

	}, nil
}

func toLesserIterator(it RecordIterator) lesserIterator {
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

/*
//
// I thought this approach was going to be quicker as it limits the comparissons to log(nIterators)
// But it requires more allocations and thus becomes more expensive up until a very large nIterators
//
// SortedRecordIterator is a smarter but slower version. :'(
func SortedRecordIterator(iterators []RecordIterator) (RecordIterator, error) {
	return buildSortedRecordIterator(iterators...)
}
func buildSortedRecordIterator(iterators ...RecordIterator) (RecordIterator, error) {
	if len(iterators) == 1 {
		return iterators[0], nil
	}
	res := []RecordIterator{}
	var i int
	for i = 0; i < len(iterators)/2; i++ {
		subIt, err := sortedRecordIterator(toLesserIterator(iterators[i*2]), toLesserIterator(iterators[i*2+1]))
		if err != nil {
			return nil, err
		}
		res = append(res, subIt)
	}
	res = append(res, iterators[i*2:]...)
	return buildSortedRecordIterator(res...)
}

func sortedRecordIterator(it1, it2 lesserIterator) (RecordIterator, error) {
	peakValue1, err1 := it1()
	peakValue2, err2 := it2()
	return func() (interface{}, error) {
		// Return variables
		var returnVal interface{}
		var returnErr error

		// End of line?
		if err1 == ErrIteratorStop && err2 == ErrIteratorStop {
			return nil, ErrIteratorStop
		}
		// Any other error that should cause abort; Note that any abnormal error in any iterator will cascade
		if err1 != nil && err1 != ErrIteratorStop {
			return nil, err1
		}
		if err2 != nil && err2 != ErrIteratorStop {
			return nil, err2
		}

		// At this point we know
		// At least 1 iterator has a non nil error
		// At most 1 iterator has the error ErrIteratorStop
		if err1 == nil && err2 == nil {
			if peakValue1 == nil || peakValue2 == nil {
				return nil, ErrGotNilRecord
			}
			if peakValue1.Less(peakValue2) {
				returnVal = peakValue1
				returnErr = err1
				peakValue1, err1 = it1()
			} else {
				returnVal = peakValue1
				returnErr = err2
				peakValue2, err2 = it2()
			}
			return returnVal, returnErr
		} else if err1 == nil {
			if peakValue1 == nil {
				return nil, ErrGotNilRecord
			}
			// Only iterator 1 left
			returnVal = peakValue1
			returnErr = err1
			peakValue1, err1 = it1()
		} else if err2 == nil {
			if peakValue2 == nil {
				return nil, ErrGotNilRecord
			}
			// Only iterator 2 left
			returnVal = peakValue2
			returnErr = err2
			peakValue2, err2 = it2()
		} else {
			return nil, fmt.Errorf("wtf, not possible case err1:%v, err2:%v, peakValue1:%v, peakValue2:%v", err1, err2, peakValue1, peakValue2)
		}
		return returnVal, returnErr
	}, nil
}
*/
