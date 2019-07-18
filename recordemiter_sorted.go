package gcsext

import "fmt"

var (
	ErrNotLesser = fmt.Errorf("Record does not implement the Lesser interface and can't be sorted")
)

// SortedRecordIterator does what it sounds like.
func SortedRecordIterator(iterators []RecordIterator) (RecordIterator, error) {
	// Create a list of destionation objects with the same type as Lesser
	nextCandidates := make([]Lesser, len(iterators))
	for i, ri := range iterators {
		nextCandidates[i] = nil

		record, err := ri()
		if err == nil {
			if l, ok := record.(Lesser); ok {
				nextCandidates[i] = l
			} else {
				return nil, ErrNotLesser
			}
		} else if err != ErrIteratorStop {
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

			if !bestCandidate.(Lesser).Less(candidate) {
				bestIndex = i
				bestCandidate = candidate
			}
		}

		if bestIndex == -1 {
			return nil, ErrIteratorStop
		}

		nextRecord, err := iterators[bestIndex]()
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
