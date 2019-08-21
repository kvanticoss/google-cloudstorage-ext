package gcsext_test

import (
	"log"
	"testing"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"github.com/stretchr/testify/assert"
)

type SortableStruct struct {
	Val int
}

// Less answers if "other" is Less (should be sorted before) this struct
func (s *SortableStruct) Less(other interface{}) bool {
	otherss, ok := other.(*SortableStruct)
	if !ok {
		log.Printf("Type assertion failed in SortableStruct; got other of type %#v", other)
		return true
	}
	res := s.Val < otherss.Val
	return res
}

func getRecordIterator(multiplier, max int) gcsext.RecordIterator {
	i := 0
	return func() (interface{}, error) {
		i = i + 1
		if i <= max {
			return &SortableStruct{
				Val: i * multiplier,
			}, nil
		}
		return nil, gcsext.ErrIteratorStop
	}
}

func TestSortedRecordIterator(t *testing.T) {

	e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
		getRecordIterator(1, 3),
		getRecordIterator(2, 12),
		getRecordIterator(4, 7),
		getRecordIterator(1, 10),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	count := 0
	lastVal := 0
	var r interface{}
	for r, err = e(); err == nil; r, err = e() {
		record, ok := r.(*SortableStruct)
		if !ok {
			t.Error("Failure in type assertion to SortableStruct")
		}
		if record.Val < lastVal {
			t.Errorf("Expected each record value to be higher than the last; got %v", record.Val)
		}
		count++
		lastVal = record.Val
	}

	if err != gcsext.ErrIteratorStop {
		t.Errorf("Expected only error to be IteratorStop; Got %v", err)
	}

	expectedCount := (3 + 12 + 7 + 10)
	if count != expectedCount {
		t.Errorf("Sorted Iterator skipped some records; all should be processed. Only processed %d / %d records", count, expectedCount)
	}

	if lastVal == 0 {
		t.Error("Record emitter didn't yeild any records")
	}
}

func BenchmarkSortedRecordIterator(b *testing.B) {
	b.Run("1000 rows x 2 streams", func(b *testing.B) {
		amount := 1000
		for n := 0; n < b.N; n++ {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
					getRecordIterator(1, amount),
					getRecordIterator(2, amount)})
				if err != nil {
					b.Fatal(err)
					return
				}
				b.StartTimer()
				for _, err := e(); err == nil; _, err = e() {
				}
			}
		}
	})

	b.Run("10000 rows x 2 streams", func(b *testing.B) {
		amount := 10000
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
				getRecordIterator(1, amount),
				getRecordIterator(2, amount)})
			if err != nil {
				b.Fatal(err)
				return
			}
			b.StartTimer()
			for _, err := e(); err == nil; _, err = e() {
			}
		}
	})
	b.Run("10000 rows x 10 streams", func(b *testing.B) {
		amount := 10000
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(2, amount),
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(2, amount),
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(10, amount),
				getRecordIterator(6, amount)})
			if err != nil {
				b.Fatal(err)
				return
			}
			b.StartTimer()
			for _, err := e(); err == nil; _, err = e() {
			}
		}
	})

	b.Run("1M rows x 2 streams", func(b *testing.B) {
		amount := 1000000
		for n := 0; n < b.N; n++ {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
					getRecordIterator(1, amount),
					getRecordIterator(2, amount)})
				if err != nil {
					b.Fatal(err)
					return
				}
				b.StartTimer()
				for _, err := e(); err == nil; _, err = e() {
				}
			}
		}
	})

	b.Run("1M rows x 10 streams", func(b *testing.B) {
		amount := 1000000
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(2, amount),
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(2, amount),
				getRecordIterator(1, amount),
				getRecordIterator(2, amount),
				getRecordIterator(10, amount),
				getRecordIterator(6, amount)})
			if err != nil {
				b.Fatal(err)
				return
			}
			b.StartTimer()
			for _, err := e(); err == nil; _, err = e() {
			}
		}
	})

}

func BenchmarkLargeBench(b *testing.B) {
	amount := 1000000
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		e, err := gcsext.SortedRecordIterator([]gcsext.RecordIterator{
			getRecordIterator(1, amount),
			getRecordIterator(2, amount),
			getRecordIterator(2, amount),
			getRecordIterator(2, amount),
			getRecordIterator(3, amount),
			getRecordIterator(5, amount),
			getRecordIterator(2, amount),
			getRecordIterator(7, amount),
			getRecordIterator(2, amount),
			getRecordIterator(10, amount)})
		if err != nil {
			b.Fatal(err)
			return
		}
		b.StartTimer()
		for _, err := e(); err == nil; _, err = e() {
		}
	}
}
