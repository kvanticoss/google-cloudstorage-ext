package gcsext_test

import (
	"context"
	"sync"
	"testing"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"github.com/stretchr/testify/assert"
)

type testLesser int

func (i testLesser) Less(other interface{}) bool {
	return i < other.(testLesser)
}

func TestSortedRecordWriterNoWriteAfterClose(t *testing.T) {
	SRW := gcsext.NewSortedRecordWriter(context.Background(), func(_ string, _ gcsext.Lesser) {})
	SRW.Close()
	assert.Error(t, SRW.WriteRecord(testLesser(1)), "Shouldn't be possible to write after close")
}

func TestSortedRecordWriterContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(10)

	paths := []string{}
	records := []gcsext.Lesser{}
	bucketSortOrder := map[string]gcsext.Lesser{}
	callback := func(path string, record gcsext.Lesser) {
		paths = append(paths, path)
		records = append(records, record)

		// Ensure that our ordering is correct
		if prevRecord, ok := bucketSortOrder[path]; ok {
			assert.True(
				t,
				prevRecord.Less(record),
				"Expected all new records to be greater than the previously written records (within each bucketID)")
		}
		bucketSortOrder[path] = record

		wg.Done()
	}

	SRW := gcsext.NewSortedRecordWriter(ctx, callback)
	SRW.WriteRecord(testLesser(1))  // 1 st bucket
	SRW.WriteRecord(testLesser(4))  // 1 st bucket
	SRW.WriteRecord(testLesser(2))  // 1 st bucket since no write yet
	SRW.WriteRecord(testLesser(20)) // 1 nd bucket
	SRW.WriteRecord(testLesser(6))  // 1 nd bucket
	SRW.WriteRecord(testLesser(7))  // 1 nd bucket
	SRW.WriteRecord(testLesser(9))  // 1 nd bucket
	SRW.WriteRecord(testLesser(3))  // 1 st bucket since no write yet
	SRW.WriteRecord(testLesser(12)) // 1 nd bucket
	SRW.WriteRecord(testLesser(11)) // 1 nd bucket

	assert.Empty(t, paths, "Expected all records to be cached")
	cancel()
	wg.Wait()
	assert.Len(t, paths, 10, "Expected all records to be flushed when the context is cancelled")
}

func TestSortedRecordWriterContextNoCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(10)

	paths := []string{}
	records := []gcsext.Lesser{}
	bucketSortOrder := map[string]gcsext.Lesser{}
	callback := func(path string, record gcsext.Lesser) {
		paths = append(paths, path)
		records = append(records, record)

		// Ensure that our ordering is correct
		if prevRecord, ok := bucketSortOrder[path]; ok {
			assert.Truef(
				t,
				prevRecord.Less(record),
				"Expected all new records to be greater than the previously written records (within each bucketID); got %d < %d on bucket %s", prevRecord, record, path)
		}
		bucketSortOrder[path] = record

		wg.Done()
	}

	SRW := gcsext.NewSortedRecordWriter(ctx, callback, gcsext.WithMaxCacheSize(1))
	SRW.WriteRecord(testLesser(1))  // 1 st bucket
	SRW.WriteRecord(testLesser(4))  // 1 st bucket
	SRW.WriteRecord(testLesser(2))  // New bucket as 2 is smaller than 4
	SRW.WriteRecord(testLesser(20)) // 1 nd bucket
	SRW.WriteRecord(testLesser(6))  // 2 nd bucket
	SRW.WriteRecord(testLesser(7))  // 2 nd bucket
	SRW.WriteRecord(testLesser(9))  // 2 nd bucket
	SRW.WriteRecord(testLesser(3))  // new bucket
	SRW.WriteRecord(testLesser(12)) // 2 nd bucket
	SRW.WriteRecord(testLesser(11)) // 2 nd bucket

	assert.NotEmpty(t, paths, "Expected all records to be flushed")
	cancel()
	wg.Wait()
	assert.Len(t, paths, 10, "Expected all records to be flushed when the context is cancelled")
	assert.Len(t, bucketSortOrder, 3, "The test data to yeild 3 unique bucketIds")
}

func TestSortedRecordWriterConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Wait for the callbacks
	wg := sync.WaitGroup{}
	wg.Add(10)

	// Wait for the producers
	wg2 := sync.WaitGroup{}
	wg2.Add(10)

	callback := func(path string, record gcsext.Lesser) {
		wg.Done()
	}

	SRW := gcsext.NewSortedRecordWriter(ctx, callback)
	addOne := func(r gcsext.Lesser) {
		t.Logf("Flushing record %v", r)
		err := SRW.WriteRecord(r)
		if err != nil {
			t.Error(err)
		}
		wg2.Done()
	}

	go addOne(testLesser(1))  // 1 st bucket
	go addOne(testLesser(4))  // 1 st bucket
	go addOne(testLesser(2))  // New bucket as 2 is smaller than 4
	go addOne(testLesser(20)) // 1 nd bucket
	go addOne(testLesser(6))  // 2 nd bucket
	go addOne(testLesser(7))  // 2 nd bucket
	go addOne(testLesser(9))  // 2 nd bucket
	go addOne(testLesser(3))  // new bucket
	go addOne(testLesser(12)) // 2 nd bucket
	go addOne(testLesser(11)) // 2 nd bucket

	wg2.Wait()
	cancel()
	wg.Wait()
}
