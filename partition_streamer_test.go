package gcsext_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"github.com/stretchr/testify/assert"
)

var _ gcsext.PartitionGetter = &testRecordWithPartition{}

func TestMultiFileStreaming(t *testing.T) {
	memDb := map[string][]byte{}

	NewWriterFactory := func(path string) (gcsext.WriteCloser, error) {
		prefix := "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_partition_streamer"
		path = prefix + "/" + strings.Trim(path, "/")

		/*
			os.MkdirAll(filepath.Dir("./testfiles/"+path), os.ModePerm)
			w, err := os.Create("./testfiles/" + path)
			if err != nil {
				panic(err)
			}
			return w, err
		*/

		buffer := []byte{}
		memDb[path] = buffer
		return gcsext.NopeWriteCloserr{bytes.NewBuffer(buffer)}, nil

	}

	count := 0
	assert.Equal(
		t,                      // test
		gcsext.ErrIteratorStop, // Expected
		gcsext.StreamJSONRecords( // actual
			context.Background(),
			NewWriterFactory,
			func() (interface{}, error) { // Record iterator; creates 5k records
				time.Sleep(time.Microsecond)
				if count > 50 {
					return nil, gcsext.ErrIteratorStop
				}
				count = count + 1
				return &testRecordWithPartition{
					Var1: fmt.Sprintf("teststring for index %d", count),
					Var2: count % 8,
					Var4: float64(count),
				}, nil
			}, time.Millisecond*150),
		"Expected error to be stopErr")

	assert.NotEmpty(t, memDb, "Expected our storage writer (mem) to contain data")
}

type testRecordWithPartition struct {
	Var1 string
	Var2 int
	Var4 float64
}

func (r *testRecordWithPartition) GetPartions() (gcsext.KeyValues, error) {
	return gcsext.KeyValues{
		gcsext.KeyValue{
			Key:   "bucket",
			Value: fmt.Sprintf("%d", r.Var2/500),
		},
		gcsext.KeyValue{
			Key:   "is_even",
			Value: strconv.FormatBool(r.Var2%2 == 0),
		},
	}, nil
}

func (r *testRecordWithPartition) Lesss(other interface{}) bool {
	if o, ok := other.(*testRecordWithPartition); ok {
		return r.Var2 < o.Var2 // to get some variability
	} else {
		panic(other) //only for testing purposes, should never happen
	}
	return false
}
