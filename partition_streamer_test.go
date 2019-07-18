package gcsext_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
)

var _ gcsext.PartitionGetter = &testRecordWithPartition{}

func TestMultiFileStreaming(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	bucket := client.Bucket(baseBucket)

	NewWriterFactory := func(path string) (gcsext.WriteCloser, error) {
		prefix := "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_partition_streamer"
		path = prefix + "/" + strings.Trim(path, "/")
		return bucket.Object(path).NewWriter(ctx), nil
	}

	count := 0
	assert.Equal(
		t,                      // test
		gcsext.ErrIteratorStop, // Expected
		gcsext.StreamJSONRecords( // actual
			ctx,
			NewWriterFactory,
			func() (interface{}, error) { // Record iterator; creates 5k records
				if count > 5000 {
					return nil, gcsext.ErrIteratorStop
				}
				count = count + 1
				return &testRecordWithPartition{
					Var1: fmt.Sprintf("teststring for index %d", count),
					Var2: count,
					Var3: float64(count),
				}, nil
			}),
		"Expected error to be stopErr")
}

type testRecordWithPartition struct {
	Var1 string
	Var2 int
	Var3 float64
}

func (r testRecordWithPartition) GetPartions() (gcsext.KeyValues, error) {
	return gcsext.KeyValues{
		gcsext.KeyValue{
			Key:   "bucket",
			Value: fmt.Sprintf("%d", r.Var2/100),
		},
		gcsext.KeyValue{
			Key:   "is_even",
			Value: strconv.FormatBool(r.Var2%2 == 0),
		},
	}, nil
}
