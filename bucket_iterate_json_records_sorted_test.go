package gcsext_test

import (
	"testing"

	"cloud.google.com/go/storage"
	gcsext "github.com/kvanticoss/google-cloudstorage-ext"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type testStruct struct {
	Var1       string
	Var2, Var3 int
}

func (s *testStruct) Less(other interface{}) bool {
	os, ok := other.(*testStruct)
	if !ok {
		return false
	}
	return s.Var2 < os.Var2
}

func TestIterateJSONRecordByFolderSorted(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	it := gcsext.IterateJSONRecordsByFoldersSorted(
		ctx,
		client.Bucket(baseBucket),
		"artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_partition_streamer/",
		func() interface{} {
			return &testStruct{}
		},
		nil,
	)

	var prevRec interface{}
	var prevFolder string
	for folder, rec, err := it(); err == nil; folder, rec, err = it() {
		if prevRec != nil && folder == prevFolder {
			if !prevRec.(iterator.Lesser).Less(rec) {
				assert.Failf("not sorted", "records doesn't come in sorted order; (current)%v, is smaller than (previous)%v", rec, prevRec)
			}
		}
		prevFolder = folder
		prevRec = rec
	}
}
