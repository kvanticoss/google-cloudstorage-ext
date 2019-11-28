package gcsext_test

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	gcsext "github.com/kvanticoss/google-cloudstorage-ext"
	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/recordbuffer"
	"github.com/kvanticoss/goutils/recordwriter"
	"github.com/kvanticoss/goutils/writercache"
	"github.com/kvanticoss/goutils/writerfactory"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestSortGCSFolders(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	records := 200
	bucket := client.Bucket(baseBucket)
	prefix := "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_sort_gcs_folders2/"
	createdFiles := map[string]bool{}

	gcsext.RemoveFolder(ctx, bucket, prefix, nil)

	// Reset bucket handle to avoid timing issues
	bucket = client.Bucket(baseBucket)

	gcsWf := gcsext.GetGCSWriterFactory(ctx, bucket)
	var wf writerfactory.WriterFactory = func(path string) (wc eioutil.WriteCloser, err error) {
		createdFiles[path] = true
		return gcsWf(path)
	}
	cache := writercache.NewCache(ctx, wf.WithPrefix(prefix), time.Minute, 10000)

	// Create some sample data
	log.Printf("Creating sample data")
	assert.EqualError(recordwriter.NewLineJSONPartitioned(
		iterator.NewBufferedRecordIteratorBTree(test_utils.DummyIterator(1, 1, records), records/5).ToRecordIterator(),
		cache.GetWriter,
		nil,
	), iterator.ErrIteratorStop.Error())

	assert.NoError(cache.Close())

	assert.NoError(gcsext.SortGCSFolders(
		ctx,
		bucket,
		prefix,
		func() iterator.Lesser {
			return test_utils.NewDummyRecordPtr()
		},
		func(obj *storage.ObjectAttrs) bool {
			_, ok := createdFiles[obj.Name] // only read files we have created this session
			return ok                       // strings.Contains(obj.Name, "unsorted_records_s")
		},
		"sorted.json.gz",
		func() recordbuffer.ReadWriteResetter {
			return &bytes.Buffer{}
		},
		nil,
		true,
		true,
	))

	it := gcsext.IterateJSONRecordsByFoldersSorted(
		ctx,
		bucket,
		prefix,
		func() interface{} {
			return test_utils.NewDummyRecordPtr()
		},
		func(obj *storage.ObjectAttrs) bool {
			log.Printf("checking for sorted.json.gz: %s", obj.Name)
			return strings.Contains(obj.Name, "sorted.json.gz")
		},
	)

	found := 0
	for _, rec, err := it(); err == nil; _, rec, err = it() {
		assert.NoError(err)
		assert.NotNil(rec)
		found++
	}

	assert.Equal(records, found)
}
