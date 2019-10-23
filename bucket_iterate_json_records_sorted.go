package gcsext

import (
	"fmt"
	"strings"

	"github.com/kvanticoss/goutils/iterator"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

// IterateJSONRecordsFilteredByPrefix returns a RecordIterator with the guarratee that records will come in sorted order (assumes the record implements the Lesser interface
// and that each object in GCS folder is saved in a sorted order). Files between folders are not guarranteed to be sorted
func IterateJSONRecordsFilteredByPrefix(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	new func() interface{},
	predicate func(*storage.ObjectAttrs) bool,
) (iterator.RecordIterator, error) {
	// Deafult is to always keep everything
	if predicate == nil {
		return nil, fmt.Errorf("Must provide filter-function; To read everything use ReadAllByPrefix")
	}

	it := bucket.Objects(ctx, &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	})

	predicate = CombineFilters(predicate, FilterOutVirtualGcsFolders)
	readerIterator := gcsObjectIteratorToReaderIterator(ctx, bucket, it, predicate)

	iteratorsByFolder := map[string][]iterator.RecordIterator{}
	lastFolderName := ""

	var err error
	// Create an interim null iterator to simplify our logic further down.
	folderIterator := func() (interface{}, error) { return nil, iterator.ErrIteratorStop }

	// Will yeild an interator combining all files inside a folder.
	return func() (interface{}, error) {
		if rec, err := folderIterator(); err == nil || err != iterator.ErrIteratorStop { // Not an error; OR any error other than the one we can handle (IteratorStop)
			return rec, err
		}

		for {
			fileName, reader, err := readerIterator()
			if err != nil {
				break
			}
			folderName := fileName[:strings.LastIndex(fileName, "/")]
			iteratorsByFolder[folderName] = append(
				iteratorsByFolder[folderName],
				iterator.JSONRecordIterator(new, reader),
			)
			if folderName != lastFolderName && lastFolderName != "" {
				lastFolderName = folderName
				break
			}
			lastFolderName = folderName
		}
		folderIterator, err = iterator.SortedRecordIterators(iteratorsByFolder[lastFolderName])
		if err != nil {
			return nil, err
		}
		return folderIterator()
	}, nil
}
