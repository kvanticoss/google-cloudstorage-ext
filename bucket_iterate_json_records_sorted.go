package gcsext

import (
	"github.com/kvanticoss/goutils/iterator"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

// IterateJSONRecordsByFoldersSorted returns a RecordIterator with the guarratee that records will come in sorted order (assumes the record implements the Lesser interface
// and that each object in the GCS folder is saved in a sorted order). Files between folders are not guarranteed to be sorted as folders are read sequencially
func IterateJSONRecordsByFoldersSorted(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	new func() interface{},
	predicate func(*storage.ObjectAttrs) bool,
) func() (string, interface{}, error) {

	// Folder iterator. Will yeild readers for all elements in the folder
	folderIt := FolderReadersByPrefixWithFilter(ctx, bucket, prefix, predicate)
	fetchNextSortedFolderIt := func() (string, iterator.RecordIterator, error) {
		folder, readers, err := folderIt()
		if err != nil {
			return "", nil, err
		}
		iterators := make([]iterator.RecordIterator, len(readers))
		for index, reader := range readers {
			iterators[index] = iterator.JSONRecordIterator(new, reader)
		}
		it, err := iterator.SortedRecordIterators(iterators)
		return folder, it, err
	}

	var currentFolderIterator iterator.RecordIterator
	var err error
	var lastRecord interface{}
	var lastFolder string
	return func() (string, interface{}, error) {
		if currentFolderIterator == nil && err == nil {
			lastFolder, currentFolderIterator, err = fetchNextSortedFolderIt()
			if err != nil {
				return "", nil, err
			}
		}

		lastRecord, err = currentFolderIterator()
		for err == iterator.ErrIteratorStop {
			if lastFolder, currentFolderIterator, err = fetchNextSortedFolderIt(); err != nil {
				return "", nil, err
			}
			lastRecord, err = currentFolderIterator()
		}

		return lastFolder, lastRecord, err
	}
}

// IterateJSONRecordsByFoldersSortedCB works like IterateJSONRecordsByFoldersSorted but through an callback pattern
func IterateJSONRecordsByFoldersSortedCB(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	new func() interface{},
	predicate func(*storage.ObjectAttrs) bool,
	callback func(string, func() (interface{}, error)) error,
) error {
	it := IterateJSONRecordsByFoldersSorted(ctx, bucket, prefix, new, predicate)

	nextFolder := ""
	previousFolder, previousRecord, err := it()
	if err != nil {
		return err
	}

	for err == nil {
		err = callback(previousFolder, func() (interface{}, error) {
			if nextFolder != previousFolder && nextFolder != "" {
				err = iterator.ErrIteratorStop
				return nil, err
			}
			defer func() {
				// Only fetch next if we are clear of errors
				if err == nil {
					nextFolder, previousRecord, err = it()
				}
			}()
			return previousRecord, err
		})
		if err != nil && err != iterator.ErrIteratorStop {
			return err
		}
		if nextFolder == "" {
			return err
		}
		previousFolder = nextFolder
		err = nil
	}
	return err
}
