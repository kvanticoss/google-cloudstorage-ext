package gcsext

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/kvanticoss/google-cloudstorage-ext/gzip"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

const (
	bufferSize = 1024 * 1024 * 5 // 5 MBytes
)

var (
	// placeholderMD5 is the MD5 hash for a virtual blob-objected used to denote folders in GCS, the blobs only contains the text "placeholder"
	placeholderMD5 = []byte{0x6a, 0x99, 0xc5, 0x75, 0xab, 0x87, 0xf8, 0xc7, 0xd1, 0xed, 0x1e, 0x52, 0xe7, 0xe3, 0x49, 0xce}
)

// ReadAllByPrefix Reads all files one into 1 combined bytestream. Autoamtically handles decompression of .gz
// First error will close the stream.
func ReadAllByPrefix(ctx context.Context, bucket *storage.BucketHandle, prefix string) (io.ReadCloser, error) {
	return ReadFilteredByPrefix(ctx, bucket, prefix, func(_ *storage.ObjectAttrs) bool {
		return true
	})
}

// ReadFilteredByPrefix Reads all files one into 1 combined bytestream. Autoamtically handles decompression of .gz
// only objects that predicate(*storage.ObjectAttrs) bool returns true will be kept
// First error will close the stream.
func ReadFilteredByPrefix(ctx context.Context, bucket *storage.BucketHandle, prefix string, predicate func(*storage.ObjectAttrs) bool) (io.ReadCloser, error) {
	// Deafult is to always keep everything
	if predicate == nil {
		return nil, fmt.Errorf("Must provide predicate-function; To read everything use ReadAllByPrefix")
	}

	q := &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	}
	it := bucket.Objects(ctx, q)
	log.Printf("Reading query: %#v", q)

	r, w := io.Pipe()
	bufferdWriter := bufio.NewWriterSize(w, bufferSize)

	predicate = CombineFilters(predicate, FilterOutVirtualGcsFolders)
	readerIterator := gcsObjectIteratorToReaderIterator(ctx, bucket, it, predicate)
	go func() {
		for {
			_, or, err := readerIterator()
			if err == iterator.Done {
				bufferdWriter.Flush()
				w.Close()
				break
			}
			if err != nil {
				_ = w.CloseWithError(err)
				return
			}

			//log.Printf("Copying data next file :%s\n", objAttr.Name)
			if _, err = io.Copy(bufferdWriter, or); err != nil {
				bufferdWriter.Flush()
				_ = w.CloseWithError(err)
				return
			}

			bufferdWriter.Flush()
			or.Close()
		}
	}()

	return r, nil
}

func FilterOutVirtualGcsFolders(objAttr *storage.ObjectAttrs) bool {
	return !(strings.HasSuffix(objAttr.Name, "/") && bytes.Equal(objAttr.MD5, placeholderMD5))
}

func CombineFilters(filters ...func(*storage.ObjectAttrs) bool) func(*storage.ObjectAttrs) bool {
	return func(o *storage.ObjectAttrs) bool {
		for _, f := range filters {
			if !f(o) {
				return false
			}
		}
		return true
	}
}

// gcsObjectIteratorToReaderIterator wraps common functionality
func gcsObjectIteratorToReaderIterator(
	ctx context.Context,
	bucket *storage.BucketHandle,
	it *storage.ObjectIterator,
	predicate func(*storage.ObjectAttrs) bool,
) func() (string, io.ReadCloser, error) {
	var iterator func() (string, io.ReadCloser, error)
	iterator = func() (string, io.ReadCloser, error) {
		var or io.ReadCloser
		objAttr, err := it.Next()
		if err != nil {
			return "", nil, err
		}
		if !predicate(objAttr) {
			return iterator() //works as continue inside an interator
		}

		or, err = bucket.Object(objAttr.Name).NewReader(ctx)
		if err != nil {
			return "", nil, err
		}

		if strings.HasSuffix(objAttr.Name, ".gz") {
			or, err = gzip.NewReader(or)
			if err != nil {
				return "", nil, err
			}
		}

		return objAttr.Name, or, nil
	}
	return iterator
}

// IterateJSONRecordsFilteredByPrefix returns a RecordIterator with the guarratee that records will come in sorted order (assumes the record implements the Lesser interface
// and that each file is saved in a sorted order)
func IterateJSONRecordsFilteredByPrefix(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	new func() interface{},
	predicate func(*storage.ObjectAttrs) bool,
) (RecordIterator, error) {
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
	iteratorsByFolder := map[string][]RecordIterator{}
	lastFolderName := ""

	// Create an interim null iterator to simplify our logic further down.
	folderIterator := func() (interface{}, error) { return nil, ErrIteratorStop }

	// Will yeild an interator combining all files inside a folder.
	return func() (interface{}, error) {
		if rec, err := folderIterator(); err == nil || err != ErrIteratorStop { // All good, or totaly bad?
			return rec, err
		}

		for {
			fileName, reader, err := readerIterator()
			if err != nil {
				break
			}
			folderName := fileName[:strings.LastIndex(fileName, "/")]
			iteratorsByFolder[folderName] = append(iteratorsByFolder[folderName], JSONRecordIterator(new, reader))
			if folderName != lastFolderName && lastFolderName != "" {
				lastFolderName = folderName
				break
			}
			lastFolderName = folderName
		}
		folderIterator = combineIteartors(iteratorsByFolder[lastFolderName])
		return folderIterator()
	}, nil
}

func combineIteartors(iterators []RecordIterator) RecordIterator {
	var f func() (interface{}, error)
	f = func() (interface{}, error) {
		if len(iterators) == 0 {
			return nil, ErrIteratorStop
		}
		rec, err := iterators[0]()
		if err == ErrIteratorStop {
			iterators = iterators[1:]
			return f()
		}
		return rec, err
	}
	return f
}
