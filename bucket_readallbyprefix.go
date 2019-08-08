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
// only objects that filter(*storage.ObjectAttrs) bool returns true will be kept
// First error will close the stream.
func ReadFilteredByPrefix(ctx context.Context, bucket *storage.BucketHandle, prefix string, filter func(*storage.ObjectAttrs) bool) (io.ReadCloser, error) {
	// Deafult is to always keep everything
	if filter == nil {
		return nil, fmt.Errorf("Must provide filter-function; To read everything use ReadAllByPrefix")
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

	go func() {
		for {
			var or io.ReadCloser
			objAttr, err := it.Next()
			if err == iterator.Done {
				log.Printf("Iterator done")
				bufferdWriter.Flush()
				w.Close()
				break
			}
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if !filter(objAttr) {
				continue
			}

			// Is virtual folder?
			// TODO: Maybe paramatrize "/"
			if strings.HasSuffix(objAttr.Name, "/") && bytes.Equal(objAttr.MD5, placeholderMD5) {
				continue
			}

			log.Printf("GotReader next file :%s\n", objAttr.Name)
			or, err = bucket.Object(objAttr.Name).NewReader(ctx)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			if strings.HasSuffix(objAttr.Name, ".gz") {
				//log.Printf("Decompressing next file :%s\n", objAttr.Name)
				or, err = gzip.NewReader(or)
				if err != nil {
					r.CloseWithError(err)
					return
				}
			}

			//log.Printf("Copying data next file :%s\n", objAttr.Name)
			if _, err = io.Copy(bufferdWriter, or); err != nil {
				bufferdWriter.Flush()
				w.CloseWithError(err)
				return
			}

			bufferdWriter.Flush()
			or.Close()
		}
	}()

	return r, nil
}

// IterateJSONRecordsFilteredByPrefix returns a RecordIterator with the guarratee that records will come in sorted order (assumes the record implements the Lesser interface
// and that each file is saved in a sorted order)
func IterateJSONRecordsFilteredByPrefix(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	new func() interface{},
	filter func(*storage.ObjectAttrs) bool,
) (RecordIterator, error) {
	// Deafult is to always keep everything
	if filter == nil {
		return nil, fmt.Errorf("Must provide filter-function; To read everything use ReadAllByPrefix")
	}

	q := &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	}
	it := bucket.Objects(ctx, q)
	log.Printf("Reading query: %#v", q)

	// Let's not require the Lesser interface to be implemented.
	var itSorter func(iterators []RecordIterator) (RecordIterator, error)
	tmp := new()
	if _, ok := tmp.(Lesser); ok {
		itSorter = SortedRecordIterator
	} else {
		itSorter = func(iterators []RecordIterator) (RecordIterator, error) {
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
			return f, nil
		}
	}

	// Default iterator
	currentFolderFileIterators := []RecordIterator{}
	currentFolderIterator := func() (interface{}, error) {
		return nil, ErrIteratorStop
	}
	return func() (interface{}, error) {
		rec, err := currentFolderIterator()
		if err == nil {
			return rec, err
		}
		if err != ErrIteratorStop {
			return rec, err
		}

		previousFolder := ""
		for {
			var or io.ReadCloser
			var err error
			objAttr, err := it.Next()
			if err == iterator.Done {
				if len(currentFolderFileIterators) > 0 {
					currentFolderIterator, err = itSorter(currentFolderFileIterators)
					if err != nil {
						return nil, err
					}
					currentFolderFileIterators = []RecordIterator{}
					return currentFolderIterator()
				} else {
					return nil, ErrIteratorStop
				}
			}
			if err != nil {
				return nil, err
			}
			if !filter(objAttr) {
				continue
			}
			// Is virtual folder?
			// TODO: Maybe paramatrize "/"
			if strings.HasSuffix(objAttr.Name, "/") && bytes.Equal(objAttr.MD5, placeholderMD5) {
				continue
			}

			// Fetch a proper reader
			or, err = bucket.Object(objAttr.Name).NewReader(ctx)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(objAttr.Name, ".gz") {
				or, err = gzip.NewReader(or)
				if err != nil {
					return nil, err
				}
			}

			// Cools; let's create an interator
			currentFileIterator := JSONRecordIterator(new, or)

			currentFolder := objAttr.Name[:strings.LastIndex(objAttr.Name, "/")]
			if currentFolder == previousFolder || previousFolder == "" {
				currentFolderFileIterators = append(currentFolderFileIterators, currentFileIterator)
			} else {
				currentFolderIterator, err = itSorter(currentFolderFileIterators)
				if err != nil {
					return nil, err
				}
				currentFolderFileIterators = []RecordIterator{currentFileIterator}
				return currentFolderIterator()
			}
		}
	}, nil
}
