package gcsext

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/kvanticoss/goutils/gzip"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	googleIterator "google.golang.org/api/iterator"
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
			if err == googleIterator.Done {
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
