package gcsext

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/kvanticoss/goutils/gzip"
	"github.com/kvanticoss/goutils/iterator"

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

	r, w := io.Pipe()
	bufferdWriter := bufio.NewWriterSize(w, bufferSize)

	predicate = CombineFilters(FilterOutVirtualGcsFolders, predicate)
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

// ReadFoldersByPrefixWithFilter Reads all files one into 1 combined bytestream per folder, autoamtically handles decompression of .gz
// only objects that predicate(*storage.ObjectAttrs) bool returns true will be kept First error will close the stream.
func ReadFoldersByPrefixWithFilter(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	predicate func(*storage.ObjectAttrs) bool,
) func() (string, io.ReadCloser, error) {
	q := &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	}
	it := bucket.Objects(ctx, q)
	predicate = CombineFilters(predicate, FilterOutVirtualGcsFolders)
	readerIterator := gcsObjectIteratorToReaderIterator(ctx, bucket, it, predicate)

	type resTuple struct {
		folder string
		r      io.ReadCloser
	}
	nextFolder := make(chan *resTuple)

	go func() {
		lastFolderName := ""
		var r io.ReadCloser
		var bw *bufio.Writer
		var w *io.PipeWriter

		closeWriters := func(err error) {
			if bw != nil {
				bw.Flush()
			}
			if w != nil {
				_ = w.CloseWithError(err)
			}
			if err != nil {
				close(nextFolder)
			}
		}

		for {
			fileName, or, err := readerIterator()
			if err == googleIterator.Done {
				closeWriters(nil) // regular EOF for this folder
				close(nextFolder) // but the iterator needs to know there's nothing more
				return
			}
			if err != nil {
				closeWriters(err)
				return
			}

			currentFolder := path.Dir(fileName)
			if currentFolder != lastFolderName {
				if w != nil {
					closeWriters(nil)
				}
				r, bw, w = newBufferedPipe()
				nextFolder <- &resTuple{currentFolder, r}
				lastFolderName = currentFolder
			}

			_, err = io.Copy(bw, or)
			if err != nil {
				closeWriters(err)
				return
			}
		}
	}()

	// Folter iterator
	return func() (string, io.ReadCloser, error) {
		select {
		case r := <-nextFolder:
			if r == nil {
				return "", nil, iterator.ErrIteratorStop
			}
			return r.folder, r.r, nil
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}
}

// FolderReadersByPrefixWithFilter returns an interator which in turn returns (potentially uncompressed gzip) readers for each unique folder found under the prefix
func FolderReadersByPrefixWithFilter(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	predicate func(*storage.ObjectAttrs) bool,
) func() (string, []io.ReadCloser, error) {
	q := &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	}
	it := bucket.Objects(ctx, q)
	predicate = CombineFilters(predicate, FilterOutVirtualGcsFolders)
	readerIterator := gcsObjectIteratorToReaderIterator(ctx, bucket, it, predicate)

	var lastFolderName string
	var previousBatch []io.ReadCloser

	fetchFolderBatch := func() (folder string, res []io.ReadCloser, err error) {
		res = previousBatch
		for {
			fileName, or, err2 := readerIterator()
			// Ensure to flush the previous batch of elements we got.
			if err2 != nil && previousBatch != nil {
				previousBatch = nil
				return lastFolderName, res, nil
			}
			if err2 == googleIterator.Done {
				return "", nil, iterator.ErrIteratorStop
			}
			if err2 != nil {
				return "", nil, err2
			}

			currentFolder := path.Dir(fileName)
			if currentFolder != lastFolderName && lastFolderName != "" {
				// anonoumous returns are uggly but here they kinda work well
				previousBatch = []io.ReadCloser{or}
				folder = lastFolderName
				err = nil
				lastFolderName = currentFolder // save for next iteration
				return
			}

			lastFolderName = currentFolder
			res = append(res, or)
		}
	}

	// Folter iterator
	return func() (string, []io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}

		return fetchFolderBatch()
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

func newBufferedPipe() (io.ReadCloser, *bufio.Writer, *io.PipeWriter) {
	r, w := io.Pipe()
	bufferdWriter := bufio.NewWriterSize(w, bufferSize)
	return r, bufferdWriter, w
}
