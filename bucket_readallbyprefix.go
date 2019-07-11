package gcsext

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

const (
	bufferSize = 1024 * 1024 * 5 // 5 MBytes
)

var (
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
			//log.Printf("Trying to open next file :%s\n", objAttr.Name)

			//Is virtual folder? TODO: Maybe paramatrize "/"
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
				rawReader := or
				gz, err := gzip.NewReader(or)
				if err != nil {
					r.CloseWithError(err)
					return
				}
				//Wrap the gzip in a reader that cloeses underlying stream as well.
				or = &deepReadCloser{gz, rawReader}
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
