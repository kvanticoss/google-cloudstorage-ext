package gcsext

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
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

	it := bucket.Objects(ctx, &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	})

	r, w := io.Pipe()
	bufferdWriter := bufio.NewWriterSize(w, bufferSize)

	go func() {
		for {
			var or io.ReadCloser
			objAttr, err := it.Next()
			if err == iterator.Done {
				w.Close()
				break
			}
			if err != nil {
				w.CloseWithError(err)
				return
			}

			//Is virtual folder? TODO: Maybe paramatrize "/"
			if strings.HasSuffix(objAttr.Name, "/") && bytes.Equal(objAttr.MD5, placeholderMD5) {
				continue
			}

			or, err = bucket.Object(objAttr.Name).NewReader(ctx)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			if strings.HasSuffix(objAttr.Name, ".gz") {
				rawReader := or
				gz, err := gzip.NewReader(or)
				if err != nil {
					r.CloseWithError(err)
					return
				}
				//Wrap the gzip in a reader that cloeses underlying stream as well.
				or = &readDeepCloser{gz, rawReader}
			}

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

type readDeepCloser struct {
	*gzip.Reader
	underlyingReader io.ReadCloser
}

func (gz *readDeepCloser) Close() error {
	if err := gz.Reader.Close(); err != nil {
		return err
	}

	if err := gz.underlyingReader.Close(); err != nil {
		return err
	}

	return nil
}
