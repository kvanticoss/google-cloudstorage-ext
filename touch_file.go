package gcsext

import (
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/kvanticoss/goutils/gzip"
	"google.golang.org/api/googleapi"

	"golang.org/x/net/context"
)

// TouchFile ensures a files exists by creating it if it doesn't exists and/or returning it otherwise. Will add
// gzip headers if the file ends in ".gz"
func TouchFile(
	ctx context.Context,
	bucket *storage.BucketHandle,
	path string,
) (*storage.ObjectHandle, error) {

	var w io.WriteCloser = bucket.Object(path).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if strings.HasSuffix(path, ".gz") {
		w = gzip.NewWriter(w) // Write gzip headers if needed
	}

	err := w.Close()
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == http.StatusPreconditionFailed {
			// Expected for when the file exits
			return bucket.Object(path), nil
		}
		// Anything else is not so expected
		return nil, err
	}
	return bucket.Object(path), nil
}
