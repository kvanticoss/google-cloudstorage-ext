package gcsext

import (
	"net/http"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"golang.org/x/net/context"
)

// TouchFile ensures a files exists by creating it if it doesn't exists and/or returning it otherwise.
func TouchFile(
	ctx context.Context,
	bucket *storage.BucketHandle,
	path string,
) (*storage.ObjectHandle, error) {
	w := bucket.Object(path).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)

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
