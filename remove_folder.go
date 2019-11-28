package gcsext

import (
	"cloud.google.com/go/storage"

	"golang.org/x/net/context"
)

// RemoveFolder remove all contents under the specificed prefix; unless a predicate function is present and returns false
// Will stop and return on first error
func RemoveFolder(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	predicate func(*storage.ObjectAttrs) bool,
) error {
	q := &storage.Query{
		Delimiter: "",
		Prefix:    prefix,
		Versions:  false,
	}
	objIt := bucket.Objects(ctx, q)
	for objAttr, err := objIt.Next(); err == nil; objAttr, err = objIt.Next() {
		if predicate != nil && !predicate(objAttr) {
			continue
		}
		if err := bucket.Object(objAttr.Name).Delete(ctx); err != nil {
			return err
		}
	}
	return nil
}
