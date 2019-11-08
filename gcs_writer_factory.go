package gcsext

import (
	"context"
	"path"

	"cloud.google.com/go/storage"
	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/writerfactory"
)

// GetGCSWriterFactory returns a writer factory backed by GCS
func GetGCSWriterFactory(ctx context.Context, bucket *storage.BucketHandle) writerfactory.WriterFactory {
	return func(filePath string) (wc eioutil.WriteCloser, err error) {
		return bucket.Object(path.Clean(filePath)).NewWriter(ctx), nil
	}
}
