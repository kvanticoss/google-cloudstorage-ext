### google-cloudstorage-ext - Utilities for working with google cloud storage

### testing
will unfourtuntely fail for non Kvantic users as list-permissions are required; will maybe be fixed later.

## Examples
### ReadAllByPrefix(ctx context.Context, bucket *storage.BucketHandle, prefix string) (io.ReadCloser, error)

```golang

import (
	"context"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"cloud.google.com/go/storage"
)

func main(){
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	r, err := gcsext.ReadAllByPrefix(ctx, client.Bucket(baseBucket), "/some/gcs/path/in/the/bucket")
	if err != nil {
		panic(err)
	}
	// Do something with r which is a continous byte-stream of all objects in the bucket that
	// starts with the prefix provided. virtual folders ("placehodlers") are ignored.
}
```
