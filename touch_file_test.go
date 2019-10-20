//+integration
package gcsext_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
)

func TestTouch(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket := client.Bucket(baseBucket)

	path := fmt.Sprintf("artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_touch/%d", time.Now().Unix())

	r, err := gcsext.TouchFile(ctx, bucket, path)
	assert.NoError(err, "Expected to be able to touch a file when it doesn't exists")
	assert.NotNil(r, "Expected to get at valid object handle back")

	r, err = gcsext.TouchFile(ctx, bucket, path)
	assert.NoError(err, "Expected to be able to touch a file when it exists")
	assert.NotNil(r, "Expected to get at valid object handle back")
}
