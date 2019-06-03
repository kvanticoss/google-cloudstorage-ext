//+integration
package gcsext_test

import (
	"context"
	"io/ioutil"
	"log"
	"testing"

	gcsext "github.com/kvanticoss/google-cloudstorage-ext"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
)

const (
	baseBucket = "kvantic-public"
)

func TestSimpleRead(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket := client.Bucket(baseBucket)

	tests := []struct {
		path, expected string
	}{
		{
			path:     "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_readplaintext/",
			expected: "A\nB\nC\n",
		}, {
			path:     "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_readgzip/",
			expected: "A\nB\nC\n",
		}, {
			path:     "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_mixed/",
			expected: "A\nA\nB\nC\n",
		}, {
			path:     "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_mixedempty/",
			expected: "A\nB\nC\nA\nB\nC\n",
		},
	}

	for index, t := range tests {
		r, err := gcsext.ReadAllByPrefix(ctx, bucket, t.path)
		b, err := ioutil.ReadAll(r)
		if err != nil {
			log.Fatal(err)
			continue
		}

		assert.Equal(t.expected, string(b), "Test number %d - %s", index, t.path)
	}
}
