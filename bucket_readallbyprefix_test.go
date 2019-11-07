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

func TestReadFoldersFilteredByPrefix(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket := client.Bucket(baseBucket)

	it, err := gcsext.ReadFoldersByPrefixWithFilter(ctx, bucket, "artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_", nil)
	assert.NoError(err, "couldn't get folderIterator from ReadFoldersByPrefixWithFilter")

	expections := map[string]string{
		"artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_readplaintext": "A\nB\nC\n",
		"artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_readgzip":      "A\nB\nC\n",
		"artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_mixed":         "A\nA\nB\nC\n",
		"artifacts/kvanticoss/github.com/google-cloudstorage-ext/test_mixedempty":    "A\nB\nC\nA\nB\nC\n",
	}

	for folder, reader, err := it(); err == nil; folder, reader, err = it() {
		//t.Log(folder)
		b, err := ioutil.ReadAll(reader)
		assert.NoError(err, "failed to read stuff")

		if expected, ok := expections[folder]; ok {
			assert.Equal(expected, string(b), "content doesn't match expectations")
			delete(expections, folder)
		}
	}
	assert.Empty(expections, "Expected to iterate through all test-cases")

}
