package gcsext

import (
	"context"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/kvanticoss/goutils/backoff"
	"github.com/kvanticoss/goutils/gzip"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/recordbuffer"
	"github.com/kvanticoss/goutils/recordwriter"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

// SortGCSFolders sorts all files picked up by the prefix + predicate and saves them into sorted NewLineJson under the filename given by
// destination prefix. If the destination prefix contains a .gz suffix the contents will be gzipped new line JSON
//
// @ctx - context
// @bucket - *storage.BucketHandle to operate on
// @prefix - The prefix under which to start searching for folders.
// @newer - A factory for creating a new instances of records to unmarshal into; For sorting to take place the record must implment github.com/kvanticoss/goutils/iterator.Lesser
// @srcPredicate - an optional predicate to identify ONLY the files to be merged.
// @destinationPrefix - The destination file-name for merged files. The final name will be folder/destinationPrefix
// @cacheFactory - The bytesBuffer to use for soring.
// @bo - A backoff time; can be left null for default of 5 re-attempts with at least 15 sleep intervals
// @removeDuplicates - Should duplicated records be removed.
// @removeSrcOnSuccess - Should we remove the original files after compacting them. Will reuse srcPredicate for file removals
func SortGCSFolders(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	newer func() iterator.Lesser,
	srcPredicate func(*storage.ObjectAttrs) bool,
	destinationPrefix string,
	cacheFactory recordbuffer.ReadWriteResetterFactory,
	bo *backoff.RandExpBackoff,
	removeDuplicates bool,
	removeSrcOnSuccess bool,
) error {
	// Create a sorted iterator from all files in a GCS folder (open all files,
	// read as sorted as possible by only iterating 1 record at a time from each reader)
	// Put into a limited memory cache (deduplication)
	// Write into sorted cache-files (either local FS or RAM)
	// Read from sorted cache files and write to dst

	newerAsIf := func() interface{} {
		return newer()
	}

	if bo == nil {
		bo = bo.WithMaxAttempts(5).WithMinBackoff(time.Second * 15).WithScale(5) // Add some sane default for our backoff timer
	}

	// Log all files we have procerssed so we know what we later can delete
	shouldDeleteOnSuccess := []*storage.ObjectAttrs{}
	srcPredicateWithLog := func(obj *storage.ObjectAttrs) bool {
		res := srcPredicate(obj)
		if res {
			shouldDeleteOnSuccess = append(shouldDeleteOnSuccess, obj)
		}
		return res
	}
	canDeletePredicate := func(obj *storage.ObjectAttrs) bool {
		for _, o := range shouldDeleteOnSuccess {
			if obj.Name == o.Name {
				return true
			}
		}
		return false
	}

	return IterateJSONRecordsByFoldersSortedCB(ctx, bucket, prefix, newerAsIf, srcPredicateWithLog,
		func(folder string, it func() (interface{}, error)) error {
			// Setup a reocrd buffer; using the provided cacheFactory for partitions
			count := 0
			cacheFactoryWithCounter := func() recordbuffer.ReadWriteResetter {
				count++
				return cacheFactory()
			}
			byteBuffer := recordbuffer.NewSortedRecordBuffers(cacheFactoryWithCounter, newer)
			byteBuffer.LoadFromRecordIterator(it)
			if count == 0 {
				return nil
			}

			// Get sorted records back.
			lessIt, err := byteBuffer.GetSortedIterator()
			if err != nil {
				return errors.Wrap(err, "failed to get sorted iterator")
			}

			// Get a reader and writer to our compaction file (yes GCS allows to read and write to the same file concurrently)
			dstPath := path.Join(folder, destinationPrefix)
			existingReader, gcsWriter, err := getFixedGenerationReadWriters(ctx, bucket, dstPath)
			if err != nil {
				return err
			}
			alreadySortedItems := iterator.JSONRecordIterator(newerAsIf, existingReader).ToLesserIterator()

			// Combine bufferd and already sorted records
			sIt, err := iterator.SortedLesserIterators(append([]iterator.LesserIterator{lessIt}, alreadySortedItems))
			if err != nil {
				return errors.Wrap(err, "failed to get sorted iterator from RecordBuffer")
			}
			rIt := sIt.ToRecordIterator()

			// Maybe deduplicate
			if removeDuplicates {
				rIt = iterator.DeduplicateRecordIterators(rIt)
			}

			// Write it all as NL JSON
			if err = recordwriter.NewLineJSON(rIt, gcsWriter); err != nil && err != iterator.ErrIteratorStop {
				return errors.Wrap(err, "failed to write JSON records to gcsWriter")
			}

			// Here we can get precondition errors; errors we can retry on
			if err = gcsWriter.Close(); err != nil {
				bo, boErr := bo.SleepAndIncr()
				if boErr != nil {
					return errors.Wrap(err, "Failed even after 5 attempts; aborting")
				}

				gerr, ok := err.(*googleapi.Error)
				if ok && (gerr.Code == http.StatusPreconditionFailed ||
					gerr.Code == http.StatusTooManyRequests) {

					return SortGCSFolders(ctx, bucket, folder, newer, // Note; this time we start in the folder we failed to compact, not the root prefix
						srcPredicate, destinationPrefix, cacheFactory, bo,
						removeDuplicates, removeSrcOnSuccess,
					)
				}
				return err // Unknown error; return it
			}

			if removeSrcOnSuccess {
				return RemoveFolder(ctx, bucket, folder, CombineFilters(
					canDeletePredicate, // Only remove the orignal files intended for compaction
					func(obj *storage.ObjectAttrs) bool { // but also make sure we don't remove the resulting file
						return obj.Name != dstPath
					},
				))
				shouldDeleteOnSuccess = []*storage.ObjectAttrs{} // Clear the log of files we can delete (since we just deleted them)
			}

			return nil
		},
	)
}

func getFixedGenerationReadWriters(
	ctx context.Context,
	bucket *storage.BucketHandle,
	dstPath string,
) (io.ReadCloser, io.WriteCloser, error) {
	// Ensure it exists first so we get a generation id
	dstHandle, err := TouchFile(ctx, bucket, dstPath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to touch gcs destination file")
	}
	attr, err := dstHandle.Attrs(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get gcs attribues for dst file")
	}

	// Createa a reader at said generation; possibly gzip unpack it
	var existingReader io.ReadCloser
	existingReader, err = dstHandle.Generation(attr.Generation).NewReader(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open reader to existing sorted file")
	}
	if strings.HasSuffix(dstPath, ".gz") {
		existingReader, err = gzip.NewReader(existingReader)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to open gzip reader to existing sorted file even though the file has .gz suffix")
		}
	}

	// Createa a writer but ensure we get 429 errors if the file has changed from the current generation.
	var gcsWriter io.WriteCloser
	gcsWriter = dstHandle.If(storage.Conditions{GenerationMatch: attr.Generation}).NewWriter(ctx)
	if strings.HasSuffix(dstHandle.ObjectName(), ".gz") {
		gcsWriter = gzip.NewWriter(gcsWriter)
	}

	return existingReader, gcsWriter, nil
}
