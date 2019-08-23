package gcsext

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kvanticoss/google-cloudstorage-ext/timeout"

	"github.com/google/btree"
)

// timeoutTree is a RedBlack tree with idleTimeout notifications which can be used for self destructs
type timeoutTree struct {
	*btree.BTree
	*timeout.Timeout
}

func (tt *timeoutTree) ReplaceOrInsert(item btree.Item) btree.Item {
	tt.Ping()
	return tt.BTree.ReplaceOrInsert(item)
}

// SortedRecordWriter provides sorted writes. Writes that are out of order will be
// written to new files so that order within each file is guarranteed
type SortedRecordWriter struct {
	// Settings
	writeCallback       func(string, Lesser)
	cacheSizePerCluster int
	cacheMaxIdleItme    time.Duration

	// State
	ctx            context.Context
	cancel         func()
	mutex          sync.Mutex
	bucketsCreated int
	buckets        map[string]timeoutTree
}

// SortedRecordWriterOption represents an option applicable to SortedRecordWriter
type SortedRecordWriterOption func(*SortedRecordWriter) *SortedRecordWriter

// NewSortedRecordWriter returns a new SortedRecordWriter
func NewSortedRecordWriter(
	ctx context.Context,
	writeCallback func(bucketId string, record Lesser),
	options ...SortedRecordWriterOption,
) *SortedRecordWriter {
	subCtx, cancel := context.WithCancel(ctx)
	r := &SortedRecordWriter{
		ctx:                 subCtx,
		cancel:              cancel,
		mutex:               sync.Mutex{},
		cacheSizePerCluster: 1000,
		cacheMaxIdleItme:    10 * time.Minute,
		writeCallback:       writeCallback,
		buckets:             map[string]timeoutTree{},
	}

	for _, o := range options {
		r = o(r)
	}

	return r
}

// WithMaxCacheSize updates the default (1000) item limit of the cache
func WithMaxCacheSize(size int) SortedRecordWriterOption {
	return func(sw *SortedRecordWriter) *SortedRecordWriter {
		sw.cacheSizePerCluster = size
		return sw
	}
}

// WithMaxCacheIdleTime updates the default (10 minute) idle timeout of buckets
func WithMaxCacheIdleTime(t time.Duration) SortedRecordWriterOption {
	return func(sw *SortedRecordWriter) *SortedRecordWriter {
		sw.cacheMaxIdleItme = t
		return sw
	}
}

// WriteRecord adds a record to a cache and potentially starts streaming
// records to the underlying writeCallback; Will return an error if written
// after ctx.Done() or Close()
func (sw *SortedRecordWriter) WriteRecord(record Lesser) error {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	if err := sw.ctx.Err(); err != nil {
		return err
	}

	if record == nil {
		return ErrInvalidRecord
	}

	bt := &btreeLesser{record}

	bucketID, tree := sw.insertIntoTree(bt)
	if tree.Len() > sw.cacheSizePerCluster {
		if item := tree.DeleteMin(); item != nil {
			sw.writeCallback(bucketID, item.(*btreeLesser).Lesser)
		}
	}
	return nil
}

// Close will stop SortedRecordWriter from accepting new writes; flush existing and return
func (sw *SortedRecordWriter) Close() {
	sw.cancel()
	sw.Flush() // Just to block untill all is flushed
}

// Flush will flush all existing buckets but gives no guarrantees that other buckets
// can not be created duing the flush. It is OK to continue writing after a flush.
func (sw *SortedRecordWriter) Flush() {
	// Make a copy of the keys so we avoid mutating a map we're iterating over
	keys := []string{}
	for key := range sw.buckets {
		keys = append(keys, key)
	}

	for _, key := range keys {
		sw.closeBucket(key)
	}
	log.Printf("Done with Flushing SRW")
}

func (sw *SortedRecordWriter) closeBucket(bucketID string) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	log.Printf("Closing sorter bucketID @ %s", bucketID)

	tree, ok := sw.buckets[bucketID]
	if !ok {
		return
	}

	for item := tree.DeleteMin(); item != nil; item = tree.DeleteMin() {
		sw.writeCallback(bucketID, item.(*btreeLesser).Lesser)
	}
	delete(sw.buckets, bucketID)
}

func (sw *SortedRecordWriter) getNewBucketID() string {
	sw.bucketsCreated++
	return fmt.Sprintf("i%06d_t%d", sw.bucketsCreated, time.Now().Unix())
}

func (sw *SortedRecordWriter) insertIntoTree(record *btreeLesser) (string, timeoutTree) {
	// See which is the first bucket this record can be appended to (and still be in sorted order)
	for bucketID, tree := range sw.buckets {
		leastVal := tree.Min()
		if leastVal == nil || //Nothin in the tree => OK
			leastVal.Less(record) || // The value we're insert is larger than what is the least value in the tree.
			tree.Len() < (sw.cacheSizePerCluster-2) { // Or we haven't started writing any records in this cluster.

			tree.ReplaceOrInsert(record)
			return bucketID, tree
		}
	}

	// Needs a new tree
	bucketID := sw.getNewBucketID()
	newTree := timeoutTree{
		BTree: btree.New(16),
		Timeout: timeout.NewTimeout(sw.ctx, sw.cacheMaxIdleItme, true, func() {
			sw.closeBucket(bucketID)
		}),
	}
	newTree.ReplaceOrInsert(record)

	// Save it for later.
	sw.buckets[bucketID] = newTree
	return bucketID, newTree
}

// btreeLesser is a supporting struct to translate Less(other interface{}) to Less(other github.com/google/btree.Item)
type btreeLesser struct {
	Lesser
}

func (l btreeLesser) Less(other btree.Item) bool {
	if other == nil {
		return false
	}
	if btl, ok := other.(btreeLesser); ok {
		return l.Lesser.Less(btl.Lesser)
	}
	if btl, ok := other.(*btreeLesser); ok && btl != nil {
		return l.Lesser.Less(btl.Lesser)
	}
	return l.Lesser.Less(other)
}
