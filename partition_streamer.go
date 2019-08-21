package gcsext

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	jsoniter "github.com/json-iterator/go"

	"github.com/kvanticoss/google-cloudstorage-ext/gzip"
)

const maxTreeSize = 500
const maxOpenPartitions = 150

// StreamJSONRecords reads records from a recordIterator and writes them to the WriterFactory provided writers
// Any record that provides GetPartions() method will have their partions expanded to the path.
//
// NOTE: The RecordIterator MUST NOT re-use the same data underlying datastructure IF the record implements the Lesser interface
// since implementing the Lesser interface implies that records should be saved in sorted order. To achieve this effect
// a copy of the last record (per partition) must be keept to compare with. If the record-reference is volatile between
// invocations of the RecordIterator any comparisson with be of limited value.
//
// e.g The following iterator will NOT work
//  s := struct{i int}{}
//  func() interface{} {
//    s.i++
//    return s // returns the same reference in each invokation; As interfaces only hold points it will be cast to a pointer and not a call by value
//  }
//
// while this WILL WORK
//  s := struct{i int}{}
//  func() interface{} {
//    sCopy := s
//    sCopy.i++
//    return sCopy // returns a reference to a new instance of the struct
//  }
//
func StreamJSONRecords(
	ctx context.Context,
	WriterFactory func(path string) (WriteCloser, error),
	ri RecordIterator,
	bucketTTL time.Duration,
) (err error) {
	rs, err := NewRecordsStreamer(ctx, WriterFactory, bucketTTL)
	if err != nil {
		return err
	}
	defer rs.Close()
	var record interface{}
	for record, err = ri(); err == nil; record, err = ri() {
		if err := rs.WriteRecord(record); err != nil {
			return err
		}
	}
	return err
}

type btreesWithTs struct {
	*btree.BTree
	lastTs time.Time
}

// JSONRecordStreamer provides partitioned writing of records to a store
type JSONRecordStreamer struct {
	mwc *MultiWriterCache

	clusters     map[string][]*btreesWithTs
	clusterMutex sync.Mutex

	hostName string
}

// NewRecordsStreamer creates an Json Record writer. It will write each record to a hadoop partitioned path (if the record implements GetPartitions()).
// All data will be gzip:ed before written to the WriterFactory-provided Writer.
func NewRecordsStreamer(ctx context.Context, WriterFactory func(path string) (WriteCloser, error), ttl time.Duration) (rs *JSONRecordStreamer, err error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	gzipWriterFactory := func(path string) (WriteCloser, error) {
		w, err := WriterFactory(path + ".gz")
		if err != nil {
			return nil, err
		}
		return gzip.NewWriter(w), err
	}

	return &JSONRecordStreamer{
		mwc: NewMultiWriterCache(ctx, gzipWriterFactory, ttl),

		clusters:     make(map[string][]*btreesWithTs),
		clusterMutex: sync.Mutex{},

		hostName: host,
	}, nil
}

// Close will flush any records and close all the underlying writers
func (rs *JSONRecordStreamer) Close() error {
	rs.clusterMutex.Lock()
	defer rs.clusterMutex.Unlock()

	var allErrors MultiError
	for path, trees := range rs.clusters {
		for index, tree := range trees {
			log.Printf("Closing tree with %d items @ %s", tree.Len(), path)
			for tree.Len() > 0 {
				path2 := strings.ReplaceAll(path, "{clusterId}", strconv.Itoa(index))
				if err := rs.writeRecord(path2, tree.DeleteMin()); err != nil {
					allErrors = append(allErrors, err)
				}
			}
		}
	}
	if err := rs.mwc.Close(); err != nil {
		allErrors = append(allErrors, err)
	}
	return allErrors.MaybeError()
}

// WriteRecord will extract any partitions (assumes record implementes PartitionGetter) and write the record
// as a new-line-delimited-JSON (gzip:ed) byte-stream. It is important that any record which implements the Lesser-interface
// have a distinct sort-order. If two records A & B fullfills A.Less(B) == B.Less(A) it is interpreted as they being the same
// record. At such a time it is undefined if A OR B OR both are written (timing dependent).
//
// Writing a nil-record will return a nil-error but have no effect
func (rs *JSONRecordStreamer) WriteRecord(record interface{}) error {

	if record == nil {
		return nil
	}

	maybePartitions := ""
	if recordPartitioner, ok := record.(PartitionGetter); ok {
		maybeParts, err := recordPartitioner.GetPartions()
		if err != nil {
			return err
		}
		maybePartitions = maybeParts.ToPartitionKey() + "/"
	}

	// within each cluster, all records must come in sorted order; which cluster should this record be appended to?
	path := maybePartitions + "data_" + rs.hostName + "_" + "{clusterId}_{suffix}.json"
	isSortable, asLesser, clusterId, tree := rs.findOptimalCluster(path, record)

	if !isSortable {
		return rs.writeRecord(path, record)
	}

	tree.ReplaceOrInsert(btreeLesser{asLesser})
	if tree.Len() > maxTreeSize {
		return rs.writeRecord(strings.ReplaceAll(path, "{clusterId}", clusterId), tree.DeleteMin())
	}

	return nil
}

func (rs *JSONRecordStreamer) writeRecord(path string, record interface{}) error {
	recordDelimiter := []byte("\n")

	// Expected case
	if record == nil {
		return nil
	}

	if btl, ok := record.(btreeLesser); ok {
		record = btl.Lesser
	}

	d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
	if err != nil {
		return err
	}
	_, err = rs.mwc.Write(path, append(d, recordDelimiter...))
	return err
}

func (rs *JSONRecordStreamer) closeLRU() {
	rs.clusterMutex.Lock()
	defer rs.clusterMutex.Unlock()

	now := time.Now()
	oldestTs := now
	oldestPath := ""
	oldestIndex := 0

	for path, trees := range rs.clusters {
		for index, tree := range trees {
			if tree.lastTs.Before(oldestTs) {
				oldestPath = path
				oldestTs = tree.lastTs
				oldestIndex = index
			}
		}
	}

	if oldestTs != now {
		log.Printf("Closing old/LRU partitions @ %s:%d", oldestPath, oldestIndex)
		tree := rs.clusters[oldestPath][oldestIndex]
		for tree.Len() > 0 {
			path2 := strings.ReplaceAll(oldestPath, "{clusterId}", strconv.Itoa(oldestIndex))
			_ = rs.writeRecord(path2, tree.DeleteMin())
		}
		if oldestIndex > 0 {
			rs.clusters[oldestPath] = rs.clusters[oldestPath][oldestIndex:]
		} else {
			delete(rs.clusters, oldestPath)
		}
	}

}

func (rs *JSONRecordStreamer) findOptimalCluster(
	path string,
	record interface{},
) (
	isSortable bool,
	recAsLesser Lesser,
	clusterID string,
	clusterTree *btreesWithTs,
) {
	rs.clusterMutex.Lock()
	defer func() {
		// TODO: Better pruning method.
		if len(rs.clusters) > maxOpenPartitions {
			go rs.closeLRU()
		}
	}()
	defer rs.clusterMutex.Unlock()

	clusterID = "0"
	recAsLesser, isSortable = record.(Lesser)
	if !isSortable {
		return isSortable, recAsLesser, clusterID, nil
	}

	_, exists := rs.clusters[path]
	if !exists {
		log.Printf("Creating INITIAL sortTree for path %s", path)
		rs.clusters[path] = append(rs.clusters[path], &btreesWithTs{btree.New(16), time.Now()})
	}

	// See which is the first cluster this record can be appended to (and still be in sorted order)
	for index, tree := range rs.clusters[path] {
		leastVal := tree.Min()
		if leastVal == nil || tree.Len() < (maxTreeSize-2) || !leastVal.Less(btreeLesser{recAsLesser}) { // If it is not less than the smallest value in the tree, or it is empty => we're good
			if leastVal != nil {
				tree.lastTs = time.Now()
			}
			return isSortable, recAsLesser, strconv.Itoa(index), tree
		}
	}

	// Otherwise we need a new tree
	log.Printf("Creating Additional sortTree for path %s (index: %d)", path, len(rs.clusters[path]))

	rs.clusters[path] = append(rs.clusters[path], &btreesWithTs{btree.New(16), time.Now()})

	return isSortable, recAsLesser, strconv.Itoa(len(rs.clusters[path])), rs.clusters[path][len(rs.clusters[path])-1]
}
