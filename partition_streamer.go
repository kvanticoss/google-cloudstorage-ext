package gcsext

import (
	"context"
	"os"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/kvanticoss/google-cloudstorage-ext/gzip"
)

// StreamJSONRecords reads records from a recordIterator and writes them to the WriterFactory provided writers
// Any record that provides GetPartions() method will have their partions expanded to the path.
//
// NOTE: The RecordIterator MUST NOT re-use the same data underlying datastructure IF the record implements the Lesser interface
// since implementing the Lesser interface implies that records should be saved in sorted order. To achieve this effect
// a copy of the last record (per partition) must be keept to compare with. If the record-reference is volatile between
// invocations of the RecordIterator any comparisson with be of limited value.
//
// e.g The following iterator will NOT work
// s := struct{i int}{}
// func() interface{} {
//   s.i++
//   return s // returns the same reference in each invokation; As interfaces only hold points it will be cast to a pointer and not a call by value
// }
//
// while this WILL WORK
// s := struct{i int}{}
// func() interface{} {
// 	 sCopy := s
//   sCopy.i++
//   return sCopy // returns a reference to a new instance of the struct
// }
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

// JSONRecordStreamer provides partitioned writing of records to a store
type JSONRecordStreamer struct {
	mwc               *MultiWriterCache
	clusterLastRecord map[string][]Lesser
	hostName          string
}

// NewRecordsStreamer creates an Json Record writer. It will write each record to a hadoop partitioned path using the
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
		mwc:               NewMultiWriterCache(ctx, gzipWriterFactory, ttl),
		clusterLastRecord: map[string][]Lesser{},
		hostName:          host,
	}, nil
}

// Close will flush any records and close all the underlying writers
func (rs *JSONRecordStreamer) Close() error {
	return rs.mwc.Close()
}

// WriteRecord will extract any partitions (assumes record implementes PartitionGetter) and write the record
// as a new-line-delimited-JSON byte-stream.
func (rs *JSONRecordStreamer) WriteRecord(record interface{}) error {
	recordDelimiter := []byte("\n")

	maybePartitions := ""
	if recordPartitioner, ok := record.(PartitionGetter); ok {
		maybeParts, err := recordPartitioner.GetPartions()
		if err != nil {
			return err
		}
		maybePartitions = maybeParts.ToPartitionKey() + "/"
	}

	// within each cluster, all records must come in sorted order; which cluster should this record be appended to?
	cluster := rs.findOptimalCluster(maybePartitions, record)

	d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
	if err != nil {
		return err
	}
	_, err = rs.mwc.Write(maybePartitions+"data_"+rs.hostName+"_"+cluster+"_{suffix}.json", append(d, recordDelimiter...))
	return err
}

func (rs *JSONRecordStreamer) findOptimalCluster(maybePartitions string, record interface{}) string {
	cluster := "0"
	if record, ok := record.(Lesser); ok {
		bestCluster := -1

		// See which is the first cluster this record can be appended to (and still be in sorted order)
		for index, lastRecordInCluster := range rs.clusterLastRecord[maybePartitions] {
			if lastRecordInCluster.Less(record) {
				bestCluster = index
				rs.clusterLastRecord[maybePartitions][index] = record
			}
		}
		// Should this record be sorted before all existing clusters?
		if bestCluster == -1 {
			bestCluster = len(rs.clusterLastRecord[maybePartitions])
			rs.clusterLastRecord[maybePartitions] = append(rs.clusterLastRecord[maybePartitions], record)
		}
		cluster = strconv.Itoa(bestCluster)
	}
	return cluster
}
