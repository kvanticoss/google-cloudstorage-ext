package gcsext

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"
)

// StreamJSONRecords reads records from a recordIterator and writes them to the WriterFactory provided writers
// Any record that provides GetPartions() method will have their partions expanded to the path.
func StreamJSONRecords(ctx context.Context, WriterFactory func(path string) (WriteCloser, error), ri RecordIterator) (err error) {
	rs, err := NewRecordsStreamer(ctx, WriterFactory)
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

type JSONRecordStreamer struct {
	mwc      *MultiWriterCache
	hostName string
}

// NewRecordsStreamer creates an Json Record writer. It will write each record to a hadoop partitioned path using the
func NewRecordsStreamer(ctx context.Context, WriterFactory func(path string) (WriteCloser, error)) (rs *JSONRecordStreamer, err error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &JSONRecordStreamer{
		mwc:      NewMultiWriterCache(ctx, WriterFactory, 15*time.Minute),
		hostName: host,
	}, nil
}

func (rs *JSONRecordStreamer) Close() {
	rs.mwc.Close()
}

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

	d, err := json.Marshal(record)
	if err != nil {
		return err
	}
	nowStr := strconv.Itoa(int(time.Now().Unix()))
	w, err := rs.mwc.GetWriter(maybePartitions+"data_"+rs.hostName+"_"+nowStr+"_{suffix}.json.gz", true)
	if err != nil {
		return err
	}

	_, err = w.Write(append(d, recordDelimiter...))
	if err != nil {
		return err
	}

	return nil
}
