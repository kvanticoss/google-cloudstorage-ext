package gcsext

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"
)

// StreamRecords reads records from a recordIterator and writes them to the WriterFactory provided writers
// Any record that provides GetPartions() method will have their partions expanded to the path.
func StreamRecords(ctx context.Context, WriterFactory func(path string) (WriteCloser, error), ri RecordIterator) (err error) {
	MultiFileStream := NewMultiWriterCache(WriterFactory, 15*time.Minute)
	defer MultiFileStream.Close()

	host, err := os.Hostname()
	if err != nil {
		return nil
	}

	recordDelimiter := []byte("\n")
	var record interface{}
	for record, err = ri(); err == nil; record, err = ri() {
		log.Printf("got Records %v", record)
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

		w, err := MultiFileStream.GetWriter(maybePartitions+"data_"+host+"_{suffix}.json.gz", true)
		if err != nil {
			return err
		}

		_, err = w.Write(append(d, recordDelimiter...))
		if err != nil {
			return err
		}
	}
	return err
}
