package gcsext

import (
	"net/url"
	"strings"
)

// KeyValue is a string tuple used to represent an order list of KeyValue pairs (unlike a map which is un-ordered)
type KeyValue struct {
	Key, Value string
}

// KeyValues is an ordered list of KeyValue elements.
type KeyValues []KeyValue

// ToPartitionKey formats KeyValues as key1=va1/key2=val2/.... as is common in hadoop file storage.
// keys and values are query escaped
func (keyvals KeyValues) ToPartitionKey() string {
	partitions := []string{}
	for _, kv := range keyvals {
		partitions = append(partitions, url.QueryEscape(kv.Key)+"="+url.QueryEscape(kv.Value))
	}
	return strings.Join(partitions, "/")
}
