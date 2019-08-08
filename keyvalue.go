package gcsext

import (
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
)

// KeyValue is a string tuple used to represent an order list of KeyValue pairs (unlike a map which is un-ordered)
type KeyValue struct {
	Key, Value string
}

func (kv KeyValue) ToHadoopPartition() string {
	return url.QueryEscape(kv.Key) + "=" + url.QueryEscape(kv.Value)
}

// KeyValues is an ordered list of KeyValue elements.
type KeyValues []KeyValue

// ToPartitionKey formats KeyValues as key1=va1/key2=val2/.... as is common in hadoop file storage.
// keys and values are query escaped
func (keyvals KeyValues) ToPartitionKey() string {
	partitions := []string{}
	for _, kv := range keyvals {
		partitions = append(partitions, kv.ToHadoopPartition())
	}
	return strings.Join(partitions, "/")
}

// AsMap returns the orders KeyValue list as an unorderd map
func (keyvals KeyValues) AsMap() map[string]string {
	res := map[string]string{}
	for _, kv := range keyvals {
		res[kv.Key] = kv.Value
	}
	return res
}

// ToPrefixReadFilter turns the key values into AND:ed filename-predicates during ReadFilteredByPrefix() scans
// It will return a (filter) function(string) bool which will answers the question "Does all the key value pairs exist as
// hadoop encoded partition keys in the provided string". Only supports exact matches.
func (keyvals KeyValues) ToPrefixReadFilter() func(*storage.ObjectAttrs) bool {
	return func(attr *storage.ObjectAttrs) bool {
		for _, kv := range keyvals {
			if !strings.Contains(attr.Name, kv.ToHadoopPartition()+"/") {
				return false
			}
		}
		return true
	}
}

func GetKeyValuesFromString(s string) KeyValues {
	res := KeyValues{}
	for _, part := range strings.Split(s, "/") {
		maybeKeyValue := strings.Split(part, "=")
		if len(maybeKeyValue) == 2 {
			maybeKey, err1 := url.QueryUnescape(maybeKeyValue[0])
			maybeValue, err2 := url.QueryUnescape(maybeKeyValue[1])
			if err1 == nil && err2 == nil {
				res = append(res, KeyValue{Key: maybeKey, Value: maybeValue})
			}
		}
	}
	return res
}
