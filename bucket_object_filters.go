package gcsext

import (
	"bytes"
	"strings"

	"cloud.google.com/go/storage"
)

// FilterOutVirtualGcsFolders is a predicate function which removes the GCS virtual folders by requiring the name to end with "/" and the hash to match "placeholder" content
func FilterOutVirtualGcsFolders(objAttr *storage.ObjectAttrs) bool {
	return !(strings.HasSuffix(objAttr.Name, "/") && bytes.Equal(objAttr.MD5, placeholderMD5))
}

// CombineFilters creates an iterator-filter function by "AND"-ing all filters.
func CombineFilters(filters ...func(*storage.ObjectAttrs) bool) func(*storage.ObjectAttrs) bool {
	return func(o *storage.ObjectAttrs) bool {
		for _, f := range filters {
			if !f(o) {
				return false
			}
		}
		return true
	}
}
