package gcsext

// Lesser is implemented by type which can be compared to each other.
type Lesser interface {
	Less(other interface{}) bool
}
