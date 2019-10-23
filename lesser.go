package gcsext

// Lesser is implemented by type which can be compared to each other and should answer
// i'm I/this/self less than the other record (argument 1)
type Lesser interface {
	Less(other interface{}) bool
}
