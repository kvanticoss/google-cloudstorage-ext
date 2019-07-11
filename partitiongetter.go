package gcsext

// PartitionGetter is implemented by records that know how they want to be saved / clustered together.
type PartitionGetter interface {
	GetPartions() (KeyValues, error)
}