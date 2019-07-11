package gcsext

// MultiError is an error that consists of a list of errors
type MultiError []error

// MaybeError returns nil if the list of errors is empty; otherwise itself
func (me MultiError) MaybeError() error {
	if len(me) == 0 {
		return nil
	}
	return me
}

func (me MultiError) Error() string {
	res := ""
	for _, err := range me {
		res = res + err.Error() + ";"
	}
	return res
}
