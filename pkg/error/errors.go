package storeError

import "fmt"

var (
	InvalidParameters = fmt.Errorf("invalid parameters")
	IntervalError     = fmt.Errorf("interval error")
)
