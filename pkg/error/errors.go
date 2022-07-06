package storeError

import "fmt"

var (
	InvalidParameters = fmt.Errorf("invalid parameters")
	IntervalError     = fmt.Errorf("interval error")
)

var KeyHasExisted = fmt.Errorf("key has existed")
var StoreClosed = fmt.Errorf("PandoStore has been closed")
