package recovery

import (
	"fmt"
)

// Do is a function wrapper that will recover from a panic and return the error.
func Do(f func() error) func() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				switch e := r.(type) {
				case error:
					err = e
				case string:
					err = fmt.Errorf("%v", e)
				}
			}
		}()
		return f()
	}
}
