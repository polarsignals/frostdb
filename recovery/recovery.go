package recovery

import (
	"fmt"
	"runtime/debug"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// Do is a function wrapper that will recover from a panic and return the error.
// Optionally it takes a logger to log the stack trace. Note that it only logs to a single logger.
func Do(f func() error, logger ...log.Logger) func() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				switch e := r.(type) {
				case error:
					err = e
				case string:
					err = fmt.Errorf("%v", e)
				}
				if len(logger) > 0 {
					level.Error(logger[0]).Log("msg", "recovered from panic", "err", err, "stacktrace", string(debug.Stack()))
				}
			}
		}()
		return f()
	}
}
