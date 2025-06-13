package errors

import (
	stdErrors "errors"
)

var ErrInvalidCommandArgs = stdErrors.New("invalid command args")
