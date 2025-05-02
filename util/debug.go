package util

import (
	"runtime"
)

func CurrentStack() string {
	buf := make([]byte, 1<<16) // 64KB buffer
	stackSize := runtime.Stack(buf, true)
	return string(buf[:stackSize])
}
