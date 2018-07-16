package common

import (
	"fmt"
	"path"
	"runtime"
)

func CurrLine() string {
	_, filePath, line, ok := runtime.Caller(1)

	if !ok {
		return fmt.Sprintf("无法获取行号")
	}

	fileName := path.Base(filePath)
	return fmt.Sprintf("%v:%v", fileName, line)
}
