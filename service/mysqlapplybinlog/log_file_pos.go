package mysqlapplybinlog

import (
	"strings"
	"strconv"
)

type LogFilePos struct {
	LogFile string
	LogPos int
}

/* 新建一个信息实例
Params:
	_logFile: binlog文件
	_logPos: binlog pos
 */
func NewLogFilePos(_logFile string, _logPos int) *LogFilePos {
	return &LogFilePos{
		LogFile: _logFile,
		LogPos: _logPos,
	}
}

/* 通过一个timestamp, logfile, logpos, 组合的key生成 LogFilePos
Params:
	_key: 1111111111111111111:mysql-bin.000000001:000001111111111
 */
func NewLogFilePosByKey(_key string) *LogFilePos {
	items := strings.Split(_key, ":")
	logFile := items[1]
	logPos, _ := strconv.Atoi(items[2])


	return &LogFilePos{
		LogFile: logFile,
		LogPos: logPos,
	}
}

/* 判断本 位点是否 >= 其他位点
Params:
	_other: 其他位点
 */
func (this *LogFilePos) IsRatherThan(_other *LogFilePos) bool {
	isRatherThan := false

	if this.LogFile > _other.LogFile {
		isRatherThan = true
	} else if(this.LogFile == _other.LogFile && this.LogPos > _other.LogPos) {
		isRatherThan = true
	}

	return isRatherThan
}
