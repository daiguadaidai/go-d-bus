package mysqlapplybinlog

import (
	"github.com/siddontang/go-mysql/replication"
	"time"
	"fmt"
)

type BinlogEventPos struct {
	BinlogEvent *replication.BinlogEvent
	LogFile string
	LogPos int
	GenerateTimestamp int64
}

/* 获取位点和时间戳字符串
Return:
    纳秒:binlog文件:binlog位点(一共15位未满以0填充)
	1111111111111111111:mysql-bin.000000001:000001111111111
 */
func (this *BinlogEventPos) GetLogFilePosTimeStamp() string {
	return fmt.Sprintf("%v:%v:%015v",
		this.GenerateTimestamp, this.LogFile, this.LogPos)
}

/* 新将一个binlog事件和位点信息
Params:
	_binlogEvent: 解析的binlog 事件
	_losFile: 解析到的binlog位点文件
	_logPos: 解析到的binlog 位点
	_generateTimestamp: 生成的该实例的时间纳秒
 */
func NewBinlogEventPos(
	_binlogEvent *replication.BinlogEvent,
	_logFile string,
	_logPos int,
	_generateTimestamp int64,
) *BinlogEventPos {

	if _generateTimestamp < 0 {
		_generateTimestamp = time.Now().UnixNano()
	}

	return &BinlogEventPos{
		BinlogEvent: _binlogEvent,
		LogFile: _logFile,
		LogPos: _logPos,
		GenerateTimestamp: _generateTimestamp,
	}

}
