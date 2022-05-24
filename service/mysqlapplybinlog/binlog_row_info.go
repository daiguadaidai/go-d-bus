package mysqlapplybinlog

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogRowInfo struct {
	Schema      string
	Table       string
	Before      []interface{}
	After       []interface{}
	EventType   replication.EventType
	ApplyRowKey string
}

/* 新建一个event中的每一行数据
Params:
	_schema: 该行的数据库
	_table: 该行的表
    _before: 旧值
	_after: 新值
	_eventType: 事件类型
	_applyRowKey: 应用的binlog标记
*/
func NewBinlogRowInfo(
	_schema string,
	_table string,
	_before []interface{},
	_after []interface{},
	_eventType replication.EventType,
	_applyRowKey string,
) *BinlogRowInfo {

	return &BinlogRowInfo{
		Schema:      _schema,
		Table:       _table,
		Before:      _before,
		After:       _after,
		EventType:   _eventType,
		ApplyRowKey: _applyRowKey,
	}
}

/* 通过给定的字段位子, 在 Before 值中计算出需要的并发槽是哪个(一般是主键)
Params:
	_columnIndexes: 需要获取的字段下角标
	_paraller: 应用binlog的并发数
*/
func (this *BinlogRowInfo) GetChanSlotByBefore(_columnIndexes []int, _paraller int) int {
	// 需要进行hash的字段值
	needHashValue := ""

	for _, columnIndex := range _columnIndexes {
		needHashValue += fmt.Sprintf("%v", this.After[columnIndex])
	}

	hashValue := common.GenerateHashByString(needHashValue)

	return hashValue % _paraller
}

/* 通过给定的字段位子, 在 After值中计算出需要的并发槽是哪个(一般是主键)
Params:
	_columnIndexes: 需要获取的字段下角标
	_paraller: 应用binlog的并发数
*/
func (this *BinlogRowInfo) GetChanSlotByAfter(_columnIndexes []int, _paraller int) int {
	// 需要进行hash的字段值
	needHashValue := ""

	for _, columnIndex := range _columnIndexes {
		needHashValue += fmt.Sprintf("%v", this.After[columnIndex])
	}

	hashValue := common.GenerateHashByString(needHashValue)

	return hashValue % _paraller
}

/* 获取 前镜像
Params:
	_columnIndexies: 相关索引信息
*/
func (this *BinlogRowInfo) GetBeforeRow(_columnIndeies []int) []interface{} {
	row := make([]interface{}, len(_columnIndeies))

	for i, valueIndex := range _columnIndeies {
		row[i] = this.Before[valueIndex]
	}

	return row
}

/* 获取 后镜像
Params:
	_columnIndies: 相关索引信息
*/
func (this *BinlogRowInfo) GetAfterRow(_columnIndies []int) []interface{} {
	row := make([]interface{}, len(_columnIndies))

	for i, valueIndex := range _columnIndies {
		row[i] = this.After[valueIndex]
	}

	return row
}

/* 获取前后镜像
Params:
	_columnIndies: 相关索引信息
*/
func (this *BinlogRowInfo) GetBeforeAndAfterRow(_columnIndies []int) ([]interface{}, []interface{}) {
	beforeRow := make([]interface{}, len(_columnIndies))
	afterRow := make([]interface{}, len(_columnIndies))

	for i, valueIndex := range _columnIndies {
		beforeRow[i] = this.Before[valueIndex]
		afterRow[i] = this.After[valueIndex]
	}

	return beforeRow, afterRow
}

/* 比较前后值是否不同
Params:
	_columnIndies: 相关列的下角表
*/
func (this *BinlogRowInfo) IsDiffBeforeAndAfter(_columnIndies []int) bool {
	isDiff := true

	for _, valueIndex := range _columnIndies {
		if this.Before[valueIndex] != this.After[valueIndex] { // 比较是否
			isDiff = false
			break
		}
	}

	return isDiff
}
