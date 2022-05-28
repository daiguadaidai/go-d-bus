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
func NewBinlogRowInfo(schema string, table string, before []interface{}, after []interface{}, eventType replication.EventType, applyRowKey string) *BinlogRowInfo {
	return &BinlogRowInfo{
		Schema:      schema,
		Table:       table,
		Before:      before,
		After:       after,
		EventType:   eventType,
		ApplyRowKey: applyRowKey,
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
func (this *BinlogRowInfo) GetBeforeRow(columnIndeies []int) []interface{} {
	cvtRow := common.ConverSQLType(this.Before)

	row := make([]interface{}, len(columnIndeies))
	for i, valueIndex := range columnIndeies {
		row[i] = cvtRow[valueIndex]
		row[i] = this.Before[valueIndex]
	}

	return row
}

/* 获取 后镜像
Params:
	_columnIndies: 相关索引信息
*/
func (this *BinlogRowInfo) GetAfterRow(columnIndies []int) []interface{} {
	// 将binlog转化为字符串或数字, 等相关类型
	cvtRow := common.ConverSQLType(this.After)

	row := make([]interface{}, len(columnIndies))
	for i, valueIndex := range columnIndies {
		row[i] = cvtRow[valueIndex]
	}

	return row
}

/* 获取前后镜像
Params:
	_columnIndies: 相关索引信息
*/
func (this *BinlogRowInfo) GetBeforeAndAfterRow(columnIndies []int) ([]interface{}, []interface{}) {
	beforeRow := this.GetBeforeRow(columnIndies)
	afterRow := this.GetAfterRow(columnIndies)

	return beforeRow, afterRow
}

/* 比较前后值是否不同
Params:
	_columnIndies: 相关列的下角表
*/
func (this *BinlogRowInfo) IsDiffBeforeAndAfter(columnIndies []int) bool {
	beforeRow := this.GetBeforeRow(columnIndies)
	afterRow := this.GetAfterRow(columnIndies)

	for i, beforeFieldValue := range beforeRow {
		if beforeFieldValue != afterRow[i] {
			return true
		}
	}

	return false
}
