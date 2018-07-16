package service

import "github.com/daiguadaidai/go-d-bus/matemap"

/* 定义 AddOrDelte 的类型
前缀: AOD 代表 AddOrDelete 缩写
*/
const (
	AOD_TYPE_ADD    = 0
	AOD_TYPE_DELETE = 1
)

type AddOrDelete struct {
	Schema            string
	Table             string
	TimestampHash     string
	Type              int
	PrimaryRangeValue *matemap.PrimaryRangeValue
}

/* 新建一个 在RowCopy WaitingTagCompletePrirmaryRangeValueMap变量中,
   传输是要添加还是删除 WaitingTagCompletePrirmaryRangeValueMap 中的primaryrangevalue值
Params:
    _schema: 数据库名
    _table: 表名
    _type: 是删除还是添加
    _timestampHash: 需要删除或添加的hash值
    _primaryRangeValue: 需要添加的primaryRangeValue
*/
func NewAddOrDelete(_schema string, _table string, _timestampHash string, _type int,
	_primaryRangeValue *matemap.PrimaryRangeValue) *AddOrDelete {

	return &AddOrDelete{
		Schema:            _schema,
		Table:             _table,
		Type:              _type,
		TimestampHash:     _timestampHash,
		PrimaryRangeValue: _primaryRangeValue,
	}
}
