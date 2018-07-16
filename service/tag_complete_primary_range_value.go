package service

import "github.com/daiguadaidai/go-d-bus/matemap"

type TagCompletePrimaryRangeValue struct {
	IsComplete        bool
	PrimaryRangeValue *matemap.PrimaryRangeValue
}

/* 创建一个新的标记是否row copy 完成的 PrimaryRangeValue

 */
func NewTagCompletePrimaryRangeValue(
	_isComplete bool,
	_primaryRangeValue *matemap.PrimaryRangeValue,
) *TagCompletePrimaryRangeValue {

	return &TagCompletePrimaryRangeValue{
		IsComplete:        _isComplete,
		PrimaryRangeValue: _primaryRangeValue,
	}
}
