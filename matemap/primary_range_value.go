package matemap

// 保存主键一个范围的值
type PrimaryRangeValue struct {
    TimestampHash int                    // 生成该范围数据的时间戳
	Schema        string                 // 数据库名
	Table         string                 // 表名
	MinValue      map[string]interface{} // 一个范围最小的主键ID值
	MaxValue      map[string]interface{} // 一个范围最大的主键ID值
}

/*获取一个PrimaryRangeValue
Params:
    _lastPrimaryRangeValue: 上一个PrimaryRangeValue
 */
func NewPrimaryRangeValue(_lastPrimaryRangeValue *PrimaryRangeValue) *PrimaryRangeValue {

	return nil
}
