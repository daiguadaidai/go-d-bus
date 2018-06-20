package matemap

// 保存主键一个范围的值
type PrimaryRangeValue struct {
    TimestampHash int                    // 生成该范围数据的时间戳
	Schema        string                 // 数据库名
	Table         string                 // 表名
	MinValue      map[string]interface{} // 一个范围最小的主键ID值
	MaxValue      map[string]interface{} // 一个范围最大的主键ID值
}

/*获取新的PrimaryRangeValue
Params:
    _timestampHash: 时间戳hash
    _schema: 数据库名
    _table: 表名
    _minValue: 最小值
    _maxValue: 最大值
 */
func NewPrimaryRangeValue(_timestampHash int, _schema string, _table string,
	_minValue map[string]interface{}, _maxValue map[string]interface{}) *PrimaryRangeValue {

	return &PrimaryRangeValue{
		TimestampHash: _timestampHash,
		Schema: _schema,
		Table: _table,
		MinValue: _minValue,
		MaxValue: _maxValue,
	}
}

/*获取下一个PrimaryRangeValue
Params:
    _lastPrimaryRangeValue: 上一个PrimaryRangeValue
 */
func GetNextPrimaryRangeValue(_lastPrimaryRangeValue *PrimaryRangeValue) *PrimaryRangeValue {

	return nil
}
