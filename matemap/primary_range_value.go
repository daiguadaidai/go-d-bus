package matemap

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/logger"
)

// 保存主键一个范围的值
type PrimaryRangeValue struct {
	TimestampHash string                 // 生成该范围数据的时间戳
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
func NewPrimaryRangeValue(_timestampHash string, _schema string, _table string,
	_minValue map[string]interface{}, _maxValue map[string]interface{}) *PrimaryRangeValue {

	if _timestampHash < "0" {
		_timestampHash = common.GetCurrentTimestampMS()
	}

	return &PrimaryRangeValue{
		TimestampHash: _timestampHash,
		Schema:        _schema,
		Table:         _table,
		MinValue:      _minValue,
		MaxValue:      _maxValue,
	}
}

/* 获取当前主键范围值的最大主键值
通过指定的顺序主键名获取对应的主键值
Params:
    _tablePKNames: 主键名
*/
func (this *PrimaryRangeValue) GetMaxValueSlice(_tablePKNames []string) []interface{} {
	maxValueSlice := make([]interface{}, 0, 1)

	for _, tablePKName := range _tablePKNames {
		maxValueSlice = append(maxValueSlice, this.MaxValue[tablePKName])
	}

	return maxValueSlice
}

/* 获取当前主键范围值的最小主键值
通过指定的顺序主键名获取对应的主键值
Params:
    _tablePKNames: 主键名
*/
func (this *PrimaryRangeValue) GetMinValueSlice(_tablePKNames []string) []interface{} {
	minValueSlice := make([]interface{}, 0, 1)

	for _, tablePKName := range _tablePKNames {
		minValueSlice = append(minValueSlice, this.MinValue[tablePKName])
	}

	return minValueSlice
}

/* 获取当前主键范围值的 最小和最大 主键值
通过指定的顺序主键名获取对应的主键值
Params:
    _tablePKNames: 主键名
*/
func (this *PrimaryRangeValue) GetMinMaxValueSlice(_tablePKNames []string) []interface{} {
	minMaxValueSlice := make([]interface{}, 0, 1)

	minValueSlice := this.GetMinValueSlice(_tablePKNames)
	maxValueSlice := this.GetMaxValueSlice(_tablePKNames)

	minMaxValueSlice = append(minMaxValueSlice, minValueSlice...)
	minMaxValueSlice = append(minMaxValueSlice, maxValueSlice...)

	return minMaxValueSlice
}

/*获取下一个PrimaryRangeValue
通过但前主键范围值到数据库中查找
Params:
    _maxRowCnt: 每次查询的行数
    _host: 链接数据库 ip
    _port: 链接数据库端口
*/
func (this *PrimaryRangeValue) GetNextPrimaryRangeValue(maxRowCnt int, host string, port int,
) (*PrimaryRangeValue, error) {
	// 获取表名
	tableName := common.FormatTableName(this.Schema, this.Table, "")

	// 通过表名获取相关表映射元数据信息
	table, err := GetMigrationTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取下一个主键范围值(获取迁移的表元数据). %v. %v", tableName, err)
	}

	// 获取操作相关表的实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取下一个主键范围值. %v", host, port, tableName)
	}

	// 获取该表的主键名
	sourceTablePKNames := table.FindSourcePKColumnNames()
	// 表当前row copy到的范围值的最大值
	maxValueSlice := this.GetMaxValueSlice(sourceTablePKNames)

	selectSql := table.GetSelPerBatchMaxPKSqlTpl(maxRowCnt)
	row := instance.QueryRow(selectSql, maxValueSlice...)
	nextValue, err := common.Row2Map(row, sourceTablePKNames, table.FindSourcePKColumnTypes())
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表row copy 下一个主键值(row 装换map出错). %v. %v. %v", tableName, err, selectSql)
	}
	logger.M.Infof("成功. 获取表 row copy 下一个主键值, %v: %v", tableName, nextValue)

	nextPrimaryRangeValue := NewPrimaryRangeValue("-1", this.Schema, this.Table, this.MaxValue, nextValue)

	return nextPrimaryRangeValue, nil
}
