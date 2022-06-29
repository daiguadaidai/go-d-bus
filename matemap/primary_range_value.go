package matemap

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/dao/daohelper"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/logger"
	"strings"
)

// 保存主键一个范围的值
type PrimaryRangeValue struct {
	TimestampHash string                 // 生成该范围数据的时间戳
	Schema        string                 // 数据库名
	Table         string                 // 表名
	MinValue      map[string]interface{} // 一个范围最小的主键ID值
	MaxValue      map[string]interface{} // 一个范围最大的主键ID值
	NextValue     map[string]interface{} // 下一个访问开始当主键ID值
}

/*获取新的PrimaryRangeValue
Params:
    timestampHash: 时间戳hash
    schema: 数据库名
    table: 表名
    minValue: 最小值
    maxValue: 最大值
*/
func NewPrimaryRangeValue(
	timestampHash string,
	schema string,
	table string,
	minValue map[string]interface{},
	maxValue map[string]interface{},
	nextValue map[string]interface{},
) *PrimaryRangeValue {

	if timestampHash < "0" {
		timestampHash = common.GetCurrentTimestampMS()
	}

	return &PrimaryRangeValue{
		TimestampHash: timestampHash,
		Schema:        schema,
		Table:         table,
		MinValue:      minValue,
		MaxValue:      maxValue,
		NextValue:     nextValue,
	}
}

/* 获取当前主键范围值的最大主键值
通过指定的顺序主键名获取对应的主键值
Params:
    _tablePKNames: 主键名
*/
func (this *PrimaryRangeValue) GetMaxValueSlice(tablePKNames []string) []interface{} {
	maxValueSlice := make([]interface{}, 0, 1)

	for _, tablePKName := range tablePKNames {
		maxValueSlice = append(maxValueSlice, this.MaxValue[tablePKName])
	}

	return maxValueSlice
}

/* 获取当前主键范围值的下一个值, 该值是下一个范围的最小值
通过指定的顺序主键名获取对应的主键值
Params:
    tablePKNames: 主键名
*/
func (this *PrimaryRangeValue) GetNextRangeFirstValueSlice(tablePKNames []string) []interface{} {
	nextValueSlice := make([]interface{}, 0, 1)

	for _, tablePKName := range tablePKNames {
		nextValueSlice = append(nextValueSlice, this.NextValue[tablePKName])
	}

	return nextValueSlice
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
func (this *PrimaryRangeValue) GetMinMaxValueSlice(tablePKNames []string) []interface{} {
	minMaxValueSlice := make([]interface{}, 0, 1)

	minValueSlice := this.GetMinValueSlice(tablePKNames)
	maxValueSlice := this.GetMaxValueSlice(tablePKNames)

	minMaxValueSlice = append(minMaxValueSlice, minValueSlice...)
	minMaxValueSlice = append(minMaxValueSlice, maxValueSlice...)

	return minMaxValueSlice
}

/*获取下一个PrimaryRangeValue
通过但前主键范围值到数据库中查找
Params:
    maxRowCnt: 每次查询的行数
    host: 链接数据库 ip
    port: 链接数据库端口
*/
/*
func (this *PrimaryRangeValue) GetNextPrimaryRangeValue(maxRowCnt int, host string, port int) (*PrimaryRangeValue, error) {
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
	nextValue, err := daohelper.Row2Map(row, sourceTablePKNames, table.FindSourcePKColumnTypes())
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表row copy 下一个主键值(row 装换map出错). %v. %v. %v", tableName, err, selectSql)
	}
	logger.M.Infof("成功. 获取表 row copy 下一个主键值, %v: %v", tableName, nextValue)

	nextPrimaryRangeValue := NewPrimaryRangeValue("-1", this.Schema, this.Table, this.MaxValue, nextValue)

	return nextPrimaryRangeValue, nil
}
*/

/*获取下一个PrimaryRangeValue
通过但前主键范围值到数据库中查找
Params:
    maxRowCnt: 每次查询的行数
    host: 链接数据库 ip
    port: 链接数据库端口
*/
func (this *PrimaryRangeValue) GetNextPrimaryRangeValueV2(maxRowCnt int, host string, port int) (*PrimaryRangeValue, error) {
	if this.NextValue == nil {
		return nil, nil
	}

	// 获取表名
	tableName := common.FormatTableName(this.Schema, this.Table, "")

	// 通过表名获取相关表映射元数据信息
	table, err := GetMigrationTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取当前主键值和下一个主键范围值(获取迁移的表元数据). %v. %v", tableName, err)
	}

	// 获取操作相关表的实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取当前主键值和下一个主键范围值. %v", host, port, tableName)
	}

	// 获取该表的主键名
	sourceTablePKNames := table.FindSourcePKColumnNames()
	// 获取下一个范围的第一个值
	firstValueSlice := this.GetNextRangeFirstValueSlice(sourceTablePKNames)

	selectSql := table.GetSelCurrAndNextPKSqlTpl(maxRowCnt)
	rows, err := instance.Query(selectSql, firstValueSlice...)
	if err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			logger.M.Warnf("获取当前和下一个主键, 没数据. %v.%v. %v", this.Schema, this.Table, err.Error())
		} else {
			return nil, fmt.Errorf("获取当前和下一个主键出错. %v.%v. %v", this.Schema, this.Table, err.Error())
		}
	}
	defer rows.Close()

	rowMaps, err := daohelper.RowsToMaps(rows, sourceTablePKNames, table.FindSourcePKColumnTypes())
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表row copy 获取当前主键值和下一个主键范围值(row装换map出错). %v. %v. %v", tableName, err, selectSql)
	}
	logger.M.Infof("成功. 获取表 row copy 当前和下一个主键值, %v: %v", tableName, common.ToJsonStr(rowMaps))

	var nextPrimaryRangeValue *PrimaryRangeValue
	if len(rowMaps) == 0 {
		return nil, nil
	} else if len(rowMaps) == 1 {
		nextPrimaryRangeValue = NewPrimaryRangeValue("-1", this.Schema, this.Table, this.NextValue, rowMaps[0], nil)
	} else if len(rowMaps) == 2 {
		nextPrimaryRangeValue = NewPrimaryRangeValue("-1", this.Schema, this.Table, this.NextValue, rowMaps[0], rowMaps[1])
	}

	return nextPrimaryRangeValue, nil
}
