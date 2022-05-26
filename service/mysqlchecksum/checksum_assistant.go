package mysqlchecksum

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/model"
)

/* 获取源实例的 checksum code
Params:
	_host: 实例 host
	_port: 实例 port
	_primaryRangeValue: 需要进行checksum的值范围
	_table: 需要迁移的表元数据信息
*/
func GetSourceRowsChecksumCode(host string, port int, priamryRangeValue *matemap.PrimaryRangeValue, table *matemap.Table) (int, error) {
	var checksumCode sql.NullInt64

	// 获取需要迁移的表的最小最大的主键值, 用于多行checksum sql语句的占位符
	primaryMinValue := priamryRangeValue.GetMinMaxValueSlice(table.FindSourcePKColumnNames())

	// 获取数据库实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return int(checksumCode.Int64), fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取源数据库实例(获取多行checksum code时)", common.CurrLine(), host, port)
	}

	if err := instance.QueryRow(table.GetSelSourceRowsChecksumSqlTpl(), primaryMinValue...).Scan(&checksumCode); err != nil {
		return int(checksumCode.Int64), fmt.Errorf("%v: 失败. 获取源实例表多行checksum值. %v:%v. %v:%v. min:%v, max:%v. %v",
			common.CurrLine(), priamryRangeValue.Schema, priamryRangeValue.Table, host, port, priamryRangeValue.MinValue, priamryRangeValue.MaxValue, err)
	}

	return int(checksumCode.Int64), nil
}

/* 获取目标实例的 checksum code
Params:
	_host: 实例 host
	_port: 实例 port
	_primaryRangeValue: 需要进行checksum的值范围
	_table: 需要迁移的表元数据信息
*/
func GetTargetRowsChecksumCode(host string, port int, priamryRangeValue *matemap.PrimaryRangeValue, table *matemap.Table) (int, error) {
	var checksumCode sql.NullInt64

	// 获取需要迁移的表的最小最大的主键值, 用于多行checksum sql语句的占位符
	primaryMinValue := priamryRangeValue.GetMinMaxValueSlice(table.FindSourcePKColumnNames())

	// 获取数据库实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return int(checksumCode.Int64), fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取目标数据库实例(获取多行checksum code时)", common.CurrLine(), host, port)
	}

	if err := instance.QueryRow(table.GetSelTargetRowsChecksumSqlTpl(), primaryMinValue...).Scan(&checksumCode); err != nil {
		return int(checksumCode.Int64), fmt.Errorf("%v: 失败. 获取目标实例表多行checksum值. %v:%v. %v:%v. min:%v, max:%v. %v",
			common.CurrLine(), priamryRangeValue.Schema, priamryRangeValue.Table, host, port, priamryRangeValue.MinValue, priamryRangeValue.MaxValue, err)
	}

	return int(checksumCode.Int64), nil
}

/* 生成一条不一致数据
Params:
    _taskUUID: 任务UUID
    _primaryRangeValue: 表的主键范围值
*/
func CreateDiffRecord(taskUUID string, priamryRangeValue *matemap.PrimaryRangeValue) error {
	// 获取需要迁移的表的元数据
	table, err := matemap.GetMigrationTableBySchemaTable(priamryRangeValue.Schema, priamryRangeValue.Table)
	if err != nil {
		return fmt.Errorf("%v: 失败. 获取目标需要迁移的表(保存不一致数据). %v:%v. %v", common.CurrLine(), priamryRangeValue.Schema, priamryRangeValue.Table, err)
	}

	minValue, err := common.Map2Json(priamryRangeValue.MinValue) // 获取范围最小值
	maxValue, err := common.Map2Json(priamryRangeValue.MaxValue) // 获取范围最大值

	diffRecord := &model.DataChecksum{
		TaskUUID:     sql.NullString{taskUUID, true},
		SourceSchema: sql.NullString{table.SourceSchema, true},
		SourceTable:  sql.NullString{table.SourceName, true},
		TargetSchema: sql.NullString{table.TargetSchema, true},
		TargetTable:  sql.NullString{table.TargetName, true},
		MinIDValue:   sql.NullString{minValue, true},
		MaxIDValue:   sql.NullString{maxValue, true},
	}

	if err := new(dao.DataChecksumDao).Create(diffRecord); err != nil {
		return fmt.Errorf("%v: 失败, 创建不一致记录. taskUUID: %v, %v.%v -> %v.%v min: %v, max: %v",
			taskUUID, table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName, minValue, maxValue)
	}

	return nil
}

/* 获取还没修复的不一致记录
Params:
	_taskUUID: 任务ID
*/
func FindNoFixDiffRecords(taskUUID string) ([]model.DataChecksum, error) {
	dataChecksumDao := new(dao.DataChecksumDao)

	columnStr := "id, task_uuid, source_schema, source_table, target_schema, target_table, min_id_value, max_id_value"
	records, err := dataChecksumDao.FindNoFixByTaskUUID(taskUUID, columnStr)
	if err != nil {
		return nil, err
	}

	return records, nil
}

/* 获取源数据主键范围值的所有行
Param:
	_host: 实例host
	_port: 实例端口
	_primaryRangeValue 主键范围值
	_table 需要迁移的表元数据
*/
func FindSourcePKRows(host string, port int, primaryRangeValue *matemap.PrimaryRangeValue, table *matemap.Table) ([][]interface{}, error) {
	// 获取源实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取源实例失败(修复数据, 获取所有主键值)", common.CurrLine(), host, port)
	}

	// 获取范围值, 用于sql语句中的占位符
	minMaxValue := primaryRangeValue.GetMinMaxValueSlice(table.FindSourcePKColumnNames())
	rows, err := instance.Query(table.GetSelPerBatchSourcePKSqlTpl(), minMaxValue...)
	defer rows.Close()
	if err != nil {
		return nil, fmt.Errorf("%v: 查询需要fix数据的所有主键值(修复数据, 获取所有主键值). %v. %v", common.CurrLine(), table.GetSelPerBatchSourcePKSqlTpl(), err)
	}

	columns, _ := rows.Columns()
	columnSize := len(columns)
	scanArgs := make([]interface{}, columnSize) // 扫描使用
	values := make([]interface{}, columnSize)   // 映射使用
	for i := range values {
		scanArgs[i] = &values[i]
	}

	data := make([][]interface{}, 0, 1) // 最终所有的数据
	for rows.Next() {
		// 生成每一行
		row := make([]interface{}, columnSize)
		//将行数据保存到record字典
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("%v: scan 字段, 查询需要fix数据的所有主键值(修复数据, 获取所有主键值). %v:%v %v", common.CurrLine(), host, port, err)
		}
		for i, col := range values {
			row[i] = col
		}
		data = append(data, row)
	}

	return data, nil
}

/* 获取源实例单行数据的checksum值
Params:
	_host: 实例ip
	_port: 实例端口
	_primaryValues: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
*/
func GetSourceRowChecksumCode(host string, port int, primaryValues []interface{}, table *matemap.Table) (int, error) {
	var checksumCode sql.NullInt64

	// 获取数据库实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return int(checksumCode.Int64), fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取源数据库实例(获取单行checksum code时)", common.CurrLine(), host, port)
	}

	if err := instance.QueryRow(table.GetSelSourceRowChecksumSqlTpl(), primaryValues...).Scan(&checksumCode); err != nil {
		return int(checksumCode.Int64), fmt.Errorf("%v: 失败. 获取源实例表单行checksum值. %v:%v. %v:%v. primary: %v. %v",
			common.CurrLine(), table.SourceSchema, table.SourceName, host, port, primaryValues, err)
	}

	return int(checksumCode.Int64), nil
}

/* 获取目标实例单行数据的checksum值
Params:
	_host: 实例ip
	_port: 实例端口
	_primaryValues: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
*/
func GetTargetRowChecksumCode(host string, port int, primaryValues []interface{}, table *matemap.Table) (int, error) {
	var checksumCode sql.NullInt64

	// 获取数据库实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return int(checksumCode.Int64), fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取目标数据库实例(获取单行checksum code时)", common.CurrLine(), host, port)
	}

	if err := instance.QueryRow(table.GetSelTargetRowChecksumSqlTpl(), primaryValues...).Scan(&checksumCode); err != nil {
		return int(checksumCode.Int64), fmt.Errorf("%v: 失败. 获取目标实例表单行checksum值. %v:%v. %v:%v. primary: %v. %v",
			common.CurrLine(), table.SourceSchema, table.SourceName, host, port, primaryValues, err)
	}

	return int(checksumCode.Int64), nil
}

/* 通过主键删除目标行
Params:
	_host: 实例ip
	_port: 实例端口
	_primaryValues: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
*/
func DeleteTargetRow(host string, port int, primaryValues []interface{}, table *matemap.Table) error {
	// 获取实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 通过主键删除目标行", common.CurrLine(), host, port)
	}

	// 开启事物执行sql
	if _, err := instance.Exec(table.GetDelSqlTpl(), primaryValues...); err != nil {
		return fmt.Errorf("%v: (%v:%v). 通过主键删除目标行. %v", common.CurrLine(), host, port, err)
	}

	return nil
}

/* 通过主键值获取源表数据
Params:
	_host: 实例ip
	_port: 实例端口
	_primaryValues: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
*/
func GetSourceRowByPK(host string, port int, primaryValues []interface{}, table *matemap.Table) ([]interface{}, error) {
	// 获取数据库实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取源数据库实例(进行修复数据, 通过主键获取源表数据)", common.CurrLine(), host, port)
	}

	columnLen := len(table.SourceUsefulColumns)
	values := make([]interface{}, columnLen)   // 数据库原生二进制值
	scanArgs := make([]interface{}, columnLen) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rows, err := instance.Query(table.GetSelSourceRowSqlTpl(), primaryValues...)
	defer rows.Close()
	if err != nil {
		return nil, fmt.Errorf("%v: 查询数据库失败, 通过主键在源表获取需要修复的行数据. %v. value: %v %v", common.CurrLine(), table.GetSelSourceRowSqlTpl, primaryValues, err)
	}

	var result []interface{} = nil
	for rows.Next() {
		result = make([]interface{}, columnLen)

		//将行数据保存到record字典
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("%v: scan字段数据错误, 通过主键在源表获取需要修复的行数据 %v", common.CurrLine(), err)
		}

		for i, col := range values {
			result[i] = col
		}
	}

	return result, nil
}

/* 通过主键 repalce into 目标行
Params:
	_host: 实例ip
	_port: 实例端口
	_sourceRow: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
*/
func ReplaceTargetRow(host string, port int, sourceRow []interface{}, table *matemap.Table) error {
	// 获取实例
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 通过主键 repalce into 目标行", common.CurrLine(), host, port)
	}

	// 开启事物执行sql
	if _, err := instance.Exec(table.GetRepPerBatchSqlTpl(1), sourceRow...); err != nil {
		return err
	}

	return nil
}

/* 通过不一致数据进行转换成 数据范围
Params:
	_record: 不一致记录
	_table: 需要迁移的表元数据信息
*/
func diffRecord2PrimaryRangeValue(record model.DataChecksum, table *matemap.Table) (*matemap.PrimaryRangeValue, error) {

	minValue, err := common.Json2MapBySqlType(record.MinIDValue.String, table.FindSourcePKColumnTypeMap())
	if err != nil {
		return nil, fmt.Errorf("%v: 失败. 将最小ID值 JSON -> Map. 生成checksum需要进行fix的记录. %v.%v. min: %v, max: %v. %v",
			common.CurrLine(), record.SourceSchema.String, record.SourceTable.String, record.MinIDValue.String, record.MinIDValue.String, err)
	}

	maxValue, err := common.Json2MapBySqlType(record.MaxIDValue.String, table.FindSourcePKColumnTypeMap())
	if err != nil {
		return nil, fmt.Errorf("%v: 失败. 将最大ID值 JSON -> Map. 生成checksum需要进行fix的记录. %v.%v. min: %v, max: %v. %v",
			common.CurrLine(), record.SourceSchema.String, record.SourceTable.String, record.MinIDValue.String, record.MinIDValue.String, err)
	}
	primaryRangeValue := matemap.NewPrimaryRangeValue("-1", record.SourceSchema.String, record.SourceTable.String, minValue, maxValue)

	return primaryRangeValue, nil
}

/* 标记记录已经修复
Params:
	_id: 需要修复数据的记录ID
*/
func TagDiffRecordFixed(_id int64) int {
	dataChecksumDao := new(dao.DataChecksumDao)

	affected := dataChecksumDao.FixCompletedByID(_id)

	return affected
}

// 还需要修复的记录数自增
func (this *Checksum) IncrNeedFixRecordCounter() {
	this.NeedFixRecordCounterRWMutex.Lock()
	this.NeedFixRecordCounter++
	this.NeedFixRecordCounterRWMutex.Unlock()
}

// 还需要修复的记录数自减
func (this *Checksum) DecrNeedFixRecordCounter() {
	this.NeedFixRecordCounterRWMutex.Lock()
	this.NeedFixRecordCounter--
	this.NeedFixRecordCounterRWMutex.Unlock()
}

// 获取还需要修复的记录数
func (this *Checksum) GetNeedFixRecordCounter() int {
	this.NeedFixRecordCounterRWMutex.RLock()
	counter := this.NeedFixRecordCounter
	this.NeedFixRecordCounterRWMutex.RUnlock()

	return counter
}
