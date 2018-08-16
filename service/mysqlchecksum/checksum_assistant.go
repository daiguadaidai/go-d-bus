package mysqlchecksum

import (
	"github.com/daiguadaidai/go-d-bus/matemap"
	"fmt"
	"github.com/juju/errors"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/model"
	"database/sql"
	"github.com/daiguadaidai/go-d-bus/dao"
)

/* 获取源实例的 checksum code
Params:
	_host: 实例 host
	_port: 实例 port
	_primaryRangeValue: 需要进行checksum的值范围
	_table: 需要迁移的表元数据信息
 */
func GetSourceRowsChecksumCode(
	_host string,
	_port int,
	_priamryRangeValue *matemap.PrimaryRangeValue,
	_table *matemap.Table,
) (int, error) {
	var checksumCode sql.NullInt64

	// 获取需要迁移的表的最小最大的主键值, 用于多行checksum sql语句的占位符
	primaryMinValue := _priamryRangeValue.GetMinMaxValueSlice(_table.FindSourcePKColumnNames())

	// 获取数据库实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取源数据库实例(获取多行checksum code时). %v:%v. %v",
			common.CurrLine(), _host, _port, err)
		return int(checksumCode.Int64), errors.New(errMSG)
	}

	err = instance.DB.QueryRow(_table.GetSelSourceRowsChecksumSqlTpl(), primaryMinValue...).Scan(&checksumCode)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取源实例表多行checksum值. %v:%v. %v:%v. " +
			"min:%v, max:%v. %v",
			common.CurrLine(), _priamryRangeValue.Schema, _priamryRangeValue.Table, _host, _port,
			_priamryRangeValue.MinValue, _priamryRangeValue.MaxValue, err)
		return int(checksumCode.Int64), errors.New(errMSG)
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
func GetTargetRowsChecksumCode(
	_host string,
	_port int,
	_priamryRangeValue *matemap.PrimaryRangeValue,
	_table *matemap.Table,
) (int, error) {
	var checksumCode sql.NullInt64

	// 获取需要迁移的表的最小最大的主键值, 用于多行checksum sql语句的占位符
	primaryMinValue := _priamryRangeValue.GetMinMaxValueSlice(_table.FindSourcePKColumnNames())

	// 获取数据库实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标数据库实例(获取多行checksum code时). %v:%v. %v",
			common.CurrLine(), _host, _port, err)
		return int(checksumCode.Int64), errors.New(errMSG)
	}

	err = instance.DB.QueryRow(_table.GetSelTargetRowsChecksumSqlTpl(), primaryMinValue...).Scan(&checksumCode)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标实例表多行checksum值. %v:%v. %v:%v. " +
			"min:%v, max:%v. %v",
			common.CurrLine(), _priamryRangeValue.Schema, _priamryRangeValue.Table, _host, _port,
			_priamryRangeValue.MinValue, _priamryRangeValue.MaxValue, err)
		return int(checksumCode.Int64), errors.New(errMSG)
	}

	return int(checksumCode.Int64), nil
}

/* 生成一条不一致数据
Params:
    _taskUUID: 任务UUID
    _primaryRangeValue: 表的主键范围值
 */
func CreateDiffRecord(_taskUUID string, _priamryRangeValue *matemap.PrimaryRangeValue) error {
	// 获取需要迁移的表的元数据
	table, err := matemap.GetMigrationTableBySchemaTable(_priamryRangeValue.Schema, _priamryRangeValue.Table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标需要迁移的表(保存不一致数据). %v:%v. %v",
			common.CurrLine(), _priamryRangeValue.Schema, _priamryRangeValue.Table, err)
		return errors.New(errMSG)
	}

	minValue, err:= common.Map2Json(_priamryRangeValue.MinValue) // 获取范围最小值
	maxValue, err:= common.Map2Json(_priamryRangeValue.MaxValue) // 获取范围最大值

	diffRecord := model.DataChecksum{
		TaskUUID: sql.NullString{_taskUUID, true},
		SourceSchema: sql.NullString{table.SourceSchema, true},
		SourceTable: sql.NullString{table.SourceName, true},
		TargetSchema: sql.NullString{table.TargetSchema, true},
		TargetTable: sql.NullString{table.TargetName, true},
		MinIDValue: sql.NullString{minValue, true},
		MaxIDValue: sql.NullString{maxValue, true},
	}

	dataChecksumDao := new(dao.DataChecksumDao)
	err = dataChecksumDao.Create(diffRecord)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败, 创建不一致记录. taskUUID: %v, %v.%v -> %v.%v" +
			"min: %v, max: %v",
			_taskUUID, table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName,
			minValue, maxValue)
		return errors.New(errMSG)
	}

	return nil
}

/* 获取还没修复的不一致记录
Params:
	_taskUUID: 任务ID
 */
func FindNoFixDiffRecords(_taskUUID string) ([]model.DataChecksum, error) {
	dataChecksumDao := new(dao.DataChecksumDao)

	columnStr := "id, task_uuid, source_schema, source_table, target_schema, target_table, " +
		"min_id_value, max_id_value"
	records, err := dataChecksumDao.FindNoFixByTaskUUID(_taskUUID, columnStr)
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
func FindSourcePKRows(
	_host string,
	_port int,
	_primaryRangeValue *matemap.PrimaryRangeValue,
	_table *matemap.Table,
) ([][]interface{}, error) {
	// 获取源实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 获取源实例失败(修复数据, 获取所有主键值). %v",
			common.CurrLine(), err)
		return nil, errors.New(errMSG)
	}

	// 获取范围值, 用于sql语句中的占位符
	minMaxValue := _primaryRangeValue.GetMinMaxValueSlice(_table.FindSourcePKColumnNames())
	rows, err := instance.DB.Query(_table.GetSelPerBatchSourcePKSqlTpl(), minMaxValue...)
	defer rows.Close()
	if err != nil {
		errMSG := fmt.Sprintf("%v: 查询需要fix数据的所有主键值(修复数据, 获取所有主键值). %v. %v",
			common.CurrLine(), _table.GetSelPerBatchSourcePKSqlTpl(), err)
		return nil, errors.New(errMSG)
	}

	columns, _ := rows.Columns()
	columnSize := len(columns)
	scanArgs := make([]interface{}, columnSize) // 扫描使用
	values := make([]interface{}, columnSize) // 映射使用
	for i := range values {
		scanArgs[i] = &values[i]
	}

	data := make([][]interface{}, 0, 1) // 最终所有的数据
	for rows.Next() {
		// 生成每一行
		row := make([]interface{}, columnSize)
		//将行数据保存到record字典
		err = rows.Scan(scanArgs...)
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
func GetSourceRowChecksumCode(
	_host string,
	_port int,
	_primaryValues []interface{},
	_table *matemap.Table,
) (int, error) {
	var checksumCode sql.NullInt64

	// 获取数据库实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取源数据库实例(获取单行checksum code时). %v:%v. %v",
			common.CurrLine(), _host, _port, err)
		return int(checksumCode.Int64), errors.New(errMSG)
	}

	err = instance.DB.QueryRow(_table.GetSelSourceRowChecksumSqlTpl(), _primaryValues...).Scan(&checksumCode)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取源实例表单行checksum值. %v:%v. %v:%v. " +
			"primary: %v. %v",
			common.CurrLine(), _table.SourceSchema, _table.SourceName, _host, _port,
			_primaryValues, err)
		return int(checksumCode.Int64), errors.New(errMSG)
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
func GetTargetRowChecksumCode(
	_host string,
	_port int,
	_primaryValues []interface{},
	_table *matemap.Table,
) (int, error) {
	var checksumCode sql.NullInt64

	// 获取数据库实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标数据库实例(获取单行checksum code时). %v:%v. %v",
			common.CurrLine(), _host, _port, err)
		return int(checksumCode.Int64), errors.New(errMSG)
	}

	err = instance.DB.QueryRow(_table.GetSelTargetRowChecksumSqlTpl(), _primaryValues...).Scan(&checksumCode)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标实例表单行checksum值. %v:%v. %v:%v. " +
			"primary: %v. %v",
			common.CurrLine(), _table.SourceSchema, _table.SourceName, _host, _port,
			_primaryValues, err)
		return int(checksumCode.Int64), errors.New(errMSG)
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
func DeleteTargetRow(
	_host string,
	_port int,
	_primaryValues []interface{},
	_table *matemap.Table,
) error {
	// 获取实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		return err
	}

	// 开启事物执行sql
	tx, err := instance.DB.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(_table.GetDelSqlTpl(), _primaryValues...)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
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
func GetSourceRowByPK(
	_host string,
	_port int,
	_primaryValues []interface{},
	_table *matemap.Table,
) ([]interface{}, error) {

	// 获取数据库实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取源数据库实例(进行修复数据, 通过主键获取源表数据)." +
			"%v:%v. %v",
			common.CurrLine(), _host, _port, err)
		return nil, errors.New(errMSG)
	}

	columnLen := len(_table.SourceUsefulColumns)
	values := make([]interface{}, columnLen)   // 数据库原生二进制值
	scanArgs := make([]interface{}, columnLen) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rows, err := instance.DB.Query(_table.GetSelSourceRowSqlTpl(), _primaryValues...)
	defer rows.Close()
	if err != nil {
		errMSG := fmt.Sprintf("%v: 查询数据库失败, 通过主键在源表获取需要修复的行数据. " +
			"%v. value: %v %v",
			common.CurrLine(), _table.GetSelSourceRowSqlTpl, _primaryValues, err)
		return nil, errors.New(errMSG)
	}

	var result []interface{} = nil
	for rows.Next() {
		result = make([]interface{}, columnLen)

		//将行数据保存到record字典
		err = rows.Scan(scanArgs...)
		if err != nil {
			errMSG := fmt.Sprintf("%v: scan字段数据错误, 通过主键在源表获取需要修复的行数据 %v",
				common.CurrLine(), err)
			return nil, errors.New(errMSG)
		}

		for i, col := range values {
			result[i] = col
		}
	}

	return result, nil
}

/* 通过主键删除目标行
Params:
	_host: 实例ip
	_port: 实例端口
	_sourceRow: 获取单行数据的sql的 where 占位符的值
	_table: 需要迁移的表的元数据信息
 */
func ReplaceTargetRow(
	_host string,
	_port int,
	_sourceRow []interface{},
	_table *matemap.Table,
) error {
	// 获取实例
	instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		return err
	}

	// 开启事物执行sql
	tx, err := instance.DB.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(_table.GetRepPerBatchSqlTpl(1), _sourceRow...)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
	}

	return nil
}

/* 通过不一致数据进行转换成 数据范围
Params:
	_record: 不一致记录
	_table: 需要迁移的表元数据信息
 */
func diffRecord2PrimaryRangeValue(
	_record model.DataChecksum,
	_table *matemap.Table,
) (*matemap.PrimaryRangeValue, error) {

	minValue, err := common.Json2MapBySqlType(_record.MinIDValue.String, _table.FindSourcePKColumnTypeMap())
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 将最小ID值 JSON -> Map. 生成checksum需要进行fix的记录. " +
			"%v.%v. min: %v, max: %v. %v",
			common.CurrLine(), _record.SourceSchema.String, _record.SourceTable.String,
			_record.MinIDValue.String, _record.MinIDValue.String, err)
		return nil, errors.New(errMSG)
	}
	maxValue, err := common.Json2MapBySqlType(_record.MaxIDValue.String, _table.FindSourcePKColumnTypeMap())
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 将最大ID值 JSON -> Map. 生成checksum需要进行fix的记录. " +
			"%v.%v. min: %v, max: %v. %v",
			common.CurrLine(), _record.SourceSchema.String, _record.SourceTable.String,
			_record.MinIDValue.String, _record.MinIDValue.String, err)
		return nil, errors.New(errMSG)
	}
	primaryRangeValue := matemap.NewPrimaryRangeValue(
		"-1",
		_record.SourceSchema.String,
		_record.SourceTable.String,
		minValue,
		maxValue,
	)

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
	this.NeedFixRecordCounter ++
	this.NeedFixRecordCounterRWMutex.Unlock()
}

// 还需要修复的记录数自减
func (this *Checksum) DecrNeedFixRecordCounter() {
	this.NeedFixRecordCounterRWMutex.Lock()
	this.NeedFixRecordCounter --
	this.NeedFixRecordCounterRWMutex.Unlock()
}

// 获取还需要修复的记录数
func (this *Checksum) GetNeedFixRecordCounter() int {
	this.NeedFixRecordCounterRWMutex.RLock()
	counter := this.NeedFixRecordCounter
	this.NeedFixRecordCounterRWMutex.RUnlock()

	return counter
}
