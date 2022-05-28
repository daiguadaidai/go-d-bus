package matemap

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/logger"
	"strings"
	"sync"
)

// 线程安全Map, 用于保存需要迁移的数据库表信息. key为源数据库的: schema.table
var migrationTableMap sync.Map

/* 获取需要迁移的表, 通过 schema 和 table
Params:
    _schemaName: schema 名称
    _tableName: table 名称
*/
func GetMigrationTableBySchemaTable(_schemaName string, _tableName string) (*Table, error) {
	key := config.GetTableKey(_schemaName, _tableName)
	table, err := GetMigrationTable(key)
	if err != nil {
		return nil, err
	}

	return table, nil
}

/* 通过 迁移表map 的key, key: schema.table
Params:
    _key: schema, table 组成的Map的key, schema.table
*/
func GetMigrationTable(_key string) (*Table, error) {
	tableInterface, ok := migrationTableMap.Load(_key)
	if !ok {
		return nil, fmt.Errorf("在迁移表Map中没有获取到需要迁移的表. table: %v", _key)
	}

	table := tableInterface.(interface{}).(*Table)

	return table, nil
}

/* 设置需要迁移的表元数据信息
Params:
	_key: map的key
	_table: 需要迁移的table
*/
func SetMigrationTableMap(_key string, _table *Table) {
	migrationTableMap.Store(_key, _table)
}

/* 通过配置文件创建所有需要迁移的表, 信息
Params:
    _configMap: 需要迁移的表的映射配置信息
*/
func InitMigrationTableMap(configMap *config.ConfigMap) error {

	for key, tableMap := range configMap.TableMapMap {
		migrationTable, err := NewTable(configMap, tableMap.Schema.String, tableMap.Source.String)
		if err != nil {
			return err
		}
		if migrationTable == nil {
			logger.M.Warnf("失败. 在实例中没有查找到表, 将忽略该表的迁移. %v.%v. %v:%v",
				tableMap.Schema.String, tableMap.Source.String, configMap.Source.Host.String, configMap.Source.Port.Int64)
			continue
		}

		SetMigrationTableMap(key, migrationTable)

		logger.M.Infof("成功. 初始化迁移表元信息 %v.%v-> %v.%v",
			migrationTable.SourceSchema, migrationTable.SourceName, migrationTable.TargetSchema, migrationTable.TargetName)
	}

	return nil
}

/* 创建一个新的需要迁移的数据库信息
Params:
    _configMap: 映射元数据信息
    _schemaName: 库名
    _tableName: 表名
*/
func NewTable(_configMap *config.ConfigMap, _schemaName string, _tableName string) (*Table, error) {
	var err error
	table := new(Table)

	// 初始化 源和目标 schema
	schemaKey := config.GetSchemaKey(_schemaName)
	table.SourceSchema = _configMap.SchemaMapMap[schemaKey].Source.String
	table.TargetSchema = _configMap.SchemaMapMap[schemaKey].Target.String

	// 初始化 源和目标 table
	tableKey := config.GetTableKey(_schemaName, _tableName)
	table.SourceName = _configMap.TableMapMap[tableKey].Source.String
	table.TargetName = _configMap.TableMapMap[tableKey].Target.String

	// 初始化 源 column
	sourceColumns, err := GetSourceTableColumns(table.SourceSchema, table.SourceName, _configMap.Source.Host.String, int(_configMap.Source.Port.Int64))
	if err != nil {
		return nil, err
	}
	if len(sourceColumns) == 0 || sourceColumns == nil {
		logger.M.Warnf("失败. 没有查寻到该表的字段信息, %v.%v, %v:%v",
			table.SourceSchema, table.SourceName, _configMap.Source.Host.String, int(_configMap.Source.Port.Int64))
		return nil, nil
	}
	table.SourceColumns = sourceColumns
	logger.M.Infof("成功. 获取所有的(源)字段, %v.%v", table.SourceSchema, table.SourceName)

	// 通过 源 columns 生成目标 columns, 只要 sourceColumns 有值, targetColumns 一定有值
	table.TargetColumns = GetTargetTableColumnBySourceColumns(
		_configMap, table.SourceSchema, table.SourceName, sourceColumns)
	logger.M.Infof("成功. 生成(目标)字段, 通过源字段, %v.%v", table.SourceSchema, table.SourceName)

	// 初始化列的名相关映射信息
	err = table.InitColumnMapInfo()
	if err != nil {
		return nil, err
	}
	logger.M.Infof("成功. 生成 源和目标 字段相关映射信息. %v.%v <-> %v.%v",
		table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName)

	// 添加不进行迁移的列
	ignoreColumnNames := _configMap.GetIgnoreColumnsBySchemaAndTable(table.SourceSchema, table.SourceName)
	table.SetSourceIgnoreColumns(ignoreColumnNames)
	logger.M.Infof("成功. 设置表不需要迁移的字段. %v.%v: %v", table.SourceSchema, table.SourceName, ignoreColumnNames)

	// 生成 最终需要使用到的 列, 一个表有多个列, 但是同步时可能, 只需要同步其中几个列就好了.
	table.InitSourceUsefulColumns()
	logger.M.Infof("成功. 生成需要迁移的字段. %v.%v", table.SourceSchema, table.SourceName)

	sourcePkColumnNames, err := FindSourcePKColumnNames(_configMap, table)
	if err != nil {
		return nil, err
	}
	logger.M.Infof("成功. 获得可用的主键. %v.%v %v", table.SourceSchema, table.SourceName, sourcePkColumnNames)

	// 通过可用的 (主键/唯一键), 初始化该表迁移时需要的主键
	table.InitSourcePKColumns(sourcePkColumnNames)
	logger.M.Infof("成功. 初始化源表的主键. %v.%v", table.SourceSchema, table.SourceName)
	// 通过源主键列初始化目标主键列
	table.InitTargetPKColumnsFromSource()
	logger.M.Infof("成功. 初始化目标表的主键. %v.%v <-> %v.%v", table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName)

	// 获取表所有的唯一键字段, 包括主键的
	distinctUKColumnNames, err := FindSourceDistinctUKColumnNames(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _schemaName, _tableName)
	if err != nil {
		return nil, err
	}

	// 初始化源表所有唯一键字段, 通过字段名
	err = table.InitSourceAllUKColumnsByNames(distinctUKColumnNames)
	if err != nil {
		return nil, err
	}

	// 初始化目标表的所有唯一键字段, 通过源字段名
	err = table.InitTargetAllUKColumnsBySourceUKNames(distinctUKColumnNames)
	if err != nil {
		return nil, err
	}

	// 设置目标表的建表 sql
	targetCreateTableSql, err := GetTargetCreateTableSql(_configMap, table)
	if err != nil {
		return nil, err
	}

	table.InitTargetCreateTableSql(targetCreateTableSql)

	// 初始化所有的该表相关sql语句模板
	table.InitALLSqlTpl()

	return table, nil
}

/* 获取源数据库表的所有列
Params:
    _schemaName: 数据库名称
    _tableName: 表名字
    _host: 实例 host
    _port: 实例 port
*/
func GetSourceTableColumns(schemaName string, tableName string, host string, port int) ([]Column, error) {
	columns := make([]Column, 0, 10)

	// 从数据库中获取表的所有列
	selectSql := `
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            COLUMN_TYPE,
            EXTRA
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION ASC    
    `
	logger.M.Infof("获取表 %v.%v 所有的字段", schemaName, tableName)

	// 获取数据库实例链接
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表的所有列. %v.%v ", host, port, schemaName, tableName)
	}

	// 查询数据库
	rows, err := instance.Query(selectSql, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表所有字段. %v.%v %v:%v. %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 循环创建 column
	for rows.Next() {
		var tableSchema sql.NullString
		var tableName sql.NullString
		var columnName sql.NullString
		var ordinalPosition sql.NullInt64
		var columnType sql.NullString
		var extra sql.NullString

		if err := rows.Scan(&tableSchema, &tableName, &columnName, &ordinalPosition, &columnType, &extra); err != nil {
			return nil, fmt.Errorf("scan表字段出错. %v.%v. %v", schemaName, tableName, err)
		}

		column := CreateColumn(columnName.String, columnType.String, extra.String, int(ordinalPosition.Int64))

		columns = append(columns, column)
		logger.M.Infof("成功. 添加表字段 %v.%v.%v", schemaName, tableName, columnName.String)
	}

	return columns, nil
}

/* 通过源列, 创建目标列
Params:
    _configMap: 迁移元数据信息
    _schemaName: 源数据库名称
    _tableName: 目标数据库名称
    _sourceColumns: 源表的所有列
*/
func GetTargetTableColumnBySourceColumns(_configMap *config.ConfigMap, _schemaName string,
	_tableName string, _sourceColumns []Column) []Column {

	// 目标表的所有列
	targetColumns := make([]Column, len(_sourceColumns))

	// 先拷贝所有的列
	copy(targetColumns, _sourceColumns)

	// 循环 源 所有的列, 对目标列进行修改,
	for i, column := range _sourceColumns {
		// 获取元映射信息中是否有对该列的映射
		columnKey := config.GetColumnKey(_schemaName, _tableName, column.Name)
		if columnMap, ok := _configMap.ColumnMapMap[columnKey]; ok {
			targetColumns[i].Name = columnMap.Target.String
			logger.M.Infof("成功. 发现(源)和(目标)字段有映射信息, 修改目标字段名. %v.%v.%v -> %v.%v.%v",
				_schemaName, _tableName, column.Name, _configMap.SchemaMapMap[config.GetSchemaKey(_schemaName)].Target.String, _configMap.TableMapMap[config.GetTableKey(_schemaName, _tableName)].Target.String, columnMap.Target.String)
		}
	}

	return targetColumns
}

/* 查找可用打主键
这边会有一个算法:
    1. 获取主键
    2. 如果组建不存在, 则获取唯一键
    3. 并需要判断获取的唯一键是否有在不需要同步打数据中,有的话则换一个主键或唯一键
Params:
    _configMap: 配置信息
    _schemaName: 数据库名称
    _tableName: 表名称
*/
func FindSourcePKColumnNames(_configMap *config.ConfigMap, _table *Table) ([]string, error) {

	// 获取主键
	pkColumnNames, err := FindPKColumnNames(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, _table.SourceName)
	if err != nil {
		return nil, err
	}

	// 判断主键列是有在不迁移打字段中, 如果主键列是否在需要迁移打列中
	// 否则继续查找 唯一键
	if len(pkColumnNames) >= 0 {
		logger.M.Infof("成功. 获取到所有的主键列 %v.%v %v", _table.SourceSchema, _table.SourceName, pkColumnNames)
		pkInUsefulColumn := true
		// 判断所有的主键列是否都在需要迁移打列中
		for _, pkColumnName := range pkColumnNames {
			// 该表是否存在这个主键列
			if columnIndex, ok := _table.SourceColumnIndexMap[pkColumnName]; ok {
				if !common.HasElem(_table.SourceUsefulColumns, columnIndex) {
					pkInUsefulColumn = false
					logger.M.Warnf("失败. 检测到主键列没有在需要迁移的列中. %v.%v.%v", _table.SourceSchema, _table.SourceName, pkColumnName)
					break
				}
			} else {
				pkInUsefulColumn = false
				logger.M.Warnf("失败. 检测到主键列没有在该表中. %v.%v.%v", _table.SourceSchema, _table.SourceName, pkColumnName)
				break
			}
		}

		// 主键列都在需要迁移打列中, 直接返回
		if pkInUsefulColumn {
			return pkColumnNames, nil
		}
	}
	logger.M.Warnf("失败, 获取的主键列中有不需要迁移打列, 将获取唯一键来代替主键. %v.%v", _table.SourceSchema, _table.SourceName)

	// 获取唯一键名称. 注意: 该名称不是列名.
	uniqueNames, err := FindUniqueNames(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, _table.SourceName)
	if err != nil {
		return nil, err
	}

	// 该表没有唯一键则返回错误, 因为迁移必须要有唯一, 或主键
	if len(uniqueNames) == 0 || uniqueNames == nil {
		return nil, fmt.Errorf("失败. 该表没有主键和可以用的唯一键. %v.%v %v:%v", _table.SourceSchema, _table.SourceName, _configMap.Source.Host.String, _configMap.Source.Port.Int64)
	}

	// 获取能用的唯一键列名称, 并且可用的唯一键就是主键
	for _, uniqueName := range uniqueNames {
		uniqueColumnNames, err := FindUniqueColumnNames(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, _table.SourceName, uniqueName)
		if err != nil {
			return nil, err
		}

		// 判断所有的唯一键列是否都在需要迁移的列中
		uniqueInUsefulColumn := true
		for _, uniqueColumnName := range uniqueColumnNames {
			// 该唯一键列是否存在表打列中
			if columnIndex, ok := _table.SourceColumnIndexMap[uniqueColumnName]; ok {
				if !common.HasElem(_table.SourceUsefulColumns, columnIndex) {
					uniqueInUsefulColumn = false
					logger.M.Warnf("失败. 检测到主键列没有在需要迁移的列中. 唯一键名称: %v. %v.%v.%v", uniqueName, _table.SourceSchema, _table.SourceName, uniqueColumnName)
					break
				}
			} else {
				uniqueInUsefulColumn = false
				logger.M.Warnf("失败. 通过唯一键列名, 没有匹配到表相关的列. 唯一键名称: %v. %v.%v.%v", uniqueName, _table.SourceSchema, _table.SourceName, uniqueColumnName)
				break
			}

		}

		// 唯一键列都在需要迁移的列中, 直接返回
		if uniqueInUsefulColumn {
			return uniqueColumnNames, nil
		}
	}

	return nil, fmt.Errorf("失败. 该表没有主键和可以用的唯一键. %v.%v %v:%v", _table.SourceSchema, _table.SourceName, _configMap.Source.Host.String, _configMap.Source.Port.Int64)
}

/* 获取指定表的主键名称
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名称
*/
func FindPKColumnNames(host string, port int, schemaName string, tableName string) ([]string, error) {

	pkColumnNames := make([]string, 0, 1)

	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表主键. %v.%v", host, port, schemaName, tableName)
	}

	selectSql := `
        SELECT
            S.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        LEFT JOIN INFORMATION_SCHEMA.STATISTICS AS S
            ON TC.TABLE_SCHEMA = S.INDEX_SCHEMA
            AND TC.TABLE_NAME = S.TABLE_NAME
            AND TC.CONSTRAINT_NAME = S.INDEX_NAME
        WHERE TC.TABLE_SCHEMA = ?
            AND TC.TABLE_NAME = ?
            AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY';
    `

	// 查询数据库
	rows, err := instance.Query(selectSql, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表主键列名. %v.%v %v:%v. %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 循环创建 column
	for rows.Next() {
		var columnName sql.NullString

		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scan 获取指定表的主键名称. %v.%v. %v", schemaName, tableName, err)
		}

		pkColumnNames = append(pkColumnNames, columnName.String)
	}

	return pkColumnNames, nil
}

/* 获取指定表的所有唯一键名称
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名称
*/
func FindUniqueNames(host string, port int, schemaName string, tableName string) ([]string, error) {

	uniqueNames := make([]string, 0, 1)

	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表唯一键名称. %v.%v", host, port, schemaName, tableName)
	}

	selectSql := `
        SELECT
            CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
            AND CONSTRAINT_TYPE = 'UNIQUE'
    `

	// 查询数据库
	rows, err := instance.Query(selectSql, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表主键列名. %v.%v %v:%v. %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 循环创建 column
	for rows.Next() {
		var uniqueName sql.NullString

		if err := rows.Scan(&uniqueName); err != nil {
			return nil, fmt.Errorf("scan 获取指定表的所有唯一键名称. %v.%v. %v", schemaName, tableName, err)
		}

		uniqueNames = append(uniqueNames, uniqueName.String)
	}

	return uniqueNames, nil

}

/* 获取指定表的所有唯一键列名
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名称
    _uniqueName: 唯一键名称
*/
func FindUniqueColumnNames(host string, port int, schemaName string, tableName string, uniqueName string) ([]string, error) {

	uniqueColumnNames := make([]string, 0, 1)

	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表唯一键列名. 唯一键名称: %v. %v.%v", host, port, uniqueName, schemaName, tableName)
	}

	selectSql := `
        SELECT
            COLUMN_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
            AND INDEX_NAME = ?
    `

	// 查询数据库
	rows, err := instance.Query(selectSql, schemaName, tableName, uniqueName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表唯一键列名. %v.%v %v:%v %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 循环创建 唯一键列名
	for rows.Next() {
		var columnName sql.NullString

		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scan 获取表唯一键列名. 唯一键名: %v. %v.%v %v", uniqueName, schemaName, tableName, err)
		}

		uniqueColumnNames = append(uniqueColumnNames, columnName.String)
	}

	return uniqueColumnNames, nil

}

/* 获取表的所有的唯一键包含的列, 不重复, 包括的主键列 */
func FindSourceDistinctUKColumnNames(host string, port int, schemaName string, tableName string) ([]string, error) {
	distinctUK := make([]string, 0, 1)

	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return nil, fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表所有的唯一键列名(包括主键). %v.%v", host, port, schemaName, tableName)
	}

	selectSql := `
		SELECT 
		    DISTINCT S.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
		LEFT JOIN INFORMATION_SCHEMA.STATISTICS AS S
		    ON TC.TABLE_SCHEMA = S.INDEX_SCHEMA
		    AND TC.TABLE_NAME = S.TABLE_NAME
		    AND TC.CONSTRAINT_NAME = S.INDEX_NAME 
		WHERE TC.TABLE_SCHEMA = ?
		    AND TC.TABLE_NAME = ?
		    AND TC.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE');
    `

	// 查询数据库
	rows, err := instance.Query(selectSql, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("失败. 获取表所有的唯一键列名(包括主键). %v.%v %v:%v %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 循环创建 唯一键列名
	for rows.Next() {
		var columnName sql.NullString

		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("scan 获取表所有的唯一键列名(包括主键) %v.%v %v", schemaName, tableName, err)
		}

		distinctUK = append(distinctUK, columnName.String)
	}

	return distinctUK, nil
}

/* 获得创建目标表语句
如果该表有不需要迁移的列:
    1. 现在源实例库中创建一个匿名表 _dbus_xxx_c 的表
    2. 对该表进行删除字段
    3. show create table schema._dbus_xxx_c 获取建表语句
    4. 处理 建表sql生成目标需要的建表sql

如果是全字段迁移直接获取表结构, 并且处理字符串就好

Params:
    _configMap: 元信息配置文件
    _table: 需要迁移的表
*/
func GetTargetCreateTableSql(_configMap *config.ConfigMap, _table *Table) (string, error) {
	var createTableSql string

	// 判断是否有不需要迁移的字段
	if _table.SourceIgnoreColumns != nil && len(_table.SourceIgnoreColumns) > 0 {
		// 获取临时匿名表
		anonymousTableName := common.GetAnonymousTableName(_table.SourceName)

		// 先清除存在一样的匿名表
		err := DropTable(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, anonymousTableName)
		if err != nil {
			return "", nil
		}
		logger.M.Infof("成功. 在创建匿名表之前先清除匿名表. %v.%v (%v)", _table.SourceSchema, anonymousTableName, _table.SourceName)

		// 从源表中创建匿名表
		err = CreateTableFromTable(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, _table.SourceName, anonymousTableName)
		if err != nil {
			return "", nil
		}
		logger.M.Infof("成功. 创建匿名表. %v.%v (%v)", _table.SourceSchema, anonymousTableName, _table.SourceName)

		// 获取所有需要删除的列名, 并删表除列
		ignoreColumnNames := _table.FindSourceIgnoreNames()
		err = DropTableColumnAndIndex(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, anonymousTableName, ignoreColumnNames, nil)
		if err != nil {
			return "", nil
		}

		// 获取匿名表的建表语句
		createTableSql, err = GetCreateTableSql(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, anonymousTableName)
		if err != nil {
			return "", err
		}
	} else { // 全字段迁移, 直接获取源表建表sql
		var err error
		createTableSql, err = GetCreateTableSql(_configMap.Source.Host.String, int(_configMap.Source.Port.Int64), _table.SourceSchema, _table.SourceName)
		if err != nil {
			return "", err
		}
	}

	// 通过源表建表sql, 转换称目标表sql
	/* 1. 将建表语句按行分割,
	   CREATE TABLE `store` (
	     `store_id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
	     `manager_staff_id` tinyint(3) unsigned NOT NULL,
	     `address_id` smallint(5) unsigned NOT NULL,
	     `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	     PRIMARY KEY (`store_id`),
	     UNIQUE KEY `idx_unique_manager` (`manager_staff_id`),
	     KEY `idx_fk_address_id` (`address_id`)
	   ) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8
	*/
	createSqlLines := strings.Split(createTableSql, "\n")

	// 2. 获得没有第一行 (create table xxx.xxx) 的sql
	createSqlBody := strings.Join(createSqlLines[1:], "\n")

	// 3. 循环每个需要迁移的字段并且并且替换相关字段名称
	for _, usefulColumnIndex := range _table.SourceUsefulColumns {
		usefulColumnName := _table.SourceColumns[usefulColumnIndex].Name
		targetColumnName := _table.SourceToTargetColumnNameMap[usefulColumnName]

		usefulColumnName = common.GetBackquote(usefulColumnName)
		targetColumnName = common.GetBackquote(targetColumnName)

		// 将所有的源字段替换为目标字段
		createSqlBody = strings.Replace(createSqlBody, usefulColumnName, targetColumnName, -1)
	}

	// 4. 将第一行 (CREATE TABLE `store` () 丢弃, 并且替换称目标表
	firstLine := fmt.Sprintf("/* go-d-bus */ CREATE TABLE IF NOT EXISTS `%v`.`%v` (\n", _table.TargetSchema, _table.TargetName)

	// 获得最终目标的 建表 SQL
	targetCreateTableSql := fmt.Sprintf("%v%v", firstLine, createSqlBody)

	return targetCreateTableSql, nil
}

/* 获得创建目标表语句
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名称
*/
func GetCreateTableSql(host string, port int, schemaName string, tableName string) (string, error) {
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return "", fmt.Errorf("缓存中不存在该实例(%v:%v). 获取表创建sql. %v.%v", host, port, schemaName, tableName)
	}

	selectSql := fmt.Sprintf("/* go-d-bus */ SHOW CREATE TABLE `%v`.`%v`", schemaName, tableName)

	// 查询数据库
	rows, err := instance.Query(selectSql)
	if err != nil {
		return "", fmt.Errorf("失败. 获取表创建sql %v.%v %v:%v %v: %v", schemaName, tableName, host, port, err, selectSql)
	}
	defer rows.Close()

	// 获取创建语句
	var rsTableName sql.NullString
	var createTableSql sql.NullString
	for rows.Next() {
		rows.Scan(&rsTableName, &createTableSql)
	}

	if !createTableSql.Valid || createTableSql.String == "" {
		return "", fmt.Errorf("失败. sql执行成功, 但是没有获取到建表sql. %v.%v %v:%v %v. %v", schemaName, tableName, host, port, err, selectSql)
	}

	return createTableSql.String, nil
}

/* 清除存在的匿名表
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名称
*/
func DropTable(host string, port int, schemaName string, tableName string) error {
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("缓存中不存在该实例(%v:%v). 删除匿名表sql. %v.%v", host, port, schemaName, tableName)
	}

	dropSql := fmt.Sprintf("/* go-d-bus */ DROP TABLE IF EXISTS `%v`.`%v`", schemaName, tableName)

	// 查询数据库
	_, err := instance.Exec(dropSql)
	if err != nil {
		return fmt.Errorf("失败. 清理匿名表. %v.%v %v:%v. %v. %v", schemaName, tableName, host, port, err, dropSql)
	}

	return nil
}

/* 获得创建目标表语句
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _fromName: 以这个表为准
    _toName: 最终生成的表名
*/
func CreateTableFromTable(host string, port int, schemaName string, fromName string, toName string) error {
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("缓存中不存在该实例(%v:%v). 创建表. %v.%v -> %v %v:%v", schemaName, fromName, toName, host, port)
	}

	createSql := fmt.Sprintf("/* go-d-bus */ CREATE TABLE IF NOT EXISTS `%v`.`%v` LIKE `%v`.`%v`", schemaName, toName, schemaName, fromName)

	// 查询数据库
	_, err := instance.Exec(createSql)
	if err != nil {
		return fmt.Errorf("失败. 创建表. %v.%v -> %v %v:%v. %v. %v", schemaName, fromName, toName, host, port, err, createSql)
	}

	return nil
}

/* 获得创建目标表语句
Params:
    _host: 实例host
    _port: 实例port
    _schemaName: 数据库名称
    _tableName: 表名
    _dropColumnNames: 需要删除的字段名称
    _dropIndexNames: 需要删除的所有名称
*/
func DropTableColumnAndIndex(host string, port int, schemaName string, tableName string, dropColumnNames []string, dropIndexNames []string) error {
	// 获取 ALTER TBALE DROP 语句
	alterDropSql := common.CreateDropColumnSql(schemaName, tableName, dropColumnNames, dropIndexNames)

	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("缓存中不存在该实例(%v:%v). 删除表的字段和索引. %v.%v %v:%v. %v", schemaName, tableName, host, port, alterDropSql)
	}

	// 查询数据库
	_, err := instance.Exec(alterDropSql)
	if err != nil {
		return fmt.Errorf("失败. 执行删除表字段和索引. %v.%v %v:%v. %v %v", schemaName, tableName, host, port, err, alterDropSql)
	}

	return nil
}

// 获取需要迁移的表 map
func FindAllMigrationTableNameMap() map[string]*MigrationTableName {
	migrationTableNameMap := make(map[string]*MigrationTableName)

	migrationTableMap.Range(func(_tableNameInterface, _tableInterface interface{}) bool {
		table := _tableInterface.(interface{}).(*Table)
		tableName := _tableNameInterface.(interface{}).(string)

		migrationTableName := NewMigrationTableName(table.SourceSchema,
			table.SourceName, table.TargetSchema, table.TargetName)

		migrationTableNameMap[tableName] = migrationTableName

		return true
	})

	return migrationTableNameMap
}

// 记录所有需要迁移的表
func ShowAllMigrationTableNames() {
	migrationTableMap.Range(func(_tableNameInterface, _tableInterface interface{}) bool {
		table := _tableInterface.(interface{}).(*Table)
		logger.M.Infof("需要迁移的表: `%v`.`%v` -> `%v`.`%v`", table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName)
		return true
	})
}

// 显示不满足迁移的表
func ShowAllIgnoreMigrationTableNames(_configMap *config.ConfigMap) {
	migrationTableNameMap := FindAllMigrationTableNameMap()

	for configTableName, configTable := range _configMap.TableMapMap {
		if _, ok := migrationTableNameMap[configTableName]; !ok {
			targetSchema := _configMap.SchemaMapMap[configTable.Schema.String].Target.String
			logger.M.Warnf("不满足迁移条件, 被忽略的表: `%v`.`%v` -> `%v`.`%v`",
				configTable.Schema.String, configTable.Source.String, targetSchema, configTable.Target.String)
		}
	}
}
