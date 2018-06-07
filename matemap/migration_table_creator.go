package matemap

import (
    "github.com/daiguadaidai/go-d-bus/config"
    "sync"
    "github.com/daiguadaidai/go-d-bus/gdbc"
    "database/sql"
    "fmt"
    "github.com/juju/errors"
    "github.com/ngaut/log"
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
        errMsg := fmt.Sprintf("在迁移表Map中没有获取到需要迁移的表. table: %v", _key)
        return nil, errors.New(errMsg)
    }

    table := tableInterface.(interface{}).(*Table)

    return table, nil
}

/* 通过配置文件创建所有需要迁移的表, 信息
Params:
    _configMap: 需要迁移的表的映射配置信息
 */
func InitMigrationTableMap(_configMap *config.ConfigMap) error {

    for key, tableMap := range _configMap.TableMapMap {
        migrationTable, err := NewTable(_configMap, tableMap.Schema.String, tableMap.Source.String)
        if err != nil {
            return err
        }
        if migrationTable == nil {
            log.Warnf(
                "失败. 在实例中没有查找到表, 将忽略该表的迁移. %v.%v. %v:%v",
                tableMap.Schema.String,
                tableMap.Source.String,
                _configMap.Source.Host.String,
                _configMap.Source.Port.Int64,
            )
            continue
        }

        migrationTableMap.Store(key, migrationTable)

        log.Infof("成功. 初始化迁移表元信息 %v.%v-> %v.%v\n", migrationTable.SourceSchema, migrationTable.SourceName,
            migrationTable.TargetSchema, migrationTable.TargetName)
    }

    return nil
}

/* 创建一个新的需要迁移的数据库信息

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
    sourceColumns, err := GetSourceTableColumns(
        _schemaName,
        _tableName,
        _configMap.Source.Host.String,
        int(_configMap.Source.Port.Int64),
    )
    if err != nil {
        return nil, err
    }
    if len(sourceColumns) == 0 || sourceColumns == nil {
        log.Warnf("失败. 没有查寻到该表的字段信息, %v.%v\n, %v:%v",
            _schemaName, _tableName, _configMap.Source.Host.String, int(_configMap.Source.Port.Int64))
        return nil, nil
    }
    table.SourceColumns = sourceColumns
    log.Infof("成功. 获取所有的(源)字段, %v.%v\n", _schemaName, _tableName)

    // 通过 源 columns 生成目标 columns, 只要 sourceColumns 有值, targetColumns 一定有值
    table.TargetColumns = GetTargetTableColumnBySourceColumns(
        _configMap, _schemaName, _tableName, sourceColumns)
    log.Infof("成功. 生成(目标)字段, 通过源字段, %v.%v\n", _schemaName, _tableName)

    // 初始化列的名相关映射信息
    err = table.InitColumnMapInfo()
    if err != nil {
        return nil, err
    }
    log.Infof("成功. 生成 源和目标 字段相关映射信息. %v.%v <-> %v.%v",
        table.SourceSchema, table.SourceName, table.TargetSchema, table.TargetName)

    // 添加不进行迁移的列
    ignoreColumnNames := _configMap.GetIgnoreColumnsBySchemaAndTable(_schemaName, _tableName)
    table.SetSourceIgnoreColumns(ignoreColumnNames)
    log.Infof("成功. 设置表不需要迁移的字段. %v.%v: %v",
        table.SourceSchema, table.SourceName, ignoreColumnNames)

    // 生成 最终需要使用到的 列, 一个表有多个列, 但是同步时可能, 只需要同步其中几个列就好了.
    table.InitSourceUsefulColumns()
    log.Infof("成功. 生成需要迁移的字段. %v.%v", table.SourceSchema, table.SourceName)


    return table, nil
}

/* 获取源数据库表的所有列
Params:
    _schemaName: 数据库名称
    _tableName: 表名字
    _host: 实例 host
    _port: 实例 port
 */
func GetSourceTableColumns(_schemaName string, _tableName string, _host string, _port int) ([]Column, error) {
    columns := make([]Column, 0, 10)

    // 从数据库中获取表的所有列
    select_sql := `
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
    log.Infof("获取表 %v.%v 所有的字段.\n", _schemaName, _tableName)

    // 获取数据库实例链接
    instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
    if err !=nil {
        return nil, err
    }

    // 查询数据库
    rows, err := instance.DB.Query(select_sql, _schemaName, _tableName)
    if err != nil {
        errMSG := fmt.Sprintf("失败. 获取表所有字段. %v.%v. %v: \n%v\n",
            _schemaName, _tableName, err, select_sql)
        return nil, errors.New(errMSG)
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

        rows.Scan(&tableSchema, &tableName, &columnName, &ordinalPosition, &columnType, &extra)

        column := CreateColumn(columnName.String, columnType.String, extra.String, int(ordinalPosition.Int64))

        columns = append(columns, column)
        log.Infof("成功. 添加表字段 %v.%v.%v\n", _schemaName, _tableName, columnName.String)
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
    _tableName string, _sourceColumns []Column) ([]Column) {

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
            log.Infof(
                "成功. 发现(源)和(目标)字段有映射信息, 修改目标字段名. %v.%v.%v -> %v.%v.%v",
                _schemaName, _tableName, column.Name,
                _configMap.SchemaMapMap[config.GetSchemaKey(_schemaName)].Target.String,
                _configMap.TableMapMap[config.GetTableKey(_schemaName, _tableName)].Target.String,
                columnMap.Target.String,
            )
        }
    }

    return targetColumns
}


