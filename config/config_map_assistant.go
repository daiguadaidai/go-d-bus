package config

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "fmt"
)

// 创建 映射信息的 schema映射信息的 Map, 源 schema 的名字为 map 的 key
func MakeSchemaMapMap(_schemaMaps []model.SchemaMap) map[string]model.SchemaMap {
    schemaMapMap := make(map[string]model.SchemaMap)

    for _, schemaMap := range _schemaMaps {
        // key: schema
        key := fmt.Sprintf("%v", schemaMap.Source.String)
        schemaMapMap[key] = schemaMap
    }

    return schemaMapMap
}

// 创建 映射信息的 table 映射信息的 Map, Map 的key为源端的: schema.table
func MakeTableMapMap(_tableMaps []model.TableMap) map[string]model.TableMap {
    tableMapMap := make(map[string]model.TableMap)

    for _, tableMap := range _tableMaps {
        // key: schema.table
        key := fmt.Sprintf("%v.%v", tableMap.Schema.String, tableMap.Source.String)
        tableMapMap[key] = tableMap
    }

    return tableMapMap
}

// 创建 映射信息的 table 映射信息的 Map, Map 的key为源端的: schema.table.column
func MakeColumnMapMap(_columnMaps []model.ColumnMap) map[string]model.ColumnMap {
    columnMapMap := make(map[string]model.ColumnMap)

    for _, columnMap := range _columnMaps {
        // key: schema.table
        key := fmt.Sprintf("%v.%v.%v",
            columnMap.Schema.String,
            columnMap.Table.String,
            columnMap.Source.String)
        columnMapMap[key] = columnMap
    }

    return columnMapMap
}

/* 获取数据库map的key
Params:
    _schema: 数据库名
*/
func GetSchemaKey(_schema string) string {
    return fmt.Sprintf("%v", _schema)
}

/* 获取表map的key
Params:
    _schema: 数据库名
    _table: 表明
*/
func GetTableKey(_schema string, _table string) string {
    return fmt.Sprintf("%v.%v", _schema, _table)
}

/* 获取字段map的key
Params:
    _schema: 数据库名
    _table: 表明
    _column: 字段名
 */
func GetColumnKey(_schema string, _table string, _column string) string {
    return fmt.Sprintf("%v.%v.%v", _schema, _table, _column)
}
