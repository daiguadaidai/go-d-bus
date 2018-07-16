package matemap

import (
	"github.com/daiguadaidai/go-d-bus/common"
	"strings"
)

type Column struct {
	Name            string // 字段名字
	Type            int    // 字段数据类型, 这边是映射成 oita 了
	OrdinalPosition int    // 顺序
	RawType         string // 数据库查询除的原生类型

	IsAuto     bool // 是否自增
	IsUnsigned bool // 是否为正数
	IsZeroFill bool // 是否 以0补齐
	IsEnum     bool // 是否是 枚举
	IsSet      bool // 是否是 集合

	EnumValues []string // 枚举的值
	SetValues  []string // 集合的值
}

/* 创建一个行的列
Params:
    _columnName: 字段名称
    _columnType: 字段类型
    _extra: 额外信息, 一般用于判断字段是否是自增
    _ordinalPosition: 字段在表中的序号, 从 1 开始
Return:
    Column: 一个字段
*/
func CreateColumn(_columnName string, _columnType string, _extra string, _ordinalPosition int) Column {
	column := Column{}

	column.Name = _columnName
	column.RawType = _columnType
	column.OrdinalPosition = _ordinalPosition

	// 判断是否是自增
	if _extra == "auto_increment" {
		column.IsAuto = true
	}

	// 是否是正数
	if strings.Contains(_columnType, "unsigned") {
		column.IsUnsigned = true
	}

	// 是否是 以 0 补齐
	if strings.Contains(_columnType, "zerofill") {
		column.IsUnsigned = true
		column.IsZeroFill = true
	}

	// 判断类型 并 赋值类型
	if strings.HasPrefix(_columnType, "bit") {
		column.Type = common.MYSQL_TYPE_BIT
	} else if strings.HasPrefix(_columnType, "tinyint") {
		column.Type = common.MYSQL_TYPE_TINYINT
	} else if strings.HasPrefix(_columnType, "smallint") {
		column.Type = common.MYSQL_TYPE_SMALLINT
	} else if strings.HasPrefix(_columnType, "mediumint") {
		column.Type = common.MYSQL_TYPE_MEDIUMINT
	} else if strings.HasPrefix(_columnType, "int") {
		column.Type = common.MYSQL_TYPE_INT
	} else if strings.HasPrefix(_columnType, "bigint") {
		column.Type = common.MYSQL_TYPE_BIGINT
	} else if strings.HasPrefix(_columnType, "decimal") {
		column.Type = common.MYSQL_TYPE_DECIMAL
	} else if strings.HasPrefix(_columnType, "float") {
		column.Type = common.MYSQL_TYPE_FLOAT
	} else if strings.HasPrefix(_columnType, "double") {
		column.Type = common.MYSQL_TYPE_DOUBLE
	} else if strings.HasPrefix(_columnType, "enum") { // 获取枚举类型和值
		column.Type = common.MYSQL_TYPE_ENUM

		// 获取值 enum('a', 'b', 'c') -> [a, b, c]
		column.EnumValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(_columnType, "enum("), ")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(_columnType, "set") { // 获取集合类型 和 值
		column.Type = common.MYSQL_TYPE_SET

		// 获取值 set('a', 'b', 'c') -> [a, b, c]
		column.SetValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(_columnType, "set("), ")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(_columnType, "date") {
		column.Type = common.MYSQL_TYPE_DATE
	} else if strings.HasPrefix(_columnType, "time") {
		column.Type = common.MYSQL_TYPE_TIME
	} else if strings.HasPrefix(_columnType, "timestamp") {
		column.Type = common.MYSQL_TYPE_TIMESTAMP
	} else if strings.HasPrefix(_columnType, "datetime") {
		column.Type = common.MYSQL_TYPE_DATETIME
	} else if strings.HasPrefix(_columnType, "year") {
		column.Type = common.MYSQL_TYPE_YEAR
	} else if strings.HasPrefix(_columnType, "char") {
		column.Type = common.MYSQL_TYPE_CHAR
	} else if strings.HasPrefix(_columnType, "varchar") {
		column.Type = common.MYSQL_TYPE_VARCHAR
	} else if strings.HasPrefix(_columnType, "binary") {
		column.Type = common.MYSQL_TYPE_BINARY
	} else if strings.HasPrefix(_columnType, "varbinary") {
		column.Type = common.MYSQL_TYPE_VARBINARY
	} else if strings.HasPrefix(_columnType, "tinyblob") {
		column.Type = common.MYSQL_TYPE_TINYBLOB
	} else if strings.HasPrefix(_columnType, "blob") {
		column.Type = common.MYSQL_TYPE_BLOB
	} else if strings.HasPrefix(_columnType, "mediumblob") {
		column.Type = common.MYSQL_TYPE_MEDIUMBLOB
	} else if strings.HasPrefix(_columnType, "longblob") {
		column.Type = common.MYSQL_TYPE_LONGBLOB
	} else if strings.HasPrefix(_columnType, "tinytext") {
		column.Type = common.MYSQL_TYPE_TINYTEXT
	} else if strings.HasPrefix(_columnType, "text") {
		column.Type = common.MYSQL_TYPE_TEXT
	} else if strings.HasPrefix(_columnType, "mediumtext") {
		column.Type = common.MYSQL_TYPE_MEDIUMTEXT
	} else if strings.HasPrefix(_columnType, "longtext") {
		column.Type = common.MYSQL_TYPE_LONGTEXT
	} else if strings.HasPrefix(_columnType, "json") {
		column.Type = common.MYSQL_TYPE_JSON
	} else if strings.HasPrefix(_columnType, "geometry") {
		column.Type = common.MYSQL_TYPE_GEOMETRY
	} else if strings.HasPrefix(_columnType, "point") {
		column.Type = common.MYSQL_TYPE_POINT
	} else if strings.HasPrefix(_columnType, "linestring") {
		column.Type = common.MYSQL_TYPE_LINESTRING
	} else if strings.HasPrefix(_columnType, "polygon") {
		column.Type = common.MYSQL_TYPE_POLYGON
	} else if strings.HasPrefix(_columnType, "geometrycollection") {
		column.Type = common.MYSQL_TYPE_GEOMETRYCOLLECTION
	} else if strings.HasPrefix(_columnType, "multipoint") {
		column.Type = common.MYSQL_TYPE_MULTIPOINT
	} else if strings.HasPrefix(_columnType, "multilinestring") {
		column.Type = common.MYSQL_TYPE_MULTILINESTRING
	} else if strings.HasPrefix(_columnType, "multipolygon") {
		column.Type = common.MYSQL_TYPE_MULTIPOLYGON
	}

	return column
}
