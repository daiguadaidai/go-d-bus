package matemap

import "strings"

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
		column.Type = TYPE_BIT
	} else if strings.HasPrefix(_columnType, "tinyint") {
		column.Type = TYPE_TINYINT
	} else if strings.HasPrefix(_columnType, "smallint") {
		column.Type = TYPE_SMALLINT
	} else if strings.HasPrefix(_columnType, "mediumint") {
		column.Type = TYPE_MEDIUMINT
	} else if strings.HasPrefix(_columnType, "int") {
		column.Type = TYPE_INT
	} else if strings.HasPrefix(_columnType, "decimal") {
		column.Type = TYPE_DECIMAL
	} else if strings.HasPrefix(_columnType, "float") {
		column.Type = TYPE_FLOAT
	} else if strings.HasPrefix(_columnType, "double") {
		column.Type = TYPE_DOUBLE
	} else if strings.HasPrefix(_columnType, "enum") { // 获取枚举类型和值
		column.Type = TYPE_ENUM

		// 获取值 enum('a', 'b', 'c') -> [a, b, c]
		column.EnumValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(_columnType, "enum("), ")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(_columnType, "set") { // 获取集合类型 和 值
		column.Type = TYPE_SET

		// 获取值 set('a', 'b', 'c') -> [a, b, c]
		column.SetValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(_columnType, "set("), ")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(_columnType, "date") {
		column.Type = TYPE_DATE
	} else if strings.HasPrefix(_columnType, "time") {
		column.Type = TYPE_TIME
	} else if strings.HasPrefix(_columnType, "timestamp") {
		column.Type = TYPE_TIMESTAMP
	} else if strings.HasPrefix(_columnType, "datetime") {
		column.Type = TYPE_DATETIME
	} else if strings.HasPrefix(_columnType, "year") {
		column.Type = TYPE_YEAR
	} else if strings.HasPrefix(_columnType, "char") {
		column.Type = TYPE_CHAR
	} else if strings.HasPrefix(_columnType, "varchar") {
		column.Type = TYPE_VARCHAR
	} else if strings.HasPrefix(_columnType, "binary") {
		column.Type = TYPE_BINARY
	} else if strings.HasPrefix(_columnType, "varbinary") {
		column.Type = TYPE_VARBINARY
	} else if strings.HasPrefix(_columnType, "tinyblob") {
		column.Type = TYPE_TINYBLOB
	} else if strings.HasPrefix(_columnType, "blob") {
		column.Type = TYPE_BLOB
	} else if strings.HasPrefix(_columnType, "mediumblob") {
		column.Type = TYPE_MEDIUMBLOB
	} else if strings.HasPrefix(_columnType, "longblob") {
		column.Type = TYPE_LONGBLOB
	} else if strings.HasPrefix(_columnType, "tinytext") {
		column.Type = TYPE_TINYTEXT
	} else if strings.HasPrefix(_columnType, "text") {
		column.Type = TYPE_TEXT
	} else if strings.HasPrefix(_columnType, "mediumtext") {
		column.Type = TYPE_MEDIUMTEXT
	} else if strings.HasPrefix(_columnType, "longtext") {
		column.Type = TYPE_LONGTEXT
	} else if strings.HasPrefix(_columnType, "json") {
		column.Type = TYPE_JSON
	} else if strings.HasPrefix(_columnType, "geometry") {
		column.Type = TYPE_GEOMETRY
	} else if strings.HasPrefix(_columnType, "point") {
		column.Type = TYPE_POINT
	} else if strings.HasPrefix(_columnType, "linestring") {
		column.Type = TYPE_LINESTRING
	} else if strings.HasPrefix(_columnType, "polygon") {
		column.Type = TYPE_POLYGON
	} else if strings.HasPrefix(_columnType, "geometrycollection") {
		column.Type = TYPE_GEOMETRYCOLLECTION
	} else if strings.HasPrefix(_columnType, "multipoint") {
		column.Type = TYPE_MULTIPOINT
	} else if strings.HasPrefix(_columnType, "multilinestring") {
		column.Type = TYPE_MULTILINESTRING
	} else if strings.HasPrefix(_columnType, "multipolygon") {
		column.Type = TYPE_MULTIPOLYGON
	}

	return column
}
