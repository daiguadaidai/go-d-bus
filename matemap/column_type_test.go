package matemap

import (
	"testing"
	"fmt"
)

func TestColumnType_PrintType(t *testing.T) {

	// 数字类型
	fmt.Println(TYPE_BIT)
	fmt.Println(TYPE_TINYINT)
	fmt.Println(TYPE_SMALLINT)
	fmt.Println(TYPE_MEDIUMINT)
	fmt.Println(TYPE_INT)
	fmt.Println(TYPE_BIGINT)
	fmt.Println(TYPE_DECIMAL)
	fmt.Println(TYPE_FLOAT)
	fmt.Println(TYPE_DOUBLE)

	// 字符串类型
	fmt.Println(TYPE_CHAR)
	fmt.Println(TYPE_VARCHAR)
	fmt.Println(TYPE_BINARY)
	fmt.Println(TYPE_VARBINARY)
	fmt.Println(TYPE_ENUM)
	fmt.Println(TYPE_SET)
	fmt.Println(TYPE_TINYBLOB)
	fmt.Println(TYPE_BLOB)
	fmt.Println(TYPE_MEDIUMBLOB)
	fmt.Println(TYPE_LONGBLOB)
	fmt.Println(TYPE_TINYTEXT)
	fmt.Println(TYPE_TEXT)
	fmt.Println(TYPE_MEDIUMTEXT)
	fmt.Println(TYPE_LONGTEXT)

	// 日期类型
	fmt.Println(TYPE_DATE)
	fmt.Println(TYPE_TIME)
	fmt.Println(TYPE_DATETIME)
	fmt.Println(TYPE_TIMESTAMP)
	fmt.Println(TYPE_YEAR)

	// json 类型
	fmt.Println(TYPE_JSON)

	// 地理位置类型
	fmt.Println(TYPE_GEOMETRY)
	fmt.Println(TYPE_POINT)
	fmt.Println(TYPE_LINESTRING)
	fmt.Println(TYPE_POLYGON)
	fmt.Println(TYPE_GEOMETRYCOLLECTION)
	fmt.Println(TYPE_MULTIPOINT)
	fmt.Println(TYPE_MULTILINESTRING)
	fmt.Println(TYPE_MULTIPOLYGON)
}
