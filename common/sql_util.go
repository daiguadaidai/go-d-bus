package common

import (
	"database/sql"
	"fmt"
	"github.com/juju/errors"
	"strconv"
	"strings"
	"time"
)

const (
	// 数字类型
	MYSQL_TYPE_BIT       = iota + 1 // bit
	MYSQL_TYPE_TINYINT              // tinyint
	MYSQL_TYPE_SMALLINT             // smallint
	MYSQL_TYPE_MEDIUMINT            // mediumint
	MYSQL_TYPE_INT                  // int
	MYSQL_TYPE_BIGINT               // bigint
	MYSQL_TYPE_DECIMAL              // decimal
	MYSQL_TYPE_FLOAT                // float
	MYSQL_TYPE_DOUBLE               // double

	// 字符串类型
	MYSQL_TYPE_CHAR       // char
	MYSQL_TYPE_VARCHAR    // varchar
	MYSQL_TYPE_BINARY     // binary
	MYSQL_TYPE_VARBINARY  // varbinary
	MYSQL_TYPE_ENUM       // enum
	MYSQL_TYPE_SET        // set
	MYSQL_TYPE_TINYBLOB   // tinyblob
	MYSQL_TYPE_BLOB       // blob
	MYSQL_TYPE_MEDIUMBLOB // mediumblob
	MYSQL_TYPE_LONGBLOB   // longblob
	MYSQL_TYPE_TINYTEXT   // tinytext
	MYSQL_TYPE_TEXT       // text
	MYSQL_TYPE_MEDIUMTEXT // mediumtext
	MYSQL_TYPE_LONGTEXT   // longtext

	// 日期类型
	MYSQL_TYPE_DATE      // date
	MYSQL_TYPE_TIME      // time
	MYSQL_TYPE_DATETIME  // datetime
	MYSQL_TYPE_TIMESTAMP // timestamp
	MYSQL_TYPE_YEAR      // year

	// json 类型
	MYSQL_TYPE_JSON // json

	// 地理位置类型
	MYSQL_TYPE_GEOMETRY           // geometry
	MYSQL_TYPE_POINT              // point
	MYSQL_TYPE_LINESTRING         // linestring
	MYSQL_TYPE_POLYGON            // polygon
	MYSQL_TYPE_GEOMETRYCOLLECTION // geometrycollection
	MYSQL_TYPE_MULTIPOINT         // multipoint
	MYSQL_TYPE_MULTILINESTRING    // multilinestring
	MYSQL_TYPE_MULTIPOLYGON       // multipolygon
)

/* 获取匿名的表名
Params:
    _tableName: 原来的表名
*/
func GetAnonymousTableName(_tableName string) string {
	anonymousName := fmt.Sprintf("_dbus_%v_c", _tableName)

	if len(anonymousName) > 64 {
		t := time.Now()
		anonymousName = fmt.Sprintf("_dbus_%v_c", t.Format("20060102150405123456"))
	}

	return anonymousName
}

/* 创建 Alter Drop column 和 index 语句
Pramas:
    _schemaName: 需要 DDL 的 数据库
    _tableName: 需要 DDL 的表
    _dropColumnNames: 需要 drop 的字段名
    _dropIndexNames: 需要 drop 的索引名称
最后生成:
ALTER TABLE `schema`.`table`
    DROP COLUMN `aa`,
    DROP COLUMN `bb`,
    DROP INDEX `idx_1`,
    DROP INDEX `idx_2`
*/
func CreateDropColumnSql(_schemaName string, _tableName string,
	_dropColumnNames []string, _dropIndexNames []string) string {

	alterSqlLines := make([]string, 0, 1)

	// 添加 DROP COLUMN 字符串
	for _, dropColumnName := range _dropColumnNames {
		dropColumnLine := fmt.Sprintf("    DROP COLUMN `%v`", dropColumnName)
		alterSqlLines = append(alterSqlLines, dropColumnLine)
	}

	// 添加 DROP INDEX 字符串
	for _, dropIndexName := range _dropIndexNames {
		dropIndexLine := fmt.Sprintf("    DROP INDEX `%v`", dropIndexName)
		alterSqlLines = append(alterSqlLines, dropIndexLine)
	}

	if len(alterSqlLines) == 0 {
		return ""
	}

	// 生成 DDL 字段和索引 字符串
	sqlBody := strings.Join(alterSqlLines, ",\n")

	// 添加 ALTER TABLE `schema`.`table` 字符串
	alterTitle := fmt.Sprintf("ALTER TABLE `%v`.`%v`\n", _schemaName, _tableName)

	return fmt.Sprintf("%v%v", alterTitle, sqlBody)
}

/* 将字符串格式化称反引号的模式  aaa -> `aaa`
Pramas:
    str: 序号格式化的的字符串
*/
func GetBackquote(str string) string {
	return fmt.Sprintf("`%v`", str)
}

/* 通过多个列名, 创建SELECT 中带反引号的列字符串
[a, b, c] -> `a`, `b`, `c`
Params:
    _columnNames: 列名
*/
func FormatColumnNameStr(_columnNames []string) string {
	return fmt.Sprintf("`%v`", strings.Join(_columnNames, "`, `"))
}

/* 通过列名格式化 ORDER BY 字句字段
[a, b, c] -> `a` ASC, `b` ASC, `c` ASC
[a, b, c] -> `a` DESC, `b` DESC, `c` DESC
Pramas:
    _columnNames: 列名
    _ascDesc: 升序还是降序  ASC/DESC
*/
func FormatOrderByStr(_columnNames []string, _ascDesc string) string {
	orderByColumns := make([]string, 0, len(_columnNames))

	for _, columName := range _columnNames {
		orderByColumn := fmt.Sprintf("`%v` %v", columName, _ascDesc)
		orderByColumns = append(orderByColumns, orderByColumn)
	}

	return strings.Join(orderByColumns, ", ")
}

/* 格式化带反引号的表名
schema table -> `schema`.`table`
Pramas:
    _schemaName: 数据库
    _tableName: 表名
*/
func FormatTableName(_schemaName string, _tableName string, _warpStr string) string {
	return fmt.Sprintf("%v%v%v.%v%v%v",
		_warpStr, _schemaName, _warpStr, _warpStr, _tableName, _warpStr)
}

/* 格式化WHERE字句
c1 c2 -> `c1` >= ? AND `c2` >= ?
Pramas:
    _columnNames: 字段名
    _oprator: 表名
*/
func FormatWhereStr(_columnNames []string, _oprator string) string {
	whereColumns := make([]string, 0, len(_columnNames))

	for _, columName := range _columnNames {
		whereColumn := fmt.Sprintf("`%v` %v ?", columName, _oprator)
		whereColumns = append(whereColumns, whereColumn)
	}

	return strings.Join(whereColumns, " AND ")
}

/* 格式化 update set 字句
c1, c2 -> `c1` = ?, `c2` = ?
Pramas:
    _columnNames: 字段名
*/
func FormatSetStr(_columnNames []string) string {
	setColumns := make([]string, 0, len(_columnNames))

	for _, columName := range _columnNames {
		setColumn := fmt.Sprintf("`%v` = ?", columName)
		setColumns = append(setColumns, setColumn)
	}

	return strings.Join(setColumns, ", ")
}

/* 获取 Where 占位符
?, ?
Params:
    _count: 占位符个数
*/
func CreatePlaceholderByCount(_count int) string {
	placeholders := make([]string, 0, _count)

	for i := 0; i < _count; i++ {
		placeholders = append(placeholders, "?")
	}

	return strings.Join(placeholders, ", ")
}

/* 获取 Insert 语句的 在为符
(?, ?, ?), (?, ?, )
Params:
    _columnLenth: 列的个数
    _rowCount: 需要多少行
*/
func FormatValuesPlaceholder(_columnLenth int, _rowCount int) string {
	valueRows := make([]string, _rowCount)

	for i := 0; i < _rowCount; i++ {
		valueRow := fmt.Sprintf("(%v)", CreatePlaceholderByCount(_columnLenth))
		valueRows[i] = valueRow
	}

	return strings.Join(valueRows, ", ")
}

func Row2Map(_row *sql.Row, _columnNames []string, _columnTypes []int) (map[string]interface{}, error) {
	rowMap := make(map[string]interface{})
	columnLen := len(_columnTypes)

	values := make([]interface{}, columnLen)   // 数据库原生二进制值
	scanArgs := make([]interface{}, columnLen) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	err := _row.Scan(scanArgs...)
	if err != nil {
		errMSG := fmt.Sprintf("%v: scan 字段数据错误 %v", CurrLine(), err)
		return nil, errors.New(errMSG)
	}

	// 开始生成字段数据
	// 真的是让人摸不着头脑. 有时候 values 是 []int8, 有时候是其他基本类型,如 int, string.
	// 同样是从数据库中查询出来的. 为啥会这样
	for i, value := range values {
		columnData, err := GetColumnData(value, _columnTypes[i])
		if err != nil {
			errMSG := fmt.Sprintf("%v: 转化字段数据出错 column Name: %v. column type: %v. %v",
				CurrLine(), _columnNames[i], _columnTypes[i], err)
			return nil, errors.New(errMSG)
		}
		rowMap[_columnNames[i]] = columnData
	}

	return rowMap, nil
}

/* 获取数据库字段数据
Params:
    _value: 查询出来的原始值
    _columnType: 在数据库中的字段类型
*/
func GetColumnData(_value interface{}, _columnType int) (interface{}, error) {
	if _value == nil {
		return nil, nil
	}

	var strData string
	switch _value.(type) {
	case []int8:
		strData = string(_value.([]uint8))
	case string:
		strData = _value.(string)
	case int:
		strData = fmt.Sprintf("%v", _value.(int))
	case int8:
		strData = fmt.Sprintf("%v", _value.(int8))
	case int16:
		strData = fmt.Sprintf("%v", _value.(int16))
	case int32:
		strData = fmt.Sprintf("%v", _value.(int32))
	case int64:
		strData = fmt.Sprintf("%v", _value.(int64))
	case float64:
		strData = strconv.FormatFloat(_value.(float64), 'E', -1, 64)
	}

	return String2GoValueBySqlType(strData, _columnType)
}

/* 将字符串转化成相应的类型值
Params:
    _value: 字符串的值
    _sqlType: sql的类型
*/
func String2GoValueBySqlType(_value string, _sqlType int) (interface{}, error) {
	switch _sqlType {
	case MYSQL_TYPE_BIT, MYSQL_TYPE_TINYINT, MYSQL_TYPE_SMALLINT, MYSQL_TYPE_MEDIUMINT,
		MYSQL_TYPE_INT, MYSQL_TYPE_BIGINT, MYSQL_TYPE_YEAR:

		data, err := strconv.Atoi(_value)
		if err != nil {
			return nil, err
		}
		return data, nil

	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_CHAR, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_BINARY,
		MYSQL_TYPE_VARBINARY, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINYBLOB,
		MYSQL_TYPE_BLOB, MYSQL_TYPE_MEDIUMBLOB, MYSQL_TYPE_LONGBLOB, MYSQL_TYPE_TINYTEXT,
		MYSQL_TYPE_TEXT, MYSQL_TYPE_MEDIUMTEXT, MYSQL_TYPE_LONGTEXT, MYSQL_TYPE_DATE,
		MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_JSON:

		return _value, nil

	case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:

		return _value, nil
	}

	errMSG := fmt.Sprintf("%v: 失败. 转化数据库字段信息出错遇到未知类型", CurrLine())
	return -1, errors.New(errMSG)
}

/* 将sql类型转化成Golang类型
Params:
    _sqlType: sql对应的类型
*/
func SqlType2GoType(_sqlType int) (int, error) {
	switch _sqlType {
	case MYSQL_TYPE_BIT, MYSQL_TYPE_TINYINT, MYSQL_TYPE_SMALLINT, MYSQL_TYPE_MEDIUMINT,
		MYSQL_TYPE_INT, MYSQL_TYPE_BIGINT, MYSQL_TYPE_YEAR:

		return GO_TYPE_INT, nil

	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_CHAR, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_BINARY,
		MYSQL_TYPE_VARBINARY, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINYBLOB,
		MYSQL_TYPE_BLOB, MYSQL_TYPE_MEDIUMBLOB, MYSQL_TYPE_LONGBLOB, MYSQL_TYPE_TINYTEXT,
		MYSQL_TYPE_TEXT, MYSQL_TYPE_MEDIUMTEXT, MYSQL_TYPE_LONGTEXT, MYSQL_TYPE_DATE,
		MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_JSON:

		return GO_TYPE_STRING, nil

	case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:

		return GO_TYPE_STRING, nil
	}

	errMSG := fmt.Sprintf("%v: 失败. 转化数据库字段信息出错遇到未知类型", CurrLine())
	return -1, errors.New(errMSG)
}
