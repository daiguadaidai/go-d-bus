package common

import (
	"fmt"
	"time"
	"strings"
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
func FormatTableName(_schemaName string, _tableName string) string {
    return fmt.Sprintf("`%v`.`%v`", _schemaName, _tableName)
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
