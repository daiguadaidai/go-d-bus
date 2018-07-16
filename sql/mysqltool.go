package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLTool struct {
	DB *sql.DB
}

func (this *MySQLTool) Open(_host string, _port int, _username string, _password string, _database string) error {
	dataSource := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8&allowOldPasswords=1",
		_username, _password, _host, _port, _database)

	var err error
	this.DB, err = sql.Open("mysql", dataSource)
	if err != nil {
		return err
	}

	return nil
}

// 关闭数据库链接
func (this *MySQLTool) Close() {
	this.DB.Close()
}

// 获取一行数据
func (this *MySQLTool) FetchOneMap(_sql string) (map[string]interface{}, error) {
	rowMap := make(map[string]interface{})

	rows, err := this.execute(_sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 创建值, 用于赋值
	values := make([]sql.RawBytes, len(columns)) // 数据库原生二进制值
	scanArgs := make([]interface{}, len(values)) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 获取字段类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		for i, value := range values {
			data, _ := this.RawBytes2Value(value, colTypes[i].ScanType().Kind())
			rowMap[colTypes[i].Name()] = data
		}

		break
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rowMap, nil
}

// 获取一行数据
func (this *MySQLTool) FetchOneSlice(_sql string) ([]interface{}, error) {
	rows, err := this.execute(_sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 创建值, 用于赋值
	values := make([]sql.RawBytes, len(columns)) // 数据库原生二进制值
	scanArgs := make([]interface{}, len(values)) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	rowSlice := make([]interface{}, len(values)) // 保存真是类型数据的值
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 获取字段类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		for i, value := range values {
			rowSlice[i], _ = this.RawBytes2Value(value, colTypes[i].ScanType().Kind())
		}

		break
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rowSlice, nil
}

// 获取多行数据
func (this *MySQLTool) FetchAllMap(_sql string) ([]map[string]interface{}, error) {
	rowsMap := make([]map[string]interface{}, 10)

	rows, err := this.execute(_sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 创建值, 用于赋值
	columnSize := len(columns)                  // 字段个数
	values := make([]sql.RawBytes, columnSize)  // 数据库原生二进制值
	scanArgs := make([]interface{}, columnSize) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 获取字段类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Fetch rows
	rowCount := 0 // 获取的行数
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, value := range values {
			data, _ := this.RawBytes2Value(value, colTypes[i].ScanType().Kind())
			rowMap[colTypes[i].Name()] = data
		}
		rowsMap = append(rowsMap, rowMap)
		rowCount++
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rowsMap[len(rowsMap)-rowCount:], nil
}

// 获取多行数据
func (this *MySQLTool) FetchAllSlice(_sql string) ([][]interface{}, error) {
	rowsSlice := make([][]interface{}, 10)

	rows, err := this.execute(_sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 创建值, 用于赋值
	columnSize := len(columns)
	values := make([]sql.RawBytes, columnSize)  // 数据库原生二进制值
	scanArgs := make([]interface{}, columnSize) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 获取字段类型
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Fetch rows
	rowCount := 0 // 用于计算有多少行
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		rowSlice := make([]interface{}, columnSize)
		for i, value := range values {
			rowSlice[i], _ = this.RawBytes2Value(value, colTypes[i].ScanType().Kind())
		}
		rowsSlice = append(rowsSlice, rowSlice)
		rowCount++
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return rowsSlice[len(rowsSlice)-rowCount:], nil
}

/* 执行insert语句传入的值比较完整
Args
    _database: 数据库
    _table: 表
    _columnNames: 字段名
    _values: insert的值Map数组
*/
func (this *MySQLTool) ExecuteInsertMap(_database string, _table string, _columnNames []string,
	_values []map[string]interface{}, _hintTag string) (sql.Result, error) {

	// 组装insert 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ INSERT INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnName := range _columnNames {
			columnValues[columnValuesIndex] = row[columnName]
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行insert通过给定的多行
// _values 是一个存放slice的slice
func (this *MySQLTool) ExecuteInsertSlice(_database string, _table string, _columnNames []string,
	_values [][]interface{}, _hintTag string) (sql.Result, error) {

	// 组装insert 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ INSERT INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnValue := range row {
			columnValues[columnValuesIndex] = columnValue
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行replace into sql map 版本
func (this *MySQLTool) ExecuteReplaceMap(_database string, _table string, _columnNames []string,
	_values []map[string]interface{}, _hintTag string) (sql.Result, error) {

	// 组装replace into 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ REPLACE INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnName := range _columnNames {
			columnValues[columnValuesIndex] = row[columnName]
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行replace into sql slice 版
func (this *MySQLTool) ExecuteReplaceSlice(_database string, _table string, _columnNames []string,
	_values [][]interface{}, _hintTag string) (sql.Result, error) {

	// 组装replace into 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ REPLACE INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnValue := range row {
			columnValues[columnValuesIndex] = columnValue
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行insert ignore into sql map 版本
func (this *MySQLTool) ExecuteInsertIgnoreMap(_database string, _table string, _columnNames []string,
	_values []map[string]interface{}, _hintTag string) (sql.Result, error) {

	// 组装replace into 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ INSERT IGNORE INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnName := range _columnNames {
			columnValues[columnValuesIndex] = row[columnName]
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行replace into sql slice 版
func (this *MySQLTool) ExecuteInsertIgnoreSlice(_database string, _table string, _columnNames []string,
	_values [][]interface{}, _hintTag string) (sql.Result, error) {

	// 组装replace into 表名
	tableName := this.getTableName(_database, _table)

	// 获取字段名组合的字符串: col1`, `col2`, `col3`
	insertColumnNames := strings.Join(_columnNames, "`, `")

	// 获取INSERT SQL前半部分: INSERT INTO db.table(col1, col2, col3) Values
	sqlInsert := fmt.Sprintf(
		"/* %v */ INSERT IGNORE INTO %v(`%v`) VALUES",
		_hintTag,
		tableName,
		insertColumnNames)

	// 获取一行的占位符 (?, ?, ?)
	placeHolder := this.getPlaceHolder(len(_columnNames)) // 获取一行占位符

	// 获取多行的值: (1, 2), (3, 4)
	columnValueSize := len(_columnNames) * len(_values)
	columnValuesIndex := 0
	var columnValues = make([]interface{}, columnValueSize)
	var placeHolders []string // 获取多行的占位符: (?, ?), (?, ?)
	for _, row := range _values {
		// 获取多行的占位符: (?, ?), (?, ?)
		placeHolders = append(placeHolders, placeHolder)

		// 获取多行的值: (1, 2), (3, 4)
		for _, columnValue := range row {
			columnValues[columnValuesIndex] = columnValue
			columnValuesIndex++
		}
	}

	// 最后组装出带占位符的isnert sql
	sqlInsert = sqlInsert + strings.Join(placeHolders, ",")

	ret, err := this.ExecuteDMLPlaceholder(sqlInsert, columnValues[:len(_columnNames)*len(_values)])
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 执行DML, 其中传入的sql是有带占位符的
func (this *MySQLTool) ExecuteDMLPlaceholder(_sql string, _values []interface{}) (sql.Result, error) {
	// 准备sql
	stmt, err := this.DB.Prepare(_sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	ret, err := stmt.Exec(_values...)
	if err != nil {
		return nil, err
	}

	return ret, err
}

// 直接执行DML sql
func (this *MySQLTool) ExecuteDML(_sql string) (sql.Result, error) {
	ret, err := this.DB.Exec(_sql)
	if err != nil {
		return nil, err
	}

	return ret, err
}

func (this *MySQLTool) ExecuteDDL(_sql string) {

}

func (this *MySQLTool) RawBytes2Value(colBytes sql.RawBytes, colType reflect.Kind) (colVal interface{}, fmtStr string) {
	if colBytes == nil {
		return nil, "NULL"
	}

	switch colType {
	case reflect.Int8:
		return RawBytes2Int8(colBytes), "%d"
	case reflect.Int16:
		return RawBytes2Int16(colBytes), "%d"
	case reflect.Int32:
		return RawBytes2Int32(colBytes), "%d"
	case reflect.Int64:
		return RawBytes2Int64(colBytes), "%d"
	case reflect.Uint8:
		return RawBytes2Uint8(colBytes), "%d"
	case reflect.Uint16:
		return RawBytes2Uint16(colBytes), "%d"
	case reflect.Uint32:
		return RawBytes2Uint32(colBytes), "%d"
	case reflect.Uint64:
		return RawBytes2Uint64(colBytes), "%d"
	case reflect.Float32:
		return RawBytes2Float32(colBytes), "%g"
	case reflect.Float64:
		return RawBytes2Float64(colBytes), "%g"
	default:
		return string(colBytes), "%#v"
	}

	return nil, "NULL"
}

// 获取一行的占位符
func (this *MySQLTool) getPlaceHolder(count int) string {
	placeHolder := "("
	for i := 0; i < count; i++ {
		placeHolder += "?, "
	}
	placeHolder = strings.TrimSuffix(placeHolder, ", ") + ")"

	return placeHolder
}

// 获取带 `(小撇)的表名
func (this *MySQLTool) getTableName(_database string, _table string) string {
	tableName := ""
	if strings.Trim(_database, " ") != "" {
		tableName += fmt.Sprintf("`%v`.`%v`", _database, _table)
	} else {
		tableName += fmt.Sprintf("`%v`", _table)
	}

	return tableName
}

func (this *MySQLTool) execute(_sql string) (*sql.Rows, error) {
	// 执行结果
	rows, err := this.DB.Query(_sql)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
