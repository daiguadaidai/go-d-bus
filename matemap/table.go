package matemap

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/juju/errors"
	"github.com/outbrain/golib/log"
)

type Table struct {
	SourceSchema         string         // 源 数据库名
	SourceName           string         // 源 表名
	SourceColumns        []Column       // 源 所有的列
	SourcePKColumns      []int          // 源 主键, 没有主键的话就用第一个唯一键
	SourceColumnIndexMap map[string]int // 列名和 sourceColumns index 的映射, key:列名, value: 列所在的位置

	TargetSchema         string         // 目标 数据库名
	TargetName           string         // 目标 表名
	TargetColumns        []Column       // 目标所有的列
	TargetPKColumns      []int          // 目标 主键, 没有主键就使用第一个唯一键
	TargetColumnIndexMap map[string]int // 列名和 TargetColumn index 的映射

	SourceToTargetColumnNameMap map[string]string // 源端列明映射到目标端的列明
	TargetToSourceColumnNameMap map[string]string // 目标端列明映射到源端的列明

	SourceIgnoreColumns []int // 不进行同步的列
	SourceUsefulColumns []int // 最终进行语句平凑操作的列, 最终使用的列

	targetCreateTableSql   string // 创建目标表sql语句的 sql
	targetDropTableSql     string // 删除目标表语句 sql
	selFirstPKSqlTpl       string // 查询第一条记录  主键/唯一键 值 sql 模板
	selLastPKSqlTpl        string // 查询最后一条记录  主键/唯一键 值 sql 模板
	selPerBatchMaxPKSqlTpl string // 每批查询表最大 主键/唯一键 值 sql 模板
	selPerBatchSqlTpl      string // 每批查询获取数据的sql, row copy 所用 sql 模板
	insIgrBatchSqlTpl      string // insert ignore into 批量 sql 模板
	repPerBatchSqlTpl      string // replace into 批量 insert 数据 sql 模板
	updSqlTpl              string // update sql 模板
	delSqlTpl              string // delete sql 模板
}

// 初始化 源 列映射关系, 通过源列
func (this *Table) initSourceColumnIndexMap() error {
	if this.SourceColumns == nil || len(this.SourceColumns) == 0 {
		errMSG := fmt.Sprintf("失败, 初始化 源 列名和位置信息. 该表没有列(源) %v.%v %v",
			this.SourceSchema, this.SourceName, common.CurrLine())
		return errors.New(errMSG)
	}

	this.SourceColumnIndexMap = make(map[string]int)

	for i, sourceColumn := range this.SourceColumns {
		this.SourceColumnIndexMap[sourceColumn.Name] = i
	}

	return nil
}

// 初始化 目标 列映射关系, 通过目标列
func (this *Table) initTargetColumnIndexMap() error {
	if this.TargetColumns == nil || len(this.TargetColumns) == 0 {
		errMSG := fmt.Sprintf("失败, 初始化 失败 列名和位置信息. 该表没有列(目标) %v.%v %v",
			this.SourceSchema, this.SourceName, common.CurrLine())
		return errors.New(errMSG)
	}

	this.TargetColumnIndexMap = make(map[string]int)

	for i, targetColumn := range this.TargetColumns {
		this.TargetColumnIndexMap[targetColumn.Name] = i
	}

	return nil
}

// 初始化 源到目标 列名的映射关系,  key:源列名, value:目标列名
func (this *Table) initSourceToTargetColumnNameMap() error {
	if this.SourceColumns == nil || len(this.SourceColumns) == 0 {
		errMSG := fmt.Sprintf("失败. 初始化 源到目标 列名的映射关系, 该表没有列(源) %v.%v",
			this.SourceSchema, this.SourceName)
		return errors.New(errMSG)
	}

	this.SourceToTargetColumnNameMap = make(map[string]string)

	for i, sourceColumn := range this.SourceColumns {
		this.SourceToTargetColumnNameMap[sourceColumn.Name] = this.TargetColumns[i].Name
	}

	return nil
}

// 初始化 目标到源 列名的映射关系, key:目标列名, value:源列名
func (this *Table) initTargetToSourceColumnNameMap() error {
	if this.TargetColumns == nil || len(this.TargetColumns) == 0 {
		errMSG := fmt.Sprintf("失败. 初始化 源到目标 列名的映射关系, 该表没有列(目标) %v.%v",
			this.SourceSchema, this.SourceName)
		return errors.New(errMSG)
	}

	this.TargetToSourceColumnNameMap = make(map[string]string)

	for i, targetColumn := range this.TargetColumns {
		this.TargetToSourceColumnNameMap[targetColumn.Name] = this.SourceColumns[i].Name
	}

	return nil
}

// 初始化所有的列映射信息
func (this *Table) InitColumnMapInfo() error {
	// 初始化 源 列映射关系, 通过源列
	err := this.initSourceColumnIndexMap()
	if err != nil {
		return err
	}

	// 初始化 目标 列映射关系, 通过目标列
	err = this.initTargetColumnIndexMap()
	if err != nil {
		return err
	}

	// 初始化 源到目标 列名的映射关系,  key:源列名, value:目标列名
	err = this.initSourceToTargetColumnNameMap()
	if err != nil {
		return err
	}

	// 初始化 目标到源 列名的映射关系, key:目标列名, value:源列名
	err = this.initTargetToSourceColumnNameMap()
	if err != nil {
		return err
	}

	return nil
}

/* 添加不需要的列
Params:
    _ignoreColumnNames: 所有的列名
*/
func (this *Table) SetSourceIgnoreColumns(_ignoreColumnNames []string) {
	if this.SourceIgnoreColumns == nil {
		this.SourceIgnoreColumns = make([]int, 0, 10)
	}

	for _, ignoreColumnName := range _ignoreColumnNames {
		if columnIndex, ok := this.SourceColumnIndexMap[ignoreColumnName]; ok {
			this.SourceIgnoreColumns = append(this.SourceIgnoreColumns, columnIndex)
		}
	}
}

// 初始化最终使用的字段
func (this *Table) InitSourceUsefulColumns() {
	if this.SourceUsefulColumns == nil {
		this.SourceUsefulColumns = make([]int, 0, len(this.SourceColumns)-len(this.SourceIgnoreColumns))
	}

	for columnIndex, _ := range this.SourceColumns {
		if common.HasElem(this.SourceIgnoreColumns, columnIndex) { // 该字段索引是不需要迁移的
			continue
		}

		this.SourceUsefulColumns = append(this.SourceUsefulColumns, columnIndex)
	}
}

/*初始化可用的唯一键
Params:
    _pkColumns: 可用打 (主键/唯一键) 列名
*/
func (this *Table) InitSourcePKColumns(_pkColumnNames []string) {
	if this.SourcePKColumns == nil {
		this.SourcePKColumns = make([]int, 0, len(_pkColumnNames))
	}

	for _, pkColumnName := range _pkColumnNames {
		sourcePKColumnIndex := this.SourceColumnIndexMap[pkColumnName]
		this.SourcePKColumns = append(this.SourcePKColumns, sourcePKColumnIndex)
	}
}

// 通过源主键列, 初始化目标主键列
func (this *Table) InitTargetPKColumnsFromSource() {
	if this.TargetPKColumns == nil {
		this.TargetPKColumns = make([]int, 0, len(this.SourcePKColumns))
	}

	for _, sourcePKColumnIndex := range this.SourcePKColumns {
		sourcePKColumnName := this.SourceColumns[sourcePKColumnIndex].Name
		targetPkColumnName := this.SourceToTargetColumnNameMap[sourcePKColumnName]
		targetPKColumnIndex := this.TargetColumnIndexMap[targetPkColumnName]
		this.TargetPKColumns = append(this.TargetPKColumns, targetPKColumnIndex)
	}
}

// 获取不要迁移的字段名称
func (this *Table) FindSourceIgnoreNames() []string {
    ignoreColumnNames := make([]string, 0, 1)

    if len(this.SourceIgnoreColumns) == 0 || this.SourceIgnoreColumns == nil {
    	return ignoreColumnNames
	}

	for _, ignoreColumnIndex := range this.SourceIgnoreColumns {
		ignoreColumnNames = append(ignoreColumnNames, this.SourceColumns[ignoreColumnIndex].Name)
	}

	return ignoreColumnNames
}

// 获取 需要迁移的字段名称
func (this *Table) FindUsefulColumnNames() []string {
	sourceUsefulColumNames := make([]string, 0, 1)

	for _, sourceUsefulColumIndex := range this.SourceUsefulColumns {
		sourceUsefulColumNames = append(sourceUsefulColumNames,
			this.SourceColumns[sourceUsefulColumIndex].Name)
	}

	return sourceUsefulColumNames
}

func (this *Table) FindTargetUsefulColumnNames() []string {
    targetUsefulColumnNames := make([]string, 0, 1)

	for _, sourceUsefulColumIndex := range this.SourceUsefulColumns {
		sourceColumnName := this.SourceColumns[sourceUsefulColumIndex].Name
		targetColumnName := this.SourceToTargetColumnNameMap[sourceColumnName]
		targetUsefulColumnNames = append(targetUsefulColumnNames, targetColumnName)
	}

	return targetUsefulColumnNames
}

// 获取 源 主键/唯一键 字段名
func (this *Table) FindSourcePKColumnNames() []string {
    sourcePKColumNames := make([]string, 0, 1)

    for _, sourcePKColumIndex := range this.SourcePKColumns {
        sourcePKColumNames = append(sourcePKColumNames, this.SourceColumns[sourcePKColumIndex].Name)
	}

    return sourcePKColumNames
}

// 获取 目标 主键/唯一键 字段名
func (this *Table) FindTargetPKColumnNames() []string {
	targetPKColumNames := make([]string, 0, 1)

	for _, sourcePKColumIndex := range this.SourcePKColumns {
		sourceColumnName := this.SourceColumns[sourcePKColumIndex].Name
		targetColumnName := this.SourceToTargetColumnNameMap[sourceColumnName]
		targetPKColumNames = append(targetPKColumNames, targetColumnName)
	}

	return targetPKColumNames
}

// 初始化所有的sql语句模板
func (this *Table) InitALLSqlTpl() {
	// 初始化删除目标表语句 sql
	this.InitTargetDropTableSql()

    // 初始化查询第一条记录  主键/唯一键 值 sql 模板
	this.InitSelFirstPKSqlTpl()

	// 初始化查询最后第一条记录  主键/唯一键 值 sql 模板
	this.InitSelLastPKSqlTpl()

	// 初始化 每批查询表最大 主键/唯一键 值 sql 模板
	this.InitSelPerBatchMaxPKSqlTpl()

	// 每批查询获取数据的sql, row copy 所用 sql 模板
	this.InitSelPerBatchSqlTpl()

    // 初始化 insert ignore into 批量 sql 模板
	this.InitInsIgrBatchSqlTpl()

	// 初始化 replace into 批量 insert 数据 sql 模板
	this.InitRepPerBatchSqlTpl()

	// update sql 模板
	this.InitUpdSqlTpl()

	// delete sql 模板
	this.InitDelSqlTpl()
}

/* 初始化目标键表语句
Pramas:
    _targetCreateTableSql: 目标建表sql语句
 */
func (this *Table) InitTargetCreateTableSql(_targetCreateTableSql string) {
    this.targetCreateTableSql = _targetCreateTableSql
}

// 初始化删除目标表语句 sql
func (this *Table) InitTargetDropTableSql() {
	this.targetDropTableSql = fmt.Sprintf("/* go-d-bus */DROP TABLE IF EXISTS `%v`.`%v`",
		this.TargetSchema, this.TargetName)
}

// 初始化查询第一条sql  主键/唯一键 值 sql 模板
func (this *Table) InitSelFirstPKSqlTpl() {
    selectSql := `
        /* go-d-bus */ SELECT %v
        FROM %v
        ORDER BY %v
        LIMIT 1
    `

    // 获取主键名称
    pkColumnNames := this.FindSourcePKColumnNames()
    // 获取 主键列组成的字符串
    fieldsStr := common.FormatColumnNameStr(pkColumnNames)
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
    // 获取升序的 ORDER BY 字句
    orderByStr := common.FormatOrderByStr(pkColumnNames, "ASC")

    this.selFirstPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, orderByStr)
}

// 初始化查询最后一条sql  主键/唯一键 值 sql 模板
func (this *Table) InitSelLastPKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT %v
        FROM %v
        ORDER BY %v
        LIMIT 1
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键列组成的字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames)
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取升序的 ORDER BY 字句
	orderByStr := common.FormatOrderByStr(pkColumnNames, "DESC")

	this.selLastPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, orderByStr)
}

// 初始化 每批查询表最大 主键/唯一键 值 sql 模板
func (this *Table) InitSelPerBatchMaxPKSqlTpl() {
    selectSql := `
        /* go-d-bus */ SELECT %v
        FROM (
            SELECT %v
            FROM %v 
            WHERE %v
            ORDER BY %v
            LIMIT 0, %v
        ) AS tmp
        ORDER BY %v
        LIMIT 1
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键列组成的字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames)
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 WHERE >= 字句
	whereMoreThenStr := common.FormatWhereStr(pkColumnNames, ">=")
	// 获取升序的 ORDER BY 字句
	orderByAscStr := common.FormatOrderByStr(pkColumnNames, "ASC")
	// limit 字句中的 offset值
	limitOffsetValue := "%v"
	// 获取降序的 ORDER BY 字句
	orderByDescStr := common.FormatOrderByStr(pkColumnNames, "DESC")

    this.selPerBatchMaxPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, fieldsStr,
    	tableName, whereMoreThenStr, orderByAscStr, limitOffsetValue, orderByDescStr)
}

// 每批查询获取数据的sql, row copy 所用 sql 模板
func (this *Table) InitSelPerBatchSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT %v
        FROM %v
        WHERE (%v) >= (%v)
            AND (%v) <= (%v)
    `

    // 获取需要迁移的字段名称
    usefulColumnNames := this.FindUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames)
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames)
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

    this.selPerBatchSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
    	wherePlaceholderStr, pkFieldsStr, wherePlaceholderStr)
}

// 初始化 insert ignore into 批量 sql 模板
func (this *Table) InitInsIgrBatchSqlTpl() {
    insIgrSql := `/* go-d-bus */ INSERT IGNORE INTO %v(%v) VALUES %v`

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取需要迁移的字段名称
	targetUsefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取目标所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(targetUsefulColumnNames)
	// values 之后的值, 这个值主要后面需要变成占位符, 所以先使用 %v 代替
    valuesStr := "%v"

	this.insIgrBatchSqlTpl = fmt.Sprintf(insIgrSql, tableName, fieldsStr, valuesStr)
}

// 初始化 replace into 批量 insert 数据 sql 模板
func (this *Table) InitRepPerBatchSqlTpl() {
	replaceSql := `/* go-d-bus */ REPLACE INTO %v(%v) VALUES %v`

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取需要迁移的字段名称
	targetUsefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取目标所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(targetUsefulColumnNames)
	// values 之后的值, 这个值主要后面需要变成占位符, 所以先使用 %v 代替
	valuesStr := "%v"

	this.repPerBatchSqlTpl = fmt.Sprintf(replaceSql, tableName, fieldsStr, valuesStr)
}

// update sql 模板
func (this *Table) InitUpdSqlTpl() {
    updateSql := `
        /* go-d-bus */ UPDATE %v
        SET %v
        WHERE %v
    `

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取需要迁移的字段名称
	targetUsefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取主键名称
	targetPKColumnNames := this.FindTargetPKColumnNames()

	// 获取目标所有需要迁移的字段 字符串
	setFieldsStr := common.FormatSetStr(targetUsefulColumnNames)
	// 获取 主键字段 字符串
	whereStr := common.FormatWhereStr(targetPKColumnNames, "=")

	this.updSqlTpl = fmt.Sprintf(updateSql, tableName, setFieldsStr, whereStr)
}

// delete sql 模板
func (this *Table) InitDelSqlTpl() {
    deleteSql := "/* go-d-bus */ DELETE FROM %v WHERE %v"

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取主键名称
	targetPKColumnNames := this.FindTargetPKColumnNames()

	// 获取 主键字段 字符串
	whereStr := common.FormatWhereStr(targetPKColumnNames, "=")

	this.delSqlTpl = fmt.Sprintf(deleteSql, tableName, whereStr)
}

// 获得创建目标表语句
func (this *Table) GetTargetCreateTableSql() string {
    return this.targetCreateTableSql
}

// 获取删除目标表语句
func (this *Table) GetTargetDropTableSql() string {
    return this.targetDropTableSql
}

// 获取 第一条 主键/唯一键 数据
func (this *Table) GetSelFirstPKSqlTpl() string {
    return this.selFirstPKSqlTpl
}

// 获取 最后一条 主键/唯一键 数据
func (this *Table) GetSelLastPKSqlTpl() string {
    return this.selLastPKSqlTpl
}

/* 获取每一次查询, 最大的 主键/唯一键 数据
Params:
    _maxRows: 查询的最大行数
 */
func (this *Table) GetSelPerBatchMaxPKSqlTpl(_maxRows int) string {
    return fmt.Sprintf(this.selPerBatchMaxPKSqlTpl, _maxRows)
}

// 获取 每一批 select的数据 sql
func (this *Table) GetSelPerBatchSqlTpl() string {
    return this.selPerBatchSqlTpl
}

/*获取 insert ignore sql模板
Params:
    _rowCount: 行数
*/
func (this *Table) GetInsIgrBatchSqlTpl(_rowCount int) string {
    valuesPlaceholder := common.FormatValuesPlaceholder(len(this.SourceUsefulColumns),
    	_rowCount)

    return fmt.Sprintf(this.insIgrBatchSqlTpl, valuesPlaceholder)
}

/* 获取 replace into sql 模板
Params:
    _rowCount: 行数
 */
func (this *Table) GetRepPerBatchSqlTpl(_rowCount int) string {
	valuesPlaceholder := common.FormatValuesPlaceholder(len(this.SourceUsefulColumns),
		_rowCount)

	return fmt.Sprintf(this.repPerBatchSqlTpl, valuesPlaceholder)
}

//  获取 update 语句
func (this *Table) GetUpdSqlTpl() string {
    return this.updSqlTpl
}

// 获取 delete sql 语句
func (this *Table) GetDelSqlTpl() string {
    return this.delSqlTpl
}

// 获取源表主键数据类型
func (this *Table) FindSourcePKColumnTypes() []int {
    pkColumnsTypes := make([]int, len(this.SourcePKColumns))

    for i, columnIndex := range this.SourcePKColumns {
    	pkColumnsTypes[i] = this.SourceColumns[columnIndex].Type
	}

	return pkColumnsTypes
}

/* 获取源表的主键MySQL对应的类型 Map
Return:
{
    "id" : 1,
    "id1": 2,
    "id2": 3,
}
 */
func (this *Table) FindSourcePKColumnTypeMap() map[string]int {
	pkColumnsTypeMap := make(map[string]int)

	for _, columnIndex := range this.SourcePKColumns {
		pkColumnsTypeMap[this.SourceColumns[columnIndex].Name] = this.SourceColumns[columnIndex].Type
	}

	return pkColumnsTypeMap
}

/* 获取源表的主键MySQL对应的Golang类型 Map
Return:
{
    "id" : 1,
    "id1": 2,
    "id2": 3,
}
 */
func (this *Table) FindSourcePKColumnGoTypeMap() map[string]int {
	pkColumnsTypeMap := make(map[string]int)

	for _, columnIndex := range this.SourcePKColumns {
		goType, err := common.SqlType2GoType(this.SourceColumns[columnIndex].Type)
		if err != nil {
			warnMSG := fmt.Sprintf("%v: 获取源表主键对应的Golang类型, " +
				"MySQL类型 -> Golang类型出错. %v.%v: %v(%v). %v",
				common.CurrLine(), this.SourceSchema, this.SourceName,
				this.SourceColumns[columnIndex].Name,
				this.SourceColumns[columnIndex].Type,
				err)
            log.Warningf(warnMSG)
		}
		pkColumnsTypeMap[this.SourceColumns[columnIndex].Name] = goType
	}

	return pkColumnsTypeMap
}
