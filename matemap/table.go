package matemap

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/logger"
	"strings"
)

type Table struct {
	SourceSchema                           string         // 源 数据库名
	SourceName                             string         // 源 表名
	SourceColumns                          []Column       // 源 所有的列
	SourcePKColumns                        []int          // 源 主键, 没有主键的话就用第一个唯一键
	SourceAllUKColumns                     []int          // 原 所有的唯一键字段, 最终不重复
	SourceColumnIndexMap                   map[string]int // 列名和 sourceColumns index 的映射, key:列名, value: 列所在的位置
	SourceBinlogDeleteWhereExternalColumns []int          // 消费Binlog Delete Where 条件额外需要的字段 所在位置

	BinlogDeleteWhereExternalColumns []Column // 消费Binlog Delete Where 条件额外需要的字段, 字段名称是目标表的字段名

	TargetSchema                           string         // 目标 数据库名
	TargetName                             string         // 目标 表名
	TargetColumns                          []Column       // 目标所有的列
	TargetPKColumns                        []int          // 目标 主键, 没有主键就使用第一个唯一键
	TargetAllUKColumns                     []int          // 目标 所有的唯一键字段, 最终不重复
	TargetColumnIndexMap                   map[string]int // 列名和 TargetColumn index 的映射
	TargetBinlogDeleteWhereExternalColumns []int          // mubio 消费Binlog Delete Where 条件额外需要的字段 所在位置

	SourceToTargetColumnNameMap map[string]string // 源端列明映射到目标端的列明
	TargetToSourceColumnNameMap map[string]string // 目标端列明映射到源端的列明

	SourceIgnoreColumns []int // 不进行同步的列
	SourceUsefulColumns []int // 最终进行语句平凑操作的列, 最终使用的列

	targetCreateTableSql      string // 创建目标表sql语句的 sql
	targetDropTableSql        string // 删除目标表语句 sql
	selFirstPKSqlTpl          string // 查询第一条记录  主键/唯一键 值 sql 模板
	selLastPKSqlTpl           string // 查询最后一条记录  主键/唯一键 值 sql 模板
	selPerBatchMaxPKSqlTpl    string // 每批查询表最大 主键/唯一键 值 sql 模板
	selCurrAndNextPkSqlTpl    string // 获取当前和下一批主键值sql模板
	selPerBatchSqlTpl         string // 每批查询获取数据的sql, row copy 所用 sql 模板
	insIgrBatchSqlTpl         string // insert ignore into 批量 sql 模板
	repPerBatchSqlTpl         string // replace into 批量 insert 数据 sql 模板
	updSqlTpl                 string // update sql 模板
	delSqlTpl                 string // delete sql 模板
	selSourceRowCheckSqlTpl   string // 源实例 单行 checksum sql 模板
	selTargetRowCheckSqlTpl   string // 目标 单行 checksum sql 模板
	selSourceRowsCheckSqlTpl  string // 源实例 多行 checksum sql 模板
	selTargetRowsCheckSqlTpl  string // 目标 多行 checksum sql 模板
	selPerBatchSourcePKSqlTpl string // 源实例每批查询主键值的sql, 用于checksum修复每行数据的时候使用
	selSourceRowSqlTpl        string // 通过主键获取源表一行数据 sql 模板
}

// 初始化 源 列映射关系, 通过源列
func (this *Table) initSourceColumnIndexMap() error {
	if this.SourceColumns == nil || len(this.SourceColumns) == 0 {
		return fmt.Errorf("失败, 初始化 源 列名和位置信息. 该表没有列(源) %v.%v", this.SourceSchema, this.SourceName)
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
		return fmt.Errorf("失败, 初始化 失败 列名和位置信息. 该表没有列(目标) %v.%v", this.SourceSchema, this.SourceName)
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
		return fmt.Errorf("失败. 初始化 源到目标 列名的映射关系, 该表没有列(源) %v.%v", this.SourceSchema, this.SourceName)
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
		return fmt.Errorf("失败. 初始化 源到目标 列名的映射关系, 该表没有列(目标) %v.%v", this.SourceSchema, this.SourceName)
	}

	this.TargetToSourceColumnNameMap = make(map[string]string)

	for i, targetColumn := range this.TargetColumns {
		this.TargetToSourceColumnNameMap[targetColumn.Name] = this.SourceColumns[i].Name
	}

	return nil
}

/* 初始化表所有的唯一键字段(包括主键), 通过给定的字段名
Params:
    _uKColulmnNames: 指定的字段名
*/
func (this *Table) InitSourceAllUKColumnsByNames(_uKColumnNames []string) error {
	if len(_uKColumnNames) < 1 {
		return fmt.Errorf("初始化源表所有的唯一键字段失败, 没有指定唯一键. 这种情况, 可能是你的源表没有唯一键. 这不符合工具使用的要求. 请检查 %v:%v", this.SourceSchema, this.SourceName)
	}

	this.SourceAllUKColumns = make([]int, len(_uKColumnNames))

	for i, columnName := range _uKColumnNames {
		this.SourceAllUKColumns[i] = this.SourceColumnIndexMap[columnName]
	}

	return nil
}

func (this *Table) InitTargetAllUKColumnsBySourceUKNames(_sourceUKColumnNames []string) error {
	if len(_sourceUKColumnNames) < 1 {
		return fmt.Errorf("初始化目标表所有的唯一键字段失败, 没有指定唯一键. 这种情况, 可能是你的源表没有唯一键.这不符合工具使用的要求. 请检查 %v:%v", this.SourceSchema, this.SourceName)
	}

	this.TargetAllUKColumns = make([]int, len(_sourceUKColumnNames))

	// 通过源表字段名找到映射的目标表字段, 从而获得目标目标的 唯一键index
	for i, sourceColumnName := range _sourceUKColumnNames {
		// 获取目标列名
		targetColumnName := this.SourceToTargetColumnNameMap[sourceColumnName]
		// 获取目标列名对应的index
		this.TargetAllUKColumns[i] = this.TargetColumnIndexMap[targetColumnName]
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
func (this *Table) InitSourcePKColumns(pkColumnNames []string) {
	if this.SourcePKColumns == nil {
		this.SourcePKColumns = make([]int, 0, len(pkColumnNames))
	}

	for _, pkColumnName := range pkColumnNames {
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

	// 初始化 获取主键, 本次和下一次值
	this.InitSelCurrAndNextPKSqlTpl()

	// 每批查询获取数据的sql, row copy 所用 sql 模板
	this.InitSelPerBatchSqlTpl()

	// 初始化 源表每批主键sql模板
	this.InitSelPerBatchSourcePKSqlTpl()

	// 初始化 insert ignore into 批量 sql 模板
	this.InitInsIgrBatchSqlTpl()

	// 初始化 replace into 批量 insert 数据 sql 模板
	this.InitRepPerBatchSqlTpl()

	// update sql 模板
	this.InitUpdSqlTpl()

	// delete sql 模板
	this.InitDelSqlTpl()

	// 初始化源 单行 checksum sql 模板
	this.InitSelSourceRowChecksumSqlTpl()

	// 初始化目标 单行 checksum sql 模板
	this.InitSelTargetRowChecksumSqlTpl()

	// 初始化源 多行 checksum sql 模板
	this.InitSelSourceRowsChecksumSqlTpl()

	// 初始化目标 多行 checksum sql 模板
	this.InitSelTargetRowsChecksumSqlTpl()

	// 初始化目标 多行 checksum sql 模板
	this.InitSelSourceRowSqlTpl()
}

/* 初始化目标键表语句
Pramas:
    _targetCreateTableSql: 目标建表sql语句
*/
func (this *Table) InitTargetCreateTableSql(targetCreateTableSql string) {
	this.targetCreateTableSql = targetCreateTableSql
}

// 初始化删除目标表语句 sql
func (this *Table) InitTargetDropTableSql() {
	this.targetDropTableSql = fmt.Sprintf("/* go-d-bus */DROP TABLE IF EXISTS `%v`.`%v`",
		this.TargetSchema, this.TargetName)
}

// 初始化查询第一条sql  主键/唯一键 值 sql 模板
func (this *Table) InitSelFirstPKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        ORDER BY %v
        LIMIT 1
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键列组成的字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取升序的 ORDER BY 字句
	orderByStr := common.FormatOrderByStr(pkColumnNames, "ASC")

	this.selFirstPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, orderByStr)
}

// 初始化查询最后一条sql  主键/唯一键 值 sql 模板
func (this *Table) InitSelLastPKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        ORDER BY %v
        LIMIT 1
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键列组成的字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取升序的 ORDER BY 字句
	orderByStr := common.FormatOrderByStr(pkColumnNames, "DESC")

	this.selLastPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, orderByStr)
}

// 初始化 每批查询表最大 主键/唯一键 值 sql 模板
func (this *Table) InitSelPerBatchMaxPKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM (
            SELECT %v
            FROM %v 
            WHERE (%v) >= (%v)
            ORDER BY %v
            LIMIT 0, %v
        ) AS tmp
        ORDER BY %v
        LIMIT 1
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键列组成的字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))
	// 获取升序的 ORDER BY 字句
	orderByAscStr := common.FormatOrderByStr(pkColumnNames, "ASC")
	// limit 字句中的 offset值
	limitOffsetValue := "%v"
	// 获取降序的 ORDER BY 字句
	orderByDescStr := common.FormatOrderByStr(pkColumnNames, "DESC")

	this.selPerBatchMaxPKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, fieldsStr,
		tableName, pkFieldsStr, wherePlaceholderStr, orderByAscStr, limitOffsetValue, orderByDescStr)
}

// 初始化 每批查询表最大 主键/唯一键 值 sql 模板
func (this *Table) InitSelCurrAndNextPKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        WHERE (%v) >= (%v)
        ORDER BY %v
        LIMIT %v, 2
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selCurrAndNextPkSqlTpl = fmt.Sprintf(selectSql, pkFieldsStr, tableName, pkFieldsStr, wherePlaceholderStr, pkFieldsStr, "%v")
}

// 每批查询获取数据的sql, row copy 所用 sql 模板
func (this *Table) InitSelPerBatchSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        WHERE (%v) >= (%v)
            AND (%v) <= (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selPerBatchSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
		wherePlaceholderStr, pkFieldsStr, wherePlaceholderStr)
}

// 每批查询获取主键值的sql 模板
func (this *Table) InitSelPerBatchSourcePKSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        WHERE (%v) >= (%v)
            AND (%v) <= (%v)
    `

	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selPerBatchSourcePKSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
		wherePlaceholderStr, pkFieldsStr, wherePlaceholderStr)
}

// 初始化 insert ignore into 批量 sql 模板
func (this *Table) InitInsIgrBatchSqlTpl() {
	insIgrSql := `/* go-d-bus */ INSERT LOW_PRIORITY IGNORE INTO %v(%v) VALUES %v`

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取需要迁移的字段名称
	targetUsefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取目标所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(targetUsefulColumnNames, "`, `")
	// values 之后的值, 这个值主要后面需要变成占位符, 所以先使用 %v 代替
	valuesStr := "%v"

	this.insIgrBatchSqlTpl = fmt.Sprintf(insIgrSql, tableName, fieldsStr, valuesStr)
}

// 初始化 replace into 批量 insert 数据 sql 模板
func (this *Table) InitRepPerBatchSqlTpl() {
	replaceSql := `/* go-d-bus */ REPLACE LOW_PRIORITY INTO %v(%v) VALUES %v`

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取需要迁移的字段名称
	targetUsefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取目标所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(targetUsefulColumnNames, "`, `")
	// values 之后的值, 这个值主要后面需要变成占位符, 所以先使用 %v 代替
	valuesStr := "%v"

	this.repPerBatchSqlTpl = fmt.Sprintf(replaceSql, tableName, fieldsStr, valuesStr)
}

// update sql 模板
func (this *Table) InitUpdSqlTpl() {
	updateSql := `
        /* go-d-bus */ UPDATE LOW_PRIORITY %v
        SET %v
        WHERE (%v) = (%v)
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
	pkFieldsStr := common.FormatColumnNameStr(targetPKColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(targetPKColumnNames))

	this.updSqlTpl = fmt.Sprintf(updateSql, tableName, setFieldsStr, pkFieldsStr, wherePlaceholderStr)
}

// delete sql 模板
func (this *Table) InitDelSqlTpl() {
	deleteSql := "/* go-d-bus */ DELETE LOW_PRIORITY FROM %v WHERE (%v) = (%v) %v"

	// 获取 目标表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取目标主键名称
	targetPKColumnNames := this.FindTargetPKColumnNames()

	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(targetPKColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreateDebugPlaceholderByCount(len(targetPKColumnNames))

	// 获取而外的字段条件
	var externalWhere string
	if len(this.BinlogDeleteWhereExternalColumns) > 0 {
		externalWhereStrs := this.GetBinlogDeleteWhereExternalStr()
		externalWhere = fmt.Sprintf("AND %v", strings.Join(externalWhereStrs, " AND "))
	}

	this.delSqlTpl = fmt.Sprintf(deleteSql, tableName, pkFieldsStr, wherePlaceholderStr, externalWhere)
}

/* 初始化源 单行数据 checksum sql 模板
把多个字段拼凑称一个字段并且使用 '#' 井号隔开, 如下显示:
id, name, age
SELECT RCR32(CONCAT(
    `id`, '#', `name`, '#', `age`
))
FROM xxx
WHERE id = xxx
使用 CONCAT 后的值: id#name#age
*/
func (this *Table) InitSelSourceRowChecksumSqlTpl() {
	selectSql := `
        /* go-d-bus checksum row source */ SELECT /*!40001 SQL_NO_CACHE */
        CRC32(CONCAT(
            %v
        ))
        FROM %v
        WHERE (%v) = (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, '#', `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selSourceRowCheckSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
		wherePlaceholderStr)
}

// 初始化源 单行数据 checksum sql 模板
// 处理方式和 InitSourceRowChecksumTpl 一样
func (this *Table) InitSelTargetRowChecksumSqlTpl() {
	selectSql := `
        /* go-d-bus checksum row target */ SELECT /*!40001 SQL_NO_CACHE */
        CRC32(CONCAT(
            %v
        ))
        FROM %v
        WHERE (%v) = (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindTargetPKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, '#', `")
	// 获取 源表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selTargetRowCheckSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
		wherePlaceholderStr)
}

/* 初始化源 多行行数据 checksum sql 模板
把多个字段拼凑称一个字段并且使用 '#' 井号隔开, 如下显示:
id, name, age
SELECT SUM(RCR32(CONCAT(
    id, "#", name, "#", age
)))
FROM xxx
WHERE id = xxx
使用 CONCAT 后的值: id#name#age
*/
func (this *Table) InitSelSourceRowsChecksumSqlTpl() {
	selectSql := `
        /* go-d-bus checksum rows source */ SELECT /*!40001 SQL_NO_CACHE */
        SUM(CRC32(CONCAT(
            %v
        )))
        FROM %v
        WHERE (%v) >= (%v)
            AND (%v) <= (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, '#', `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selSourceRowsCheckSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr,
		wherePlaceholderStr, pkFieldsStr, wherePlaceholderStr)

}

// 初始化源 多行数据 checksum sql 模板
// 处理方式和 InitSourceRowsChecksumTpl 一样
func (this *Table) InitSelTargetRowsChecksumSqlTpl() {
	selectSql := `
        /* go-d-bus checksum row target */ SELECT /*!40001 SQL_NO_CACHE */
        SUM(CRC32(CONCAT(
            %v
        )))
        FROM %v
        WHERE (%v) >= (%v)
            AND (%v) <= (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindTargetUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindTargetPKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, '#', `")
	// 获取 源表名
	tableName := common.FormatTableName(this.TargetSchema, this.TargetName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selTargetRowsCheckSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr, wherePlaceholderStr, pkFieldsStr, wherePlaceholderStr)
}

// 初始化 通过主键值获取源表数据 sql 模板
func (this *Table) InitSelSourceRowSqlTpl() {
	selectSql := `
        /* go-d-bus */ SELECT /*!40001 SQL_NO_CACHE */
            %v
        FROM %v
        WHERE (%v) = (%v)
    `

	// 获取需要迁移的字段名称
	usefulColumnNames := this.FindUsefulColumnNames()
	// 获取主键名称
	pkColumnNames := this.FindSourcePKColumnNames()
	// 获取所有需要迁移的字段 字符串
	fieldsStr := common.FormatColumnNameStr(usefulColumnNames, "`, `")
	// 获取 源表名
	tableName := common.FormatTableName(this.SourceSchema, this.SourceName, "`")
	// 获取 主键字段 字符串
	pkFieldsStr := common.FormatColumnNameStr(pkColumnNames, "`, `")
	// 获取 Where 中需要的值的占位符
	wherePlaceholderStr := common.CreatePlaceholderByCount(len(pkColumnNames))

	this.selSourceRowSqlTpl = fmt.Sprintf(selectSql, fieldsStr, tableName, pkFieldsStr, wherePlaceholderStr)
}

func (this *Table) InitSourceBinlogDeleteWhereExternalColumns() error {
	if len(this.BinlogDeleteWhereExternalColumns) == 0 {
		return nil
	}

	sourceExternalColumns := make([]int, 0, len(this.BinlogDeleteWhereExternalColumns))
	for _, externalColumn := range this.BinlogDeleteWhereExternalColumns {
		// 通过目标表的字段名称获取 原表的字段名称
		sourceExternalColumnName, ok := this.TargetToSourceColumnNameMap[externalColumn.Name]
		if !ok {
			return fmt.Errorf("通过binlog delete where额外字段获取原表字段失败, 目标字段: %v 找不到原表字段名", externalColumn.Name)
		}

		// 通过原表字段找到原表字段位置
		sourceExternalColumnIndex, ok := this.SourceColumnIndexMap[sourceExternalColumnName]
		if !ok {
			return fmt.Errorf("通过binlog delete where额外字段获取原表字段失败,  原表字段: %v 找不到对应位置, 目标字段: %v,", sourceExternalColumnName, externalColumn.Name)
		}

		sourceExternalColumns = append(sourceExternalColumns, sourceExternalColumnIndex)
	}

	this.SourceBinlogDeleteWhereExternalColumns = sourceExternalColumns

	return nil
}

func (this *Table) InitTargetBinlogDeleteWhereExternalColumns() error {
	if len(this.BinlogDeleteWhereExternalColumns) == 0 {
		return nil
	}

	targetExternalColumns := make([]int, 0, len(this.BinlogDeleteWhereExternalColumns))
	for _, externalColumn := range this.BinlogDeleteWhereExternalColumns {
		// 通过目标表字段找到原表字段位置
		targetExternalColumnIndex, ok := this.TargetColumnIndexMap[externalColumn.Name]
		if !ok {
			return fmt.Errorf("通过binlog delete where额外字段获取原表字段失败,  目标字段: %v 找不到对应位置", externalColumn.Name)
		}

		targetExternalColumns = append(targetExternalColumns, targetExternalColumnIndex)
	}

	this.TargetBinlogDeleteWhereExternalColumns = targetExternalColumns

	return nil
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
func (this *Table) GetSelPerBatchMaxPKSqlTpl(maxRows int) string {
	return fmt.Sprintf(this.selPerBatchMaxPKSqlTpl, maxRows)
}

/* 获取每一次当前和下一次的主键值
Params:
    maxRows: 查询的最大行数
*/
func (this *Table) GetSelCurrAndNextPKSqlTpl(maxRows int) string {
	return fmt.Sprintf(this.selCurrAndNextPkSqlTpl, maxRows)
}

// 获取 每一批 select的数据 sql
func (this *Table) GetSelPerBatchSqlTpl() string {
	return this.selPerBatchSqlTpl
}

/*获取 insert ignore sql模板
Params:
    _rowCount: 行数
*/
func (this *Table) GetInsIgrBatchSqlTpl(rowCount int) string {
	valuesPlaceholder := common.FormatValuesPlaceholder(len(this.SourceUsefulColumns), rowCount)

	return fmt.Sprintf(this.insIgrBatchSqlTpl, valuesPlaceholder)
}

func (this *Table) GetInsIgrBatchSqlTpl_V2(rows [][]interface{}) string {
	valuesPlaceholder := common.FormatValuesPlaceholder_V2(rows)

	return fmt.Sprintf(this.insIgrBatchSqlTpl, valuesPlaceholder)
}

func (this *Table) GetInsIgrBatchSqlTpl_V3(rows [][]interface{}) (string, error) {
	valuesPlaceholder, err := common.FormatValuesPlaceholder_V3(rows)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(this.insIgrBatchSqlTpl, valuesPlaceholder), nil
}

/* 获取 replace into sql
Params:
    rowCount: 行数
*/
func (this *Table) GetRepPerBatchSqlTpl(rowCount int) string {
	valuesPlaceholder := common.FormatValuesPlaceholder(len(this.SourceUsefulColumns), rowCount)

	return fmt.Sprintf(this.repPerBatchSqlTpl, valuesPlaceholder)
}

/* 获取 replace into sql
 */
func (this *Table) GetRepPerBatchSqlTpl_V2(rows [][]interface{}) string {
	valuesPlaceholder := common.FormatValuesPlaceholder_V2(rows)

	return fmt.Sprintf(this.repPerBatchSqlTpl, valuesPlaceholder)
}

/* 获取 replace into sql
 */
func (this *Table) GetRepPerBatchSqlTpl_V3(rows [][]interface{}) (string, error) {
	valuesPlaceholder, err := common.FormatValuesPlaceholder_V3(rows)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(this.repPerBatchSqlTpl, valuesPlaceholder), nil
}

//  获取 update 语句
func (this *Table) GetUpdSqlTpl() string {
	return this.updSqlTpl
}

// 获取 delete sql 语句
func (this *Table) GetDelSqlTpl(row []interface{}) string {
	return fmt.Sprintf(this.delSqlTpl, row...)
}

// 获取源实例表 单行checksum语句
func (this *Table) GetSelSourceRowChecksumSqlTpl() string {
	return this.selSourceRowCheckSqlTpl
}

// 获取目标实例表 单行checksum语句
func (this *Table) GetSelTargetRowChecksumSqlTpl() string {
	return this.selTargetRowCheckSqlTpl
}

// 获取源实例表 多行checksum语句
func (this *Table) GetSelSourceRowsChecksumSqlTpl() string {
	return this.selSourceRowsCheckSqlTpl
}

// 获取目标实例表 多行checksum语句
func (this *Table) GetSelTargetRowsChecksumSqlTpl() string {
	return this.selTargetRowsCheckSqlTpl
}

// 获取源实例表 主键 范围所有值的sql
func (this *Table) GetSelPerBatchSourcePKSqlTpl() string {
	return this.selPerBatchSourcePKSqlTpl
}

// 获取源实例表 主键 范围所有值的sql
func (this *Table) GetSelSourceRowSqlTpl() string {
	return this.selSourceRowSqlTpl
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
			logger.M.Warnf("获取源表主键对应的Golang类型, MySQL类型 -> Golang类型出错. %v.%v: %v(%v). %v",
				this.SourceSchema, this.SourceName, this.SourceColumns[columnIndex].Name, this.SourceColumns[columnIndex].Type, err)
		}
		pkColumnsTypeMap[this.SourceColumns[columnIndex].Name] = goType
	}

	return pkColumnsTypeMap
}

func (this *Table) GetBinlogDeleteWhereExternalStr() []string {
	whereStrs := make([]string, 0, len(this.BinlogDeleteWhereExternalColumns))

	for _, column := range this.BinlogDeleteWhereExternalColumns {
		whereStr := fmt.Sprintf("`%v` = %v", column.Name, "%#v")

		whereStrs = append(whereStrs, whereStr)
	}

	return whereStrs
}
