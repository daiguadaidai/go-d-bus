package matemap

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/daiguadaidai/go-d-bus/common"
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

	SourceIgnoreColumns []int    // 不进行同步的列
	SourceUsefulColumns []int    // 最终进行语句平凑操作的列, 最终使用的列

	Create_table_sql         string // 创建表sql语句的模板
	Drop_table_sql           string // 删除表语句sql
	Sel_first_pk_sql         string // 查询第一条sql  主键/唯一键 值 sql
	Sel_per_batch_max_pk_sql string // 每批查询表最大 主键/唯一键 值 sql
	Sel_per_batch_sql        string // 每批查询获取数据的sql, row copy 所用
	Ins_igr_batch_sql        string // insert ignore into 批量 sql
	Rep_per_batch_sql        string // replace into 批量 insert 数据
	Upd_sql                  string // update sql
	Del_sql                  string // delete sql
}

// 初始化 源 列映射关系, 通过源列
func (this *Table) initSourceColumnIndexMap() error {
	if this.SourceColumns == nil || len(this.SourceColumns) == 0 {
		errMSG := fmt.Sprintf("失败, 初始化 源 列名和位置信息. 该表没有列(源) %v.%v",
			this.SourceSchema, this.SourceName)
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
		errMSG := fmt.Sprintf("失败, 初始化 失败 列名和位置信息. 该表没有列(目标) %v.%v",
			this.SourceSchema, this.SourceName)
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
		this.SourceToTargetColumnNameMap[targetColumn.Name] = this.SourceColumns[i].Name
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
		fmt.Println("------------        ---------", len(this.SourceColumns), len(this.SourceIgnoreColumns))
		this.SourceUsefulColumns = make([]int, 0, len(this.SourceColumns) - len(this.SourceIgnoreColumns))
	}

	for columnIndex, _ := range this.SourceColumns {
        if common.HasElem(this.SourceIgnoreColumns, columnIndex) { // 该字段索引是不需要迁移的
        	continue
		}

		this.SourceUsefulColumns = append(this.SourceUsefulColumns, columnIndex)
	}
}


