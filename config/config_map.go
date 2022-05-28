package config

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/daiguadaidai/go-d-bus/model"
)

type ConfigMap struct {
	TaskUUID string

	Source *model.Source // 源数据库
	Target *model.Target // 目标数据库

	SchemaMapMap    map[string]*model.SchemaMap    // 数据库映射信息, key为源数据库名称
	TableMapMap     map[string]*model.TableMap     // 数据库表映射信息, key 为 源数据库的 schema.table
	ColumnMapMap    map[string]*model.ColumnMap    // 数据库字段映射信息, key 为 源数据库的 schema.table.column
	IgnoreColumnMap map[string]*model.IgnoreColumn // 不需要同步的列

	RunQuota *model.Task // 获取运行任务的参数
}

// 判断 是否有需要迁移的任务
func (this *ConfigMap) TaskExists() (bool, error) {
	taskDao := new(dao.TaskDao)

	Task, err := taskDao.GetByTaskUUID(this.TaskUUID, "*")
	if err != nil || Task == nil { // 运行错误
		return false, err
	}
	if Task == nil { // 没有发现数据
		return false, nil
	}

	return true, nil
}

// 判断 是否有需要迁移的数据库
func (this *ConfigMap) SchemaMapExists() bool {
	schemaMapDao := new(dao.SchemaMapDao)

	count := schemaMapDao.Count(this.TaskUUID)
	if count <= 0 {
		return false
	}

	return true
}

// 判断 是否有需要迁移的数据库
func (this *ConfigMap) TableMapExists() bool {
	tableMapDao := new(dao.TableMapDao)

	count := tableMapDao.Count(this.TaskUUID)
	if count <= 0 {
		return false
	}

	return true
}

// 设置默认运行参数
func (this *ConfigMap) InitRunQuota() error {
	taskDao := new(dao.TaskDao)

	task, err := taskDao.GetByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("没有找到相关的默认运行参数. task UUID: %v", this.TaskUUID)
	}

	this.RunQuota = task

	return nil
}

// 设置源实例信息
func (this *ConfigMap) InitSource() error {
	sourceDao := new(dao.SourceDao)

	source, err := sourceDao.GetByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}
	if source == nil {
		return fmt.Errorf("没有找到源实例信息. task UUID: %v", this.TaskUUID)
	}

	srcPassword, err := common.Decrypt(source.Password.String)
	if err != nil {
	} else {
		source.Password.String = srcPassword
	}

	this.Source = source

	return nil
}

// 设置目标实例信息
func (this *ConfigMap) InitTarget() error {
	targetDao := new(dao.TargetDao)

	target, err := targetDao.GetByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("没有找到目标实例信息. task UUID: %v", this.TaskUUID)
	}

	tagetPasword, err := common.Decrypt(target.Password.String)
	if err != nil {
	} else {
		target.Password.String = tagetPasword
	}

	this.Target = target

	return nil
}

// 设置 schema 映射信息
func (this *ConfigMap) InitSchemaMapMap() error {
	schemaMapDao := new(dao.SchemaMapDao)

	schemaMaps, err := schemaMapDao.FindByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}

	this.SchemaMapMap = MakeSchemaMapMap(schemaMaps)

	return nil
}

// 设置 table 映射信息
func (this *ConfigMap) InitTableMapMap() error {
	tableMapDao := new(dao.TableMapDao)

	tableMaps, err := tableMapDao.FindByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}

	this.TableMapMap = MakeTableMapMap(tableMaps)

	return nil
}

// 设置 column 映射信息
func (this *ConfigMap) InitColumnMapMap() error {
	columnMapDao := new(dao.ColumnMapDao)

	columnMaps, err := columnMapDao.FindByTaskUUID(this.TaskUUID, "*")
	if err != nil {
		return err
	}

	this.ColumnMapMap = MakeColumnMapMap(columnMaps)

	return nil
}

// 设置 不需要同步的列
func (this *ConfigMap) InitIgnoreColumnMap() error {
	this.IgnoreColumnMap = make(map[string]*model.IgnoreColumn)

	return nil
}

/* 通过指定的表名, 获取不需要迁移的列名
Params:
    _schemaName: 哪个数据库
    _tableName: 哪个表
*/
func (this *ConfigMap) GetIgnoreColumnsBySchemaAndTable(schemaName string, tableName string) []string {

	ignoreColumnNames := make([]string, 0, 10)

	// 便利所有不需要迁移的列, 并塞选出指定表的列名称
	for _, ignoreColumn := range this.IgnoreColumnMap {
		if ignoreColumn.Schema.String == schemaName && ignoreColumn.Table.String == tableName {
			ignoreColumnNames = append(ignoreColumnNames, ignoreColumn.Name.String)
		}
	}

	return ignoreColumnNames
}

/* 创建映射配置文件
从数据库读取 库, 表, 列等 映射信息
Params:
    _taskUUID: 任务的UUID
*/

func NewConfigMap(_taskUUID string) (*ConfigMap, error) {
	configMap := new(ConfigMap)
	configMap.TaskUUID = _taskUUID

	// 判断任务是否存在
	exists, err := configMap.TaskExists()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("在任务中没有找到指定的任务, Task UUID: %v", _taskUUID)
	}

	// 判断 有没有需要迁移的 schema
	exists = configMap.SchemaMapExists()
	if !exists {
		return nil, fmt.Errorf("在任务中没有需要迁移的 schema, Task UUID: %v", _taskUUID)
	}

	// 判断 有没有需要迁移的 Table
	exists = configMap.TableMapExists()
	if !exists {
		return nil, fmt.Errorf("在任务中没有需要迁移的 table, Task UUID: %v", _taskUUID)
	}

	// 获取源实例信息
	err = configMap.InitSource()
	if err != nil {
		return nil, err
	}

	// 获取目标实例信息
	err = configMap.InitTarget()
	if err != nil {
		return nil, err
	}

	// 获取 默认运行参数
	err = configMap.InitRunQuota()
	if err != nil {
		return nil, err
	}

	// 获取 需要迁移的 schema
	err = configMap.InitSchemaMapMap()
	if err != nil {
		return nil, err
	}

	// 获取需要迁移的 table
	err = configMap.InitTableMapMap()
	if err != nil {
		return nil, err
	}

	// 获取需要迁移的 column
	err = configMap.InitColumnMapMap()
	if err != nil {
		return nil, err
	}

	// 设置不需要迁移的列
	err = configMap.InitIgnoreColumnMap()
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

func (this *ConfigMap) GetRandSchemaMap() *model.SchemaMap {
	for _, schemMap := range this.SchemaMapMap {
		return schemMap
	}

	return nil
}
