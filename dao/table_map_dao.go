package dao

import (
	"database/sql"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type TableMapDao struct{}

func (this *TableMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]*model.TableMap, error) {
	ormDB := gdbc.GetOrmInstance()

	var tableMaps []*model.TableMap
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&tableMaps).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return tableMaps, nil
		}
		return nil, err
	}

	return tableMaps, nil
}

// 通过 uuid 获取 schema 数量
func (this *TableMapDao) Count(taskUUID string) int {
	ormDB := gdbc.GetOrmInstance()

	count := 0
	ormDB.Model(&model.TableMap{}).Where("task_uuid = ?", taskUUID).Count(&count)

	return count
}

// 标记表row copy 完成
func (this *TableMapDao) TagTableRowCopyComplete(taskUUID string, schema string, table string) int {
	ormDB := gdbc.GetOrmInstance()

	updateTableMap := model.TableMap{RowCopyComplete: sql.NullInt64{1, true}}
	affected := ormDB.Model(&model.TableMap{}).Where("`task_uuid`=? AND `schema`=? AND `source`=?", taskUUID, schema, table).Updates(updateTableMap).RowsAffected

	return int(affected)
}

/* 跟新表当前row copy 到的主键值
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
*/
func (this *TableMapDao) UpdateCurrIDValue(taskUUID, schema, table, jsonData string) int {
	ormDB := gdbc.GetOrmInstance()

	updateTableMap := model.TableMap{CurrIDValue: sql.NullString{jsonData, true}}
	affected := ormDB.Model(&model.TableMap{}).Where("`task_uuid`=? AND `schema`=? AND `source`=?", taskUUID, schema, table).Updates(updateTableMap).RowsAffected

	return int(affected)
}

/* 跟新表row copy 截止的主键值
Params:
    taskUUID: 任务ID
    schema: 数据库名
    table: 表名
    jsonData: 需要更新的数据
*/
func (this *TableMapDao) UpdateMaxIDValue(taskUUID, schema, table, jsonData string) int {
	ormDB := gdbc.GetOrmInstance()

	updateTableMap := model.TableMap{MaxIDValue: sql.NullString{jsonData, true}}
	affected := ormDB.Model(&model.TableMap{}).Where("`task_uuid`=? AND `schema`=? AND `source`=?", taskUUID, schema, table).Updates(updateTableMap).RowsAffected

	return int(affected)
}
