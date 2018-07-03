package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
    "database/sql"
)

type TableMapDao struct{}

func (this *TableMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.TableMap, error) {
    ormInstance := gdbc.GetOrmInstance()

    tableMaps := []model.TableMap{}
    err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&tableMaps).Error
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
    ormInstance := gdbc.GetOrmInstance()

    count := 0
    ormInstance.DB.Model(&model.TableMap{}).Where("task_uuid = ?", taskUUID).Count(&count)

    return count
}

// 标记表row copy 完成
func (this *TableMapDao) TagTableRowCopyComplete(_taskUUID string, _schema string, _table string) int {
    ormInstance := gdbc.GetOrmInstance()

    updateTableMap := model.TableMap{RowCopyComplete: sql.NullInt64{1, true}}
    affected := ormInstance.DB.Model(&model.TableMap{}).Where(
        "`task_uuid`=? AND `schema`=? AND `source`=?",
        _taskUUID,
        _schema,
        _table,
    ).Updates(updateTableMap).RowsAffected

    return int(affected)
}

/* 跟新表当前row copy 到的主键值
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
 */
func (this *TableMapDao)UpdateCurrIDValue(_taskUUID , _schema, _table, _jsonData string) int {
    ormInstance := gdbc.GetOrmInstance()

    updateTableMap := model.TableMap{CurrIDValue: sql.NullString{_jsonData, true}}
    affected := ormInstance.DB.Model(&model.TableMap{}).Where(
        "`task_uuid`=? AND `schema`=? AND `source`=?",
        _taskUUID,
        _schema,
        _table,
    ).Updates(updateTableMap).RowsAffected

    return int(affected)
}

/* 跟新表row copy 截止的主键值
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
 */
func (this *TableMapDao) UpdateMaxIDValue(_taskUUID , _schema, _table, _jsonData string) int {
    ormInstance := gdbc.GetOrmInstance()

    updateTableMap := model.TableMap{MaxIDValue: sql.NullString{_jsonData, true}}
    affected := ormInstance.DB.Model(&model.TableMap{}).Where(
        "`task_uuid`=? AND `schema`=? AND `source`=?",
        _taskUUID,
        _schema,
        _table,
    ).Updates(updateTableMap).RowsAffected

    return int(affected)
}
