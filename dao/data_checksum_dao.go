package dao

import (
	"database/sql"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type DataChecksumDao struct{}

/* 获取指定任务所有有不一致情况的数据
Params:
    taskUUID: 任务UUID
    columnStr: 需要查询的字段有哪些
 */
func (this *DataChecksumDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.DataChecksum, error) {
	ormInstance := gdbc.GetOrmInstance()

	dataChecksums := []model.DataChecksum{}
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&dataChecksums).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return dataChecksums, nil
		}
		return nil, err
	}

	return dataChecksums, nil
}

/* 获取指定任务所有还没有修复的不一致情况的数据
Params:
    taskUUID: 任务UUID
    columnStr: 需要查询的字段有哪些
 */
func (this *DataChecksumDao) FindNoFixByTaskUUID(taskUUID string, columnStr string) ([]model.DataChecksum, error) {
	ormInstance := gdbc.GetOrmInstance()

	dataChecksums := []model.DataChecksum{}
	err := ormInstance.DB.Select(columnStr).Where(
		"task_uuid = ? AND is_fix = 0",
		taskUUID,
	).Find(&dataChecksums).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return dataChecksums, nil
		}
		return nil, err
	}

	return dataChecksums, nil
}


/* 保存数据
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
*/
func (this *DataChecksumDao) Create(_dataChecksum model.DataChecksum) error {
	ormInstance := gdbc.GetOrmInstance()

	tx := ormInstance.DB.Begin()

	if err := tx.Create(&_dataChecksum).Error; err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}


/* 标记checksum不一致的已经修复
Params:
    _id: 主键ID
*/
func (this *DataChecksumDao) FixCompletedByID(_id int64) int {
	ormInstance := gdbc.GetOrmInstance()

	updateDataChecksum := model.DataChecksum{IsFix: sql.NullInt64{1, true}}
	affected := ormInstance.DB.Model(&model.DataChecksum{}).Where(
		"`id`=?",
		_id,
	).Updates(updateDataChecksum).RowsAffected

	return int(affected)
}


