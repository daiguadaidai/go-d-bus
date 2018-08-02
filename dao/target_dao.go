package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
	"database/sql"
)

type TargetDao struct{}

func (this *TargetDao) GetByID(id int64, columnStr string) (*model.Target, error) {
	ormInstance := gdbc.GetOrmInstance()

	target := new(model.Target)
	err := ormInstance.DB.Select(columnStr).Where("id = ?", id).First(target).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return target, nil
}

func (this *TargetDao) GetByTaskUUID(taskUUID string, columnStr string) (*model.Target, error) {
	ormInstance := gdbc.GetOrmInstance()

	target := new(model.Target)
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(target).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return target, nil
}

func (this *TargetDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.Target, error) {
	ormInstance := gdbc.GetOrmInstance()

	targets := []model.Target{}
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&targets).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return targets, nil
		}
		return nil, err
	}

	return targets, nil
}

/* 更新目标实例位点信息
Params:
	_taskUUID: 实例UUID
	_logFile: 日志文件
	_logPos: 日志位点
 */
func (this *TargetDao) UpdateLogFilePos(_taskUUID string, _logFile string, _logPos int) int {
	ormInstance := gdbc.GetOrmInstance()

	updateTarget := model.Target {
		RollbackLogFile: sql.NullString{_logFile, true},
		RollbackLogPos: sql.NullInt64{int64(_logPos), true},
	}

	affected := ormInstance.DB.Model(&model.Target{}).Where(
		"`task_uuid`=?",
		_taskUUID,
	).Updates(updateTarget).RowsAffected

	return int(affected)
}
