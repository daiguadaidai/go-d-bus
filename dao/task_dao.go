package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
	"database/sql"
)

type TaskDao struct{}

func (this *TaskDao) GetByID(id int64, columnStr string) (*model.Task, error) {
	ormInstance := gdbc.GetOrmInstance()

	task := new(model.Task)
	err := ormInstance.DB.Select(columnStr).Where("id = ?", id).First(task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (this *TaskDao) GetByTaskUUID(taskUUID string, columnStr string) (*model.Task, error) {
	ormInstance := gdbc.GetOrmInstance()

	task := new(model.Task)
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (this *TaskDao) Count(_taskUUID string) int {
	ormInstance := gdbc.GetOrmInstance()

	count := 0
	ormInstance.DB.Model(&model.Task{}).Where("task_uuid = ?", _taskUUID).Count(&count)

	return count
}

/* 更新任务row copy完成
Params:
    _taskUUID: 任务ID
*/
func (this *TaskDao) TagTaskRowCopyComplete(_taskUUID string) int {
	ormInstance := gdbc.GetOrmInstance()

	updateTask := model.Task{RowCopyComplete: sql.NullInt64{1, true}}
	affected := ormInstance.DB.Model(&model.Task{}).Where(
		"`task_uuid`=?",
		_taskUUID,
	).Updates(updateTask).RowsAffected

	return int(affected)
}
