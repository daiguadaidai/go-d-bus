package dao

import (
	"database/sql"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type TaskDao struct{}

func (this *TaskDao) GetByID(id int64, columnStr string) (*model.Task, error) {
	ormDB := gdbc.GetOrmInstance()

	task := new(model.Task)
	err := ormDB.Select(columnStr).Where("id = ?", id).First(task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (this *TaskDao) GetByTaskUUID(taskUUID string, columnStr string) (*model.Task, error) {
	ormDB := gdbc.GetOrmInstance()

	task := new(model.Task)
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (this *TaskDao) Count(taskUUID string) int {
	ormDB := gdbc.GetOrmInstance()

	count := 0
	ormDB.Model(&model.Task{}).Where("task_uuid = ?", taskUUID).Count(&count)

	return count
}

/* 更新任务row copy完成
Params:
    _taskUUID: 任务ID
*/
func (this *TaskDao) TagTaskRowCopyComplete(taskUUID string) int {
	ormDB := gdbc.GetOrmInstance()

	updateTask := model.Task{RowCopyComplete: sql.NullInt64{1, true}}
	affected := ormDB.Model(&model.Task{}).Where("`task_uuid`=?", taskUUID).Updates(updateTask).RowsAffected

	return int(affected)
}

/* row copy任务是否完成
Params:
    _taskUUID: 任务ID
*/
func (this *TaskDao) TaskRowCopyIsComplete(taskUUID string) (bool, error) {
	ormDB := gdbc.GetOrmInstance()

	task := new(model.Task)
	columnStr := "row_copy_complete"
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(task).Error
	if err != nil {
		return true, err
	}

	if task.RowCopyComplete.Int64 == 1 {
		return true, nil
	} else {
		return false, nil
	}

	return true, nil
}
