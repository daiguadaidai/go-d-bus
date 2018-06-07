package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
)

type TaskRunHistoryDao struct{}

func (this *TaskRunHistoryDao) GetByID(id int64, columnStr string) (*model.TaskRunHistory, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskRunHistory := new(model.TaskRunHistory)
    err := ormInstance.DB.Select(columnStr).Where("id = ?", id).First(taskRunHistory).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return nil, nil
        }
        return nil, err
    }

    return taskRunHistory, nil
}

func (this *TaskRunHistoryDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.TaskRunHistory, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskRunHistorys := []model.TaskRunHistory{}
    err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&taskRunHistorys).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return taskRunHistorys, nil
        }
        return nil, err
    }

    return taskRunHistorys, nil
}
