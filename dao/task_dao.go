package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
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
