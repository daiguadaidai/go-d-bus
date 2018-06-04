package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/daiguadaidai/go-d-bus/sql"
    "github.com/jinzhu/gorm"
)

type TargetDao struct{}

func (this *TargetDao) GetByID(id int64, columnStr string) (*model.Target, error) {
    ormInstance := sql.GetOrmInstance()

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
    ormInstance := sql.GetOrmInstance()

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
    ormInstance := sql.GetOrmInstance()

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
