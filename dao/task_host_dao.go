package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
)

type TaskHostDao struct{}

func (this *TaskHostDao) GetByID(id int64, columnStr string) (*model.TaskHost, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskHost := new(model.TaskHost)
    err := ormInstance.DB.Select(columnStr).Where("id = ?", id).First(taskHost).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return nil, nil
        }
        return nil, err
    }

    return taskHost, nil
}

func (this *TaskHostDao) GetLeastAvailable(idc string, columnStr string) (*model.TaskHost, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskHost := new(model.TaskHost)
    err := ormInstance.DB.Select(columnStr).Where("idc = ? AND is_available=1", idc).Order("curr_process_cnt ASC").First(taskHost).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return nil, nil
        }
        return nil, err
    }

    return taskHost, nil
}

func (this *TaskHostDao) FindByHost(host string, columnStr string) ([]model.TaskHost, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskHosts := []model.TaskHost{}
    err := ormInstance.DB.Select(columnStr).Where("host = ?", host).Find(&taskHosts).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return taskHosts, nil
        }
        return nil, err
    }

    return taskHosts, nil
}

func (this *TaskHostDao) FindByAvailable(isAvailable int64, columnStr string) ([]model.TaskHost, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskHosts := []model.TaskHost{}
    err := ormInstance.DB.Select(columnStr).Where("is_available = ?", isAvailable).Find(&taskHosts).Error
    if err != nil {
        return nil, err
    }

    return taskHosts, nil
}

func (this *TaskHostDao) FindByAvailableAndIDC(isAvailable int64, idc string, columnStr string) ([]model.TaskHost, error) {
    ormInstance := gdbc.GetOrmInstance()

    taskHosts := []model.TaskHost{}
    err := ormInstance.DB.Select(columnStr).Where("is_available = ? and idc = ?", isAvailable, idc).Find(&taskHosts).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return taskHosts, nil
        }
        return nil, err
    }

    return taskHosts, nil
}
