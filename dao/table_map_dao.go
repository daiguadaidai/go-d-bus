package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/daiguadaidai/go-d-bus/sql"
    "github.com/jinzhu/gorm"
)

type TableMapDao struct{}

func (this *TableMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.TableMap, error) {
    ormInstance := sql.GetOrmInstance()

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
