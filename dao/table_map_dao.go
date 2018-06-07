package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
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
