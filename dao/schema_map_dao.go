package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/jinzhu/gorm"
    "github.com/daiguadaidai/go-d-bus/gdbc"
)

type SchemaMapDao struct{}

// 通过 uuid 获取 所有的schema
func (this *SchemaMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.SchemaMap, error) {
    ormInstance := gdbc.GetOrmInstance()

    schemaMaps := []model.SchemaMap{}
    err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&schemaMaps).Error
    if err != nil {
        if err == gorm.ErrRecordNotFound {
            return schemaMaps, nil
        }
        return nil, err
    }

    return schemaMaps, nil
}

// 通过 uuid 获取 schema 数量
func (this *SchemaMapDao) Count(taskUUID string) int {
    ormInstance := gdbc.GetOrmInstance()

    count := 0
    ormInstance.DB.Model(&model.SchemaMap{}).Where("task_uuid = ?", taskUUID).Count(&count)

    return count
}
