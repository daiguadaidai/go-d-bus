package dao

import (
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/daiguadaidai/go-d-bus/sql"
    "github.com/jinzhu/gorm"
)

type SchemaMapDao struct{}

func (this *SchemaMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.SchemaMap, error) {
    ormInstance := sql.GetOrmInstance()

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
