package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type ColumnMapDao struct{}

func (this *ColumnMapDao) FindByTaskUUID(taskUUID string, columnStr string) ([]*model.ColumnMap, error) {
	ormDB := gdbc.GetOrmInstance()

	var columnMaps []*model.ColumnMap
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&columnMaps).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return columnMaps, nil
		}
		return nil, err
	}

	return columnMaps, nil
}
