package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type BinlogDeleteWhereExternalColumnDao struct{}

func (this *BinlogDeleteWhereExternalColumnDao) FindByTaskUUID(taskUUID string, columnStr string) ([]*model.BinlogDeleteWhereExternalColumn, error) {
	ormDB := gdbc.GetOrmInstance()

	var externalColumns []*model.BinlogDeleteWhereExternalColumn
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&externalColumns).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return externalColumns, nil
		}
		return nil, err
	}

	return externalColumns, nil
}
