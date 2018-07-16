package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type SourceDao struct{}

func (this *SourceDao) GetByID(id int64, columnStr string) (*model.Source, error) {
	ormInstance := gdbc.GetOrmInstance()

	source := new(model.Source)
	err := ormInstance.DB.Select(columnStr).Where("id = ?", id).First(source).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return source, nil
}

func (this *SourceDao) GetByTaskUUID(taskUUID string, columnStr string) (*model.Source, error) {
	ormInstance := gdbc.GetOrmInstance()

	source := new(model.Source)
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(source).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return source, nil
}

func (this *SourceDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.Source, error) {
	ormInstance := gdbc.GetOrmInstance()

	sources := []model.Source{}
	err := ormInstance.DB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&sources).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return sources, nil
		}
		return nil, err
	}

	return sources, nil
}
