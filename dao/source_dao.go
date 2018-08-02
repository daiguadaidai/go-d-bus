package dao

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
	"database/sql"
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

/* 更新位点信息
Params:
	_taskUUID: 任务ID
	_startLogFile: 开始的日志文件
	_startLogPos: 开始的日志位点
	_parseLogFile: 解析到的日志文件
	_parseLogPos: 解析到的日志位点
	_appliedLogFile: 已经应用到的日志文件
	_appliedLogPos: 已经应用到的日志位点
	_stopLogFile: 停止的日志文件
	_stopLogPos: 停止的日志位点
 */
func (this *SourceDao) UpdateLogPosInfo(
	_taskUUID string,
	_startLogFile string,
	_startLogPos int,
	_parseLogFile string,
	_parseLogPos uint32,
	_appliedLogFile string,
	_appliedLogPos int,
	_stopLogFile string,
	_stopLogPos int,
) int {
	ormInstance := gdbc.GetOrmInstance()

	updateSource := model.Source{}

	// 有设置开始位点信息
	if _startLogFile != "" {
		updateSource.StartLogFile = sql.NullString{_startLogFile, true}
	}
	if _startLogPos >= 0 {
		updateSource.StartLogPos = sql.NullInt64{int64(_startLogPos), true}
	}

	// 有设置解析位点信息
	if _parseLogFile != "" {
		updateSource.ParseLogFile = sql.NullString{_parseLogFile, true}
	}
	if _parseLogPos >= 0 {
		updateSource.ParseLogPos = sql.NullInt64{int64(_parseLogPos), true}
	}

	// 有设置应用到位点信息
	if _appliedLogFile != "" {
		updateSource.ApplyLogFile = sql.NullString{_appliedLogFile, true}
	}
	if _appliedLogPos >= 0 {
		updateSource.ApplyLogPos = sql.NullInt64{int64(_appliedLogPos), true}
	}

	// 设置停止位点的信息
	if _appliedLogFile != "" {
		updateSource.StopLogFile = sql.NullString{_stopLogFile, true}
	}
	if _appliedLogPos >= 0 {
		updateSource.StopLogPos = sql.NullInt64{int64(_stopLogPos), true}
	}

	affected := ormInstance.DB.Model(&model.Source{}).Where(
		"`task_uuid`=?",
		_taskUUID,
	).Updates(updateSource).RowsAffected

	return int(affected)

}