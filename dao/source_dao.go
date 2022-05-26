package dao

import (
	"database/sql"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/jinzhu/gorm"
)

type SourceDao struct{}

func (this *SourceDao) GetByID(id int64, columnStr string) (*model.Source, error) {
	ormDB := gdbc.GetOrmInstance()

	source := new(model.Source)
	err := ormDB.Select(columnStr).Where("id = ?", id).First(source).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return source, nil
}

func (this *SourceDao) GetByTaskUUID(taskUUID string, columnStr string) (*model.Source, error) {
	ormDB := gdbc.GetOrmInstance()

	source := new(model.Source)
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).First(source).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}

	return source, nil
}

func (this *SourceDao) FindByTaskUUID(taskUUID string, columnStr string) ([]model.Source, error) {
	ormDB := gdbc.GetOrmInstance()

	sources := []model.Source{}
	err := ormDB.Select(columnStr).Where("task_uuid = ?", taskUUID).Find(&sources).Error
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
	parseLogFile: 解析到的日志文件
	parseLogPos: 解析到的日志位点
	appliedLogFile: 已经应用到的日志文件
	appliedLogPos: 已经应用到的日志位点
	_stopLogFile: 停止的日志文件
	_stopLogPos: 停止的日志位点
*/
func (this *SourceDao) UpdateLogPosInfo(
	taskUUID string,
	startLogFile string,
	startLogPos int,
	parseLogFile string,
	parseLogPos int,
	appliedLogFile string,
	appliedLogPos int,
	stopLogFile string,
	stopLogPos int,
) int {
	ormDB := gdbc.GetOrmInstance()

	updateSource := model.Source{}

	// 有设置开始位点信息
	if startLogFile != "" {
		updateSource.StartLogFile = sql.NullString{startLogFile, true}
	}
	if startLogPos >= 0 {
		updateSource.StartLogPos = sql.NullInt64{int64(startLogPos), true}
	}

	// 有设置解析位点信息
	if parseLogFile != "" {
		updateSource.ParseLogFile = sql.NullString{parseLogFile, true}
	}
	if parseLogPos >= 0 {
		updateSource.ParseLogPos = sql.NullInt64{int64(parseLogPos), true}
	}

	// 有设置应用到位点信息
	if appliedLogFile != "" {
		updateSource.ApplyLogFile = sql.NullString{appliedLogFile, true}
	}
	if appliedLogPos >= 0 {
		updateSource.ApplyLogPos = sql.NullInt64{int64(appliedLogPos), true}
	}

	// 设置停止位点的信息
	if appliedLogFile != "" {
		updateSource.StopLogFile = sql.NullString{stopLogFile, true}
	}
	if appliedLogPos >= 0 {
		updateSource.StopLogPos = sql.NullInt64{int64(stopLogPos), true}
	}

	affected := ormDB.Model(&model.Source{}).Where("`task_uuid`=?", taskUUID).Updates(updateSource).RowsAffected

	return int(affected)
}

func (this *SourceDao) UpdateStartLogPosInfo(taskUUID string, startLogFile string, startLogPos int) error {
	ormDB := gdbc.GetOrmInstance()

	updateSource := model.Source{}

	// 有设置开始位点信息
	if startLogFile != "" {
		updateSource.StartLogFile = sql.NullString{startLogFile, true}
	}
	if startLogPos >= 0 {
		updateSource.StartLogPos = sql.NullInt64{int64(startLogPos), true}
	}

	return ormDB.Model(&model.Source{}).Where("`task_uuid`=?", taskUUID).Updates(updateSource).Error
}
