package mysqlapplybinlog

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/outbrain/golib/log"
	"syscall"
	"time"
)

/* 判断是否是需要应用的表
Params:
	_schema: 需要检测的数据库
	_table: 需要检测的表
*/
func (this *ApplyBinlog) IsApplyTable(_schema string, _table string) bool {
	_, ok := this.NeedApplyTableMap[common.FormatTableName(_schema, _table, "")]

	return ok
}

// 等待binlog消费完成后, 并且替换表元数据信息
func (this *ApplyBinlog) WaitingApplyEventAndReplaceTableMap(_schemaName string, _tableName string) {
	// 每秒检测还需要应用的事件是否为 0
	for {
		// 需要应用的事件为0后就可以开始重新生成该表的元数据信息
		if this.NeedApplyEventCount.Load() == 0 {
			log.Infof("%v: 队列中的剩余binlog event已经消费完成. 开始生成新的元数据, %v.%v",
				common.CurrLine(), _schemaName, _tableName)

			migrationTable, err := matemap.NewTable(this.ConfigMap, _schemaName, _tableName)
			if err != nil {
				log.Errorf("%v: 失败. 重新生成需要迁移的表元数据信息. "+
					"%v.%v 退出迁移 %v",
					common.CurrLine(), _schemaName, _tableName, err)
				syscall.Exit(1)
			}
			if migrationTable == nil {
				log.Errorf("%v: 失败. 无法重新生成表元数据信息. "+
					"%v.%v 退出迁移 %v",
					common.CurrLine(), _schemaName, _tableName, err)
				syscall.Exit(1)
			}

			// 设置新生成的需要迁移的表元数据信息
			matemap.SetMigrationTableMap(common.FormatTableName(_schemaName, _tableName, ""),
				migrationTable)

			log.Infof("%v: 成功生成表的元数据, %v.%v",
				common.CurrLine(), _schemaName, _tableName)

			return
		} else {
			log.Warningf("%v: 检测到还有binlog event没有消费完成. 等待消费, "+
				"再进行生成表的元数据. %v.%v",
				common.CurrLine(), _schemaName, _tableName)
		}

		time.Sleep(time.Second)
	}

}

// 通过停止位点信息判断是否需要停止解析binlog
func (this *ApplyBinlog) IsStopParseBinlogByStopLogFilePos() bool {
	if this.StopLogFile == "" {
		return false
	}
	if this.StopLogFile != "" {
		if this.ParsedLogFile > this.StopLogFile {
			return true
		} else if this.ParsedLogFile == this.StopLogFile && int(this.ParsedLogPos) >= this.StopLogPos {
			return true
		}
	}

	return false
}

/* 更新源已经应用过了的位点信息
Params:
	_taskUUID: 任务ID
	_startLogFile: 开始的日志文件
	_startLogPos: 开始的日志位点
	_parseLogFile: 解析到的日志文件
	_parseLogPos: 解析到的日志位点
	_appliedLogFile: 已经应用到的日志文件
	_appliedLogPos: 已经应用到的日志位点
	_stopLogFile: 停止位点文件
	_stopLogPos: 停止位点信息
*/
func UpdateSourceLogPosInfo(
	_taskUUID string,
	_startLogFile string,
	_startLogPos int,
	_parseLogFile string,
	_parseLogPos int,
	_appliedLogFile string,
	_appliedLogPos int,
	_stopLogFile string,
	_stopLogPos int,
) int {
	sourceDao := new(dao.SourceDao)
	affected := sourceDao.UpdateLogPosInfo(_taskUUID, _startLogFile, _startLogPos, _parseLogFile, _parseLogPos, _appliedLogFile, _appliedLogPos, _stopLogFile, _stopLogPos)

	return affected
}

/* 获取指定任务的停止位点信息
Params:
	_taskUUID: 任务UUID
*/
func GetStopLogFilePos(_taskUUID string) (string, int) {
	var stopLogFile string = ""
	var stopLogPos int = -1

	sourceDao := new(dao.SourceDao)
	columnStr := "stop_log_file, stop_log_pos"
	source, err := sourceDao.GetByTaskUUID(_taskUUID, columnStr)
	if err != nil {
		log.Errorf("%v: 错误. 获取停止位点信息失败. 将设置为没有停止位点. %v", common.CurrLine(), err)
		return stopLogFile, stopLogPos
	}

	if source.StopLogFile.Valid {
		stopLogFile = source.StopLogFile.String
	}
	if source.StopLogPos.Valid {
		stopLogPos = int(source.StopLogPos.Int64)
	}

	return stopLogFile, stopLogPos
}

/* 执行 show master status 获取数据库位点信息
Params:
	_host: 实例IP
	_port: 实例端口
*/
func ShowMasterStatus(host string, port int) (string, int, error) {
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return "", -1, fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 执行 show master status 获取数据库位点信息", common.CurrLine(), host, port)
	}

	sql := "/* go-d-bus */ SHOW MASTER STATUS"
	row := instance.QueryRow(sql)

	var logFile string
	var logPos int
	var ignore interface{}
	if err := row.Scan(&logFile, &logPos, &ignore, &ignore, &ignore); err != nil {
		return "", -1, fmt.Errorf("%v: scan 字段出错, 执行 show master status 获取数据库位点信息. %v", common.CurrLine(), err)
	}

	return logFile, logPos, nil
}

/* 更新目标实例位点信息
Params:
	_host: 实例IP
	_port: 实例端口
*/
func UpdateTargetLogFilePos(_taskUUID string, _logFile string, _logPos int) int {
	targetDao := new(dao.TargetDao)
	affected := targetDao.UpdateLogFilePos(_taskUUID, _logFile, _logPos)

	return affected
}
