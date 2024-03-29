package parser

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/logger"
	"strings"
)

const (
	APPLY_BINLOG_PARALLER        = 8     // 默认应用binlog并发数
	ROW_COPY_PARALLER            = 8     // 默认 row copy 并发数
	CHECKSUM_PARALLER            = 1     // 默认 checksum 并发数
	CHECKSUM_FIX_PARALLER        = 1     // 默认 checksum修复数据 并发数
	APPLY_BINLOG_HIGH_WATER_MARK = 10000 // 默认 binlog 缓存队列大小
	ROW_COPY_HIGH_WATER_MARK     = 100   // 默认 row copy 队列缓存大小
	ROW_COPY_LIMIT               = 1000  // 默认 每次 row copy 行数
	HEARTBEAT_SCHEMA             = ""    // 默认 心跳库
	HEARTBEAT_TABLE              = ""    // 默认 心跳表
	ERR_RETRY_COUNT              = 60    // 默认出错重试次数
)

// 在启动一个任务时用于接收和保存 命令行输入的参数值
type RunParser struct {
	TaskUUID string // 需要运行的任务id

	StartLogFile string // 任务开始binlog文件
	StartLogPos  int    // 任务开始binlog 位点

	StopLogFile string // 应用到那个 binlog 停止
	StopLogPos  int    // 应用到 binlog 哪个位点停止

	EnableRowCopy     bool // 是否运行 row copy
	EnableApplyBinlog bool // 是否运行应用 binlog
	EnableChecksum    bool // 是否运行checksum

	RowCopyParaller     int // row copy 的并发数
	ApplyBinlogParaller int // 应用binlog 的并发数
	ChecksumParaller    int // checksum 的并发数
	ChecksumFixParaller int // checkusm修复数据并发数

	ApplyBinlogHighWaterMark int // 进行 应用 binlog 队列中最多缓存多少个值
	RowCopyHighWaterMark     int // 进行row copy队列中最多缓存多少个值

	RowCopyLimit int // 进行每次 row copy 的行数

	HeartbeatSchema string // 心跳数据库
	HeartbeatTable  string // 心跳表 该表的数据不会被应用, 主要是为了解析的位点能不段变, 应用的位点有可能不变

	ErrRetryCount int // 当出现错误的时候默认重试次数
}

// 对输入的命令进行检测
func (this *RunParser) Parse() error {
	// 检测任务信息
	err := DetectTask(this.TaskUUID)
	if err != nil {
		return err
	}

	// 解析开始binlog信息
	if err := this.ParseStartBinlogInfo(); err != nil {
		return err
	}

	// 解析停止 binlog 位点
	if err := this.ParseStopBinlogInfo(); err != nil {
		return err
	}

	// 解析并发队列缓存大小
	this.ParseApplyBinlogHighWaterMark()
	this.ParseRowCopyHighWaterMark()

	// 解析相关并发数
	this.ParseApplyBinlogParaller()
	this.ParseRowCopyParaller()
	this.ParseChecksumParaller()
	this.ParseChecksumFixParaller()

	// 解析每次row copy行数
	this.ParseRowCopyLimit()

	// 解析 heartbeat schema 和 heartbeat table
	if err := this.ParseHeartbeat(); err != nil {
		return err
	}

	// 解析 出错重试次数
	this.ParseErrRetryCount()

	return nil
}

// 解析开始的binlog信息
func (this *RunParser) ParseStartBinlogInfo() error {
	// 如果有手动指定开始位点则不需要去数据库中取
	if strings.TrimSpace(this.StartLogFile) != "" { // 命令行有指定开始的 binlog 文件
		if this.StartLogPos >= 0 { // 命令行有指定开始的 binlog 位点
			return nil
		} else { // 命令行没有指定开始的binlog 位点, 进行赋值为 0
			logger.M.Warnf("指定了开始binlog文件, 但是没有指定开始binlog pos, 将开始binlog pos 设置为0. %v -> 0", this.StartLogPos)
			this.StartLogPos = 0
			return nil
		}
	}

	// 没有指定开始位点信息, 则中数据库中获取
	sourceDao := new(dao.SourceDao)
	columnStr := "log_file, log_pos, start_log_file, start_log_pos"
	source, err := sourceDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		return fmt.Errorf("失败. 获取数据库源实例开始位点信息(获取数据库错误). Task UUID: %v %v", this.TaskUUID, err)
	}

	// 数据库中有当期应用到的位点
	if source.ApplyLogFile.Valid && strings.TrimSpace(source.ApplyLogFile.String) != "" {
		this.StartLogFile = source.ApplyLogFile.String

		if source.ApplyLogPos.Valid && source.ApplyLogPos.Int64 >= 0 { // 有当期应用 pos
			this.StartLogPos = int(source.ApplyLogPos.Int64)
		} else { // 没有 当前 pos
			this.StartLogPos = 0
			return nil
		}
		logger.M.Warnf("位点信息来源于数据库的当前应用位点, %v:%v", this.StartLogFile, this.StartLogPos)

		return nil
	}

	// 数据库中有 开始位点信息
	if source.StartLogFile.Valid && strings.TrimSpace(source.StartLogFile.String) != "" {
		this.StartLogFile = source.StartLogFile.String

		if source.StartLogPos.Valid && source.StartLogPos.Int64 >= 0 { // 有开始 pos
			this.StartLogPos = int(source.StartLogPos.Int64)
		} else { // 没有 开始 pos
			this.StartLogPos = 0
			return nil
		}
		logger.M.Warnf("位点信息来源于数据库的开始位点, %v:%v", this.StartLogFile, this.StartLogPos)

		return nil
	}

	// 没有有效可用的 binlog位点, 会在后面使用 show master status 来获取
	this.StartLogFile = ""
	this.StartLogPos = -1
	logger.M.Warn("没有获取到有效的开始位点信息")

	return nil
}

// 解析停止的binlog信息
func (this *RunParser) ParseStopBinlogInfo() error {
	// 如果有手动指定停止位点则不需要去数据库中取
	if strings.TrimSpace(this.StopLogFile) != "" { // 命令行有指定停止的 binlog 文件
		if this.StopLogPos >= 0 { // 命令行有指定停止的 binlog 位点
			return nil
		} else { // 命令行没有指定停止的binlog 位点, 进行赋值为 0
			logger.M.Warnf("指定了停止binlog文件, 但是没有指定停止binlog pos, 将停止binlog pos 设置为0. %v -> 0", this.StopLogPos)
			this.StopLogPos = 0
			return nil
		}
	}

	// 没有指定开始位点信息, 则中数据库中获取
	sourceDao := new(dao.SourceDao)
	columnStr := "stop_log_file, stop_log_pos"
	source, err := sourceDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		return fmt.Errorf("失败. 获取数据库源实例停止位点信息(获取数据库错误). Task UUID: %v %v", this.TaskUUID, err)
	}

	// 数据库中有指定停止位点
	if source.StopLogFile.Valid && strings.TrimSpace(source.StopLogFile.String) != "" {
		this.StartLogFile = source.StopLogFile.String

		if source.StopLogPos.Valid && source.StopLogPos.Int64 >= 0 { // 有当期应用 pos
			this.StartLogPos = int(source.StopLogPos.Int64)
		} else { // 没有 当前 pos
			this.StopLogPos = 0
			return nil
		}
		logger.M.Warnf("位点信息来源于数据库的停止位点, %v:%v", this.StopLogFile, this.StopLogPos)

		return nil
	}

	this.StopLogFile = ""
	this.StopLogPos = -1
	logger.M.Warn("有指定和获取到停止的位点信息")

	return nil
}

// 解析 应用binlog时的并发
func (this *RunParser) ParseApplyBinlogParaller() {
	// 如果命令行有传入参数, 直接返回
	if this.ApplyBinlogParaller > 0 {
		return
	}

	// 命令行中没有指定应用binlog的并发数, 则从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "binlog_paraller"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析应用binlog并发参数失败(从数据库获取数据时). 将设置称默认值: %v", APPLY_BINLOG_PARALLER)
		this.ApplyBinlogParaller = APPLY_BINLOG_PARALLER
		return
	}

	// 数据库中有 应用binlog的并发数
	if task.BinlogParaller.Valid && task.BinlogParaller.Int64 > 0 {
		logger.M.Warnf("Apply Binlog 并发数从数据库中获取. %v", task.RowCopyParaller.Int64)
		this.ApplyBinlogParaller = int(task.BinlogParaller.Int64)
		return
	}

	// 数据库也获取不到则使用默认值
	this.ApplyBinlogParaller = APPLY_BINLOG_PARALLER
	logger.M.Warnf("无法获取到 Apply Binlog 并发数. 使用默认值: %v", APPLY_BINLOG_PARALLER)
	return
}

// 解析 row copy 并发
func (this *RunParser) ParseRowCopyParaller() {
	// 如果在命令行参数中有指定row copy 并发数. 则使用命令行中的并发数
	if this.RowCopyParaller > 0 {
		return
	}

	// 如果命令行没指定则从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "row_copy_paraller"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析并发参数失败(从数据库获取数据时). 将设置称默认值: %v", ROW_COPY_PARALLER)
		this.RowCopyParaller = ROW_COPY_PARALLER
		return
	}

	// 在数据库中有 row copy 的并发数
	if task.RowCopyParaller.Valid && task.RowCopyParaller.Int64 > 0 {
		logger.M.Warnf("Row copy 并发数从数据库中获取. %v", task.RowCopyParaller.Int64)
		this.RowCopyParaller = int(task.RowCopyParaller.Int64)
		return
	}

	// 数据库中没有则使用默认值
	this.RowCopyParaller = ROW_COPY_PARALLER
	logger.M.Warnf("无法获取到row copy 并发数. 使用默认值: %v", ROW_COPY_PARALLER)
	return
}

// 解析 checksum 并发
func (this *RunParser) ParseChecksumParaller() {
	// 如果在命令行参数中有指定 checksum 并发数. 则使用命令行中的并发数
	if this.ChecksumParaller > 0 {
		return
	}

	// 如果命令行没指定则从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "checksum_paraller"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析checksum并发参数失败(从数据库获取数据时). 将设置称默认值: %v", CHECKSUM_PARALLER)
		this.ChecksumParaller = CHECKSUM_PARALLER
		return
	}

	// 在数据库中有 row copy 的并发数
	if task.ChecksumParaller.Valid && task.ChecksumParaller.Int64 > 0 {
		logger.M.Warnf("Checksum 并发数从数据库中获取. %v", task.ChecksumParaller.Int64)
		this.ChecksumParaller = int(task.ChecksumParaller.Int64)
		return
	}

	// 数据库中没有则使用默认值
	this.ChecksumParaller = CHECKSUM_PARALLER
	logger.M.Warnf("无法获取到 checksum 并发数. 使用默认值: %v", CHECKSUM_PARALLER)
	return
}

func (this *RunParser) ParseChecksumFixParaller() {
	// 如果在命令行参数中有指定 checksum 并发数. 则使用命令行中的并发数
	if this.ChecksumFixParaller > 0 {
		return
	}

	// 如果命令行没指定则从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "checksum_fix_paraller"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析checksum修复数据并发参数失败(从数据库获取数据时). 将设置称默认值: %v", CHECKSUM_FIX_PARALLER)
		this.ChecksumFixParaller = CHECKSUM_FIX_PARALLER
		return
	}

	// 在数据库中有 row copy 的并发数
	if task.ChecksumFixParaller.Valid && task.ChecksumFixParaller.Int64 > 0 {
		logger.M.Warnf("Checksum 修复数据并发数从数据库中获取 %v", task.ChecksumParaller.Int64)
		this.ChecksumFixParaller = int(task.ChecksumFixParaller.Int64)
		return
	}

	// 数据库中没有则使用默认值
	this.ChecksumFixParaller = CHECKSUM_FIX_PARALLER
	logger.M.Warnf("无法获取到 checksum 修复数据并发数. 使用默认值: %v", CHECKSUM_FIX_PARALLER)
	return
}

// 解析 binlog 事件缓存大小
func (this *RunParser) ParseApplyBinlogHighWaterMark() {
	// 如果在命令行参数中有指定binlog缓存大小. 则使用命令行的
	if this.ApplyBinlogHighWaterMark > 0 {
		return
	}

	// 数据库中没有则使用默认值
	this.ApplyBinlogHighWaterMark = APPLY_BINLOG_HIGH_WATER_MARK
	logger.M.Warnf("没有输入 Apply Binlog 缓存大小. 使用默认值: %v", APPLY_BINLOG_HIGH_WATER_MARK)
	return
}

// 解析 row copy 缓存大小
func (this *RunParser) ParseRowCopyHighWaterMark() {
	// 如果在命令行参数中有指定 row copy 缓存大小. 则使用命令行的
	if this.RowCopyHighWaterMark > 0 {
		return
	}

	// 数据库中没有则使用默认值
	this.RowCopyHighWaterMark = ROW_COPY_HIGH_WATER_MARK
	logger.M.Warnf("没有输入 Row Copy 缓存大小. 使用默认值: %v", ROW_COPY_HIGH_WATER_MARK)
	return
}

// 解析每次 row copy 的行数
func (this *RunParser) ParseRowCopyLimit() {
	// 如果在命令行参数中有指定 每次row copy 的行数.
	if this.RowCopyLimit > 0 {
		return
	}

	// 没有指定从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "row_copy_limit"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析每次row copy行数参数失败(从数据库获取数据时). 将设置称默认值: %v", ROW_COPY_LIMIT)
		this.RowCopyLimit = ROW_COPY_LIMIT
		return
	}

	// 在数据库中有 row copy 的并发数
	if task.RowCopyLimit.Valid && task.RowCopyLimit.Int64 > 0 {
		logger.M.Warnf("Row copy 并发数从数据库中获取. %v", task.RowCopyParaller.Int64)
		this.RowCopyLimit = int(task.RowCopyLimit.Int64)
		return
	}

	// 数据库中没有则使用默认值
	this.RowCopyLimit = ROW_COPY_LIMIT
	logger.M.Warnf("无法获取到每次row copy的行数. 使用默认值: %v", ROW_COPY_LIMIT)
	return
}

// 解析 心跳检测所需信息
func (this *RunParser) ParseHeartbeat() error {
	// 如果在命令行参数中有指定 heartbeat 库和表, 则使用命令行指定的
	if strings.TrimSpace(this.HeartbeatSchema) != "" && strings.TrimSpace(this.HeartbeatTable) != "" {
		return nil
	} else if strings.TrimSpace(this.HeartbeatSchema) == "" && strings.TrimSpace(this.HeartbeatTable) != "" {
		// 只指定了 heartbeat schema 或 heatbeat table 都不行, 必须两个都指定
		return fmt.Errorf("失败. heartbeat schema 和 heartbeat table 必须两个都指定, 你只指定了 heartbeat schema. %v.%v", this.HeartbeatSchema, this.HeartbeatTable)
	} else if strings.TrimSpace(this.HeartbeatSchema) != "" && strings.TrimSpace(this.HeartbeatTable) == "" {
		return fmt.Errorf("失败. heartbeat schema 和 heartbeat table 必须两个都指定, 你只指定了 heartbeat table. %v.%v", this.HeartbeatSchema, this.HeartbeatTable)
	}

	// 如果命令行没指定则从数据库中获取
	taskDao := new(dao.TaskDao)
	columnStr := "heartbeat_schema, heartbeat_table"
	task, err := taskDao.GetByTaskUUID(this.TaskUUID, columnStr)
	if err != nil {
		logger.M.Errorf("失败. 解析heartbeat信息(从数据库获取数据时). 将设置默认值为空字符串, 将不进行heartbeat binlog 的解析. %v", err)
		this.HeartbeatSchema = HEARTBEAT_SCHEMA
		this.HeartbeatTable = HEARTBEAT_TABLE
		return nil
	}

	// 数据库中获取到了 heartbeat schema 和 heartbeat table
	if task.HeartbeatSchema.Valid && strings.TrimSpace(task.HeartbeatSchema.String) != "" &&
		task.HeartbeatTable.Valid && strings.TrimSpace(task.HeartbeatTable.String) != "" {

		logger.M.Warnf("heartbeat 信息中数据库中获取 %v.%v", task.HeartbeatSchema.String, task.HeartbeatTable.String)
		this.HeartbeatSchema = task.HeartbeatSchema.String
		this.HeartbeatTable = task.HeartbeatTable.String

		return nil
	} else if task.HeartbeatSchema.Valid && strings.TrimSpace(task.HeartbeatSchema.String) != "" &&
		task.HeartbeatTable.Valid && strings.TrimSpace(task.HeartbeatTable.String) == "" {
		// 数据库中只有 heartbeats chema
		return fmt.Errorf("失败. 数据库中只指定了 heartbeat schema. %v", err)
	} else if task.HeartbeatSchema.Valid && strings.TrimSpace(task.HeartbeatSchema.String) != "" &&
		!task.HeartbeatTable.Valid {
		// 数据库中只有 heartbeats chema
		return fmt.Errorf("失败. 数据库中只指定了 heartbeat schema(2). %v", err)
	} else if task.HeartbeatSchema.Valid && strings.TrimSpace(task.HeartbeatSchema.String) == "" &&
		task.HeartbeatTable.Valid && strings.TrimSpace(task.HeartbeatTable.String) != "" {
		// 数据库中只有 heartbeats table
		return fmt.Errorf("失败. 数据库中只指定了 heartbeat table. %v", err)
	} else if !task.HeartbeatSchema.Valid && task.HeartbeatTable.Valid &&
		strings.TrimSpace(task.HeartbeatTable.String) != "" {
		// 数据库中只有 heartbeats table
		return fmt.Errorf("失败. 数据库中只指定了 heartbeat table(2). %v", err)
	}

	// 数据库中没有则使用默认值,
	this.HeartbeatSchema = HEARTBEAT_SCHEMA
	this.HeartbeatTable = HEARTBEAT_TABLE
	logger.M.Warn("没有指定, 数据库中也没有 heartbeat 相关信息, 该任务则不进行 heartbeat binlog 解析")
	return nil
}

func (this *RunParser) ParseErrRetryCount() {
	if this.ErrRetryCount < 0 {
		this.ErrRetryCount = ERR_RETRY_COUNT
	}
}

/* 设置binlog位点信息, 通过给的实例 host, port
Params:
    _host: 实例host
    _port: 实例port
*/
func (this *RunParser) SetStartBinlogInfoByHostAndPort(host string, port int) error {
	instance, ok := gdbc.GetDynamicDBByHostPort(host, int64(port))
	if !ok {
		return fmt.Errorf("缓存中不存在该实例(%v:%v). 设置binlog开始位点失败", host, port)
	}

	showSql := "/* go-d-bus */ SHOW MASTER STATUS"

	var file sql.NullString
	var position sql.NullInt64
	var binlogDoDB sql.NullString
	var binlogIgnoreDB sql.NullString
	var executedGtidSet sql.NullString

	err := instance.QueryRow(showSql).Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet)
	if err != nil {
		return fmt.Errorf("失败. 获取实例 binlog 位点信息(查询sql) %v:%v %v", host, port, err)
	}

	// 设置binlog位点信息
	if file.Valid && position.Valid && strings.TrimSpace(file.String) != "" && int(position.Int64) > 0 {
		this.StartLogFile = file.String
		this.StartLogPos = int(position.Int64)
		return nil
	}

	return fmt.Errorf("失败. 没有获得到binlog位点信息. %v:%v", host, port)
}
