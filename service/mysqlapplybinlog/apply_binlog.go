package mysqlapplybinlog

import (
	"context"
	"fmt"
	"github.com/cevaris/ordered_map"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/parser"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"
	"github.com/outbrain/golib/log"
	"math/rand"
	"sync"
	"syscall"
	"time"
)

const (
	APPLIED_MIN_VALUE_INDEX = iota // binlog应用到的最小位点
	APPLIED_MAX_VALUE_INDEX        // binlog应用到的最大位点
)

type ApplyBinlog struct {
	Parser    *parser.RunParser
	ConfigMap *config.ConfigMap

	Syncer *replication.BinlogSyncer // 解析的binlog

	/* 保存了每个需要迁移的有效表 源表名和目标表名
	    map{
		    "schema.table": {
		        "SourceSchema": "sourceSchema",
			    "SourceName": "sourceName",
			    "TargetSchema": "targetSchema",
			    "TargetName": "targetName",
		    }
		}
	*/
	MigrationTableNameMap map[string]*matemap.MigrationTableName
	// 还需要生成主键范围数据的表 map: {"schema.table": true}
	NeedApplyTableMap map[string]bool

	// 从解析到分配的binlog的通道
	Parse2DistributeChan chan *BinlogEventPos

	// 从分配到应用binlog的通道
	Distribute2ApplyChans []chan *BinlogRowInfo

	/* 已经解析完成了的binlog位点, 还需要消费的binlog
	map {
	     // key: event的 行数
		"1111111111111111111:mysql-bin.000000001:000001111111111": 1
	}
	*/
	NeedApplyBinlogMap             *ordered_map.OrderedMap
	AddOrDeleteNeedApplyBinlogChan chan *AddOrDeleteNeedApplyBinlog // 是添加还需要应用的binlog,还是减少还需要应用binlog的行数
	NeedApplyEventCount            int                              // 还需要应用的事件数
	NeedApplyEventCountRWMutex     *sync.RWMutex

	/* 保存了当前应用完最小的binlog位点和最大binlog位点
	map{
		0: *LogFilePos, // 应用到的最小位点
		1: *LogFilePos, // 应用到的最大位点
	}
	*/
	AppliedMinMaxLogPos map[int]*LogFilePos

	// 通知记录目标实例的位点信息 chan
	NotifySaveTargetLogFilePos chan bool

	// 应用 binlog 延时时间
	ParseTimestamp uint32

	ParsedLogFile string // 解析到的日志文件
	ParsedLogPos  int    // 解析到的位点
	StopLogFile   string // 停止的的日志文件
	StopLogPos    int    // 停止的的位点
}

/* 创建一个应用binlog
Params:
    _parser: 命令行解析的信息
    _configMap: 配置信息
    _wg: 并发控制参数
*/
func NewApplyBinlog(_parser *parser.RunParser, _configMap *config.ConfigMap) (*ApplyBinlog, error) {
	applyBinlog := new(ApplyBinlog)

	applyBinlog.ConfigMap = _configMap
	applyBinlog.Parser = _parser

	// 初始化 需要迁移的表名映射信息
	applyBinlog.MigrationTableNameMap = matemap.FindAllMigrationTableNameMap()
	log.Infof("%v: 成功. 初始化 apply binlog 所有迁移的表名. 包含了可以不用迁移的", common.CurrLine())

	// 初始化需要应用binlog的表
	applyBinlog.NeedApplyTableMap = make(map[string]bool)
	for key, _ := range applyBinlog.MigrationTableNameMap {
		applyBinlog.NeedApplyTableMap[key] = true
	}
	// 将heartbeat table 也添加入需要应用binlog的表中
	if heartbeatTable := common.FormatTableName(applyBinlog.Parser.HeartbeatSchema, applyBinlog.Parser.HeartbeatTable, ""); heartbeatTable == "" {
		log.Warning("%v: 警告. 没有指定心跳表, 解析binlog显示的进度, 会不准确, 但不影响. 解析和应用binlog", common.CurrLine())
	} else {
		applyBinlog.NeedApplyTableMap[heartbeatTable] = true
		log.Infof("%v: 成功. 添加心跳表到需要迁移的表集合中. %v", common.CurrLine(), heartbeatTable)
	}

	// 初始化解析binlog 到 分配binlog的
	applyBinlog.Parse2DistributeChan = make(chan *BinlogEventPos, _parser.ApplyBinlogHighWaterMark)

	// 初始化分配binlog到应用binlog的通道
	applyBinlog.Distribute2ApplyChans = make([]chan *BinlogRowInfo, _parser.ApplyBinlogParaller)
	for i := 0; i < _parser.ApplyBinlogParaller; i++ {
		applyBinlog.Distribute2ApplyChans[i] = make(chan *BinlogRowInfo, _parser.ApplyBinlogHighWaterMark)
	}
	// 初始化还需要应用到事件个数
	applyBinlog.NeedApplyEventCount = 0
	// 初始化操作还需要应用事件数的锁
	applyBinlog.NeedApplyEventCountRWMutex = new(sync.RWMutex)

	// 用于保存需要消费的binlog行数数量
	applyBinlog.NeedApplyBinlogMap = ordered_map.NewOrderedMap()

	// 初始化添加和减少需要应用binglog标签通道
	applyBinlog.AddOrDeleteNeedApplyBinlogChan = make(chan *AddOrDeleteNeedApplyBinlog, _parser.ApplyBinlogParaller*_parser.ApplyBinlogHighWaterMark)

	// 初始化通知记录目标实例的位点信息 chan
	applyBinlog.NotifySaveTargetLogFilePos = make(chan bool)

	// 初始化延时信息
	applyBinlog.ParseTimestamp = 0

	// 初始化已经应用到的binlog最大最小位点信息
	// AppliedMinMaxLogPos map[int]*LogFilePos
	minAppliedLogPos := NewLogFilePos(_parser.StartLogFile, _parser.StartLogPos)
	applyBinlog.AppliedMinMaxLogPos = make(map[int]*LogFilePos)
	applyBinlog.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX] = minAppliedLogPos
	applyBinlog.AppliedMinMaxLogPos[APPLIED_MAX_VALUE_INDEX] = minAppliedLogPos

	// 初始化解析到的位点信息
	applyBinlog.ParsedLogFile = _parser.StartLogFile
	applyBinlog.ParsedLogPos = _parser.StartLogPos
	applyBinlog.StopLogFile = _parser.StopLogFile
	applyBinlog.StopLogPos = _parser.StopLogPos

	// 初始化 Syncer
	applyBinlog.InitSyncer()

	return applyBinlog, nil
}

func (this *ApplyBinlog) InitSyncer() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(rand.Intn(10000)),
		Flavor:   "mysql",
		Host:     this.ConfigMap.Source.Host.String,
		Port:     uint16(this.ConfigMap.Source.Port.Int64),
		User:     this.ConfigMap.Source.UserName.String,
		Password: this.ConfigMap.Source.Password.String,
	}
	this.Syncer = replication.NewBinlogSyncer(cfg)
}

func (this *ApplyBinlog) Start() {
	wg := new(sync.WaitGroup)
	// 产生binlog event
	wg.Add(1)
	go this.ProduceEvent(wg)

	// 分配 binlog event 中每一行数据
	wg.Add(1)
	go this.DistributeEventRows(wg)

	// 并发应用每一行数据
	for i, _ := range this.Distribute2ApplyChans {
		wg.Add(1)
		go this.ConsumeEevetRows(wg, i)
	}

	// 循环记录应用进度
	wg.Add(1)
	go this.LoopSaveApplyBinlogProgress(wg)

	// 循环获取暂停位点信息
	wg.Add(1)
	go this.LoopGetAndSetStopLogFilePos(wg)

	// 循环设置目标show master status信息(回滚信息)
	wg.Add(1)
	go this.LoopSaveTargetLogFilePos(wg)

	wg.Wait()

	log.Infof("%v: !!!!!!!!!!!!! 整个应用binlog完成 !!!!!!!!!!!!!", common.CurrLine())
}

// 开始产生binlog event
func (this *ApplyBinlog) ProduceEvent(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Infof("%v: 开始解析binlog. 开始位点 = 解析为点 = 应用到位点: %v:%v",
		common.CurrLine(), this.Parser.StartLogFile, this.Parser.StartLogPos)
	// 保存一下开始位点信息
	UpdateSourceLogPosInfo(
		this.ConfigMap.TaskUUID,
		this.Parser.StartLogFile,
		this.Parser.StartLogPos,
		this.Parser.StartLogFile,
		this.Parser.StartLogPos,
		this.Parser.StartLogFile,
		this.Parser.StartLogPos,
		this.StopLogFile,
		this.StopLogPos,
	)

	// 初始化位点
	position := mysql.Position{
		Name: this.Parser.StartLogFile,
		Pos:  uint32(this.Parser.StartLogPos),
	}

	// 生成解析binglog 工具
	streamer, err := this.Syncer.StartSync(position)
	if err != nil {
		log.Errorf("%v: 错误. 开始binlog发生错误. %v. 退出迁移.", common.CurrLine(), err)
		syscall.Exit(1)
	}

	// 初始化binlog文件
	logFile := this.Parser.StartLogFile
	produceErrCNT := 0

	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			log.Errorf("%v: 错误. 获取binlog event出错. %v", common.CurrLine(), err)
			syscall.Exit(1)
		}
		this.ParseTimestamp = ev.Header.Timestamp // 设置当前binlog解析到的事件点
		this.ParsedLogPos = int(ev.Header.LogPos) // 设置解析到的位点信息

		// 判断是否有设置 停止位点信息. 和解析位点是否大于停止位点. 是的化则不进行binlog应用
		for this.IsStopParseBinlogByStopLogFilePos() {
			log.Warningf("%v: 检测到有设置停止位点信息. 并且解析到的位点(%v:%v) >= 停止位点(%v:%v). 该迁移任务将停止解析binlog",
				common.CurrLine(), this.ParsedLogFile, this.ParsedLogPos, this.StopLogFile, this.StopLogPos)

			time.Sleep(time.Second * 10)

			continue
		}

		if err != nil {
			produceErrCNT++
			if produceErrCNT > this.Parser.ErrRetryCount {
				log.Errorf("%v: 错误. 获取binlog event. 达到上线 %v 次. 将退出迁移. %v", common.CurrLine(), produceErrCNT, err)

				syscall.Exit(1)
			}
			log.Errorf("%v: 错误. 获取binlog event. 第 %v 次. %v",
				common.CurrLine(), produceErrCNT, err)

			time.Sleep(time.Second)
			continue
		} else {
			if produceErrCNT != 0 {
				produceErrCNT = 0
			}
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent: // 更新在解析的binlog文件
			logFile = string(e.NextLogName)
			this.ParsedLogFile = logFile

		case *replication.TableMapEvent:
			schemaName := string(e.Schema)
			tableName := string(e.Table)

			// 只需要处理需要应用的表
			if this.IsApplyTable(schemaName, tableName) {
				table, err := matemap.GetMigrationTableBySchemaTable(schemaName, tableName)
				if err != nil {
					log.Errorf("%v: 在解析binlog使用TableMapEvent时, 获取需要迁移的表的元数据出错. %v.%v %v, 退出迁移",
						common.CurrLine(), schemaName, tableName, err)
					syscall.Exit(1)
				}

				if len(table.SourceColumns) != int(e.ColumnCount) {
					log.Warningf("%v: 警告. 发现表字段有变化. 可能是有进行DDL. 重新生成该表元数据. %v:%v, 字段数 %v -> %v",
						common.CurrLine(), schemaName, tableName, len(table.SourceColumns), int(e.ColumnCount))

					// 等待binlog应用完成后, 替换表元数据信息
					this.WaitingApplyEventAndReplaceTableMap(schemaName, tableName)
				}
			}
		case *replication.RowsEvent:
			schema := string(e.Table.Schema)
			table := string(e.Table.Table)

			// 只需要处理需要应用的表
			if this.IsApplyTable(schema, table) {
				binlogEventPos := NewBinlogEventPos(ev, logFile, int(ev.Header.LogPos), -1)
				this.Parse2DistributeChan <- binlogEventPos
			}
		}
	}
}

// 读取每个event, 并且分配每一行
func (this *ApplyBinlog) DistributeEventRows(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Infof("%v: 开始分配事件的每一行", common.CurrLine())

	// 循环获取binglog事件
	for binlogEventPos := range this.Parse2DistributeChan {
		errCNT := 0

		for {
			switch binlogEventPos.BinlogEvent.Header.EventType {
			// insert 事件
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				// 分配 insert 事件的每一行
				err := this.DistributeInsertEventRows(binlogEventPos)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 错误次数达到上线 %v 将退出迁移. %v", common.CurrLine(), errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 第%v次错误. %v", common.CurrLine(), err)
					continue
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				// update 事件
				err := this.DistributeUpdateEventRows(binlogEventPos)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 错误次数达到上线 %v 将退出迁移. %v", common.CurrLine(), errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 第%v次错误. %v", common.CurrLine(), err)
					continue
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				// delete 事件
				err := this.DistributeDeleteEventRows(binlogEventPos)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 错误次数达到上线 %v 将退出迁移. %v", common.CurrLine(), errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 第%v次错误. %v", common.CurrLine(), err)
					continue
				}
			}

			break
		}
	}
}

/*分配 insert 事件的每一行
Params:
	_binlogEventPos: 自己封装过的 binlog 事件
*/
func (this *ApplyBinlog) DistributeInsertEventRows(_binlogEventPos *BinlogEventPos) error {
	rowEvent := _binlogEventPos.BinlogEvent.Event.(*replication.RowsEvent)
	rowCount := len(rowEvent.Rows)
	schemaName := string(rowEvent.Table.Schema)
	TableName := string(rowEvent.Table.Table)
	table, err := matemap.GetMigrationTableBySchemaTable(schemaName, TableName)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取需要迁移的表(在分配insert行的时候), %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 还需要应用的事件数 加1
	this.IncrNeedApplyEventCount()

	// 添加该事件的行数
	addOrDeleteNeedApplyBinlog := NewAddOrDeleteNeedApplyBinlog(
		_binlogEventPos.GetLogFilePosTimeStamp(),
		AODNAB_TYPE_ADD,
		rowCount,
	)
	this.AddOrDeleteNeedApplyBinlogChan <- addOrDeleteNeedApplyBinlog

	for _, row := range rowEvent.Rows {
		// 新建每一行数据
		binlogRowInfo := NewBinlogRowInfo(
			schemaName,
			TableName,
			row,
			row,
			_binlogEventPos.BinlogEvent.Header.EventType,
			_binlogEventPos.GetLogFilePosTimeStamp(),
		)

		// 获取该行应该应该放入那个chan
		slot := binlogRowInfo.GetChanSlotByAfter(table.SourcePKColumns, this.Parser.ApplyBinlogParaller)
		this.Distribute2ApplyChans[slot] <- binlogRowInfo
	}

	return nil
}

/*分配 update 事件的每一行
Params:
	_binlogEventPos: 自己封装过的 binlog 事件
*/
func (this *ApplyBinlog) DistributeUpdateEventRows(_binlogEventPos *BinlogEventPos) error {
	rowEvent := _binlogEventPos.BinlogEvent.Event.(*replication.RowsEvent)
	rowCount := len(rowEvent.Rows) / 2 // update slice中 偶数是前镜像, 基数是后镜像
	schemaName := string(rowEvent.Table.Schema)
	TableName := string(rowEvent.Table.Table)
	table, err := matemap.GetMigrationTableBySchemaTable(schemaName, TableName)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取需要迁移的表(在分配update行的时候), %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 还需要应用的事件数 加1
	this.IncrNeedApplyEventCount()

	// 添加该事件的行数
	addOrDeleteNeedApplyBinlog := NewAddOrDeleteNeedApplyBinlog(_binlogEventPos.GetLogFilePosTimeStamp(), AODNAB_TYPE_ADD, rowCount)
	this.AddOrDeleteNeedApplyBinlogChan <- addOrDeleteNeedApplyBinlog

	for i := 0; i < rowCount; i++ {
		beforeIndex := i * 2          // 前镜像 index
		afterIndex := beforeIndex + 1 // 后镜像 index

		// 新建每一行数据
		binlogRowInfo := NewBinlogRowInfo(
			schemaName,
			TableName,
			rowEvent.Rows[beforeIndex],
			rowEvent.Rows[afterIndex],
			_binlogEventPos.BinlogEvent.Header.EventType,
			_binlogEventPos.GetLogFilePosTimeStamp(),
		)

		// 获取该行应该应该放入那个chan
		slot := binlogRowInfo.GetChanSlotByBefore(table.SourcePKColumns, this.Parser.ApplyBinlogParaller)
		this.Distribute2ApplyChans[slot] <- binlogRowInfo
	}

	return nil
}

/*分配 delete 事件的每一行
Params:
	_binlogEventPos: 自己封装过的 binlog 事件
*/
func (this *ApplyBinlog) DistributeDeleteEventRows(_binlogEventPos *BinlogEventPos) error {
	rowEvent := _binlogEventPos.BinlogEvent.Event.(*replication.RowsEvent)
	rowCount := len(rowEvent.Rows)
	schemaName := string(rowEvent.Table.Schema)
	TableName := string(rowEvent.Table.Table)
	table, err := matemap.GetMigrationTableBySchemaTable(schemaName, TableName)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取需要迁移的表(在分配delete行的时候), %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 还需要应用的事件数 加1
	this.IncrNeedApplyEventCount()

	// 添加该事件的行数
	addOrDeleteNeedApplyBinlog := NewAddOrDeleteNeedApplyBinlog(
		_binlogEventPos.GetLogFilePosTimeStamp(),
		AODNAB_TYPE_ADD,
		rowCount,
	)
	this.AddOrDeleteNeedApplyBinlogChan <- addOrDeleteNeedApplyBinlog

	for _, row := range rowEvent.Rows {
		// 新建每一行数据
		binlogRowInfo := NewBinlogRowInfo(
			schemaName,
			TableName,
			row,
			row,
			_binlogEventPos.BinlogEvent.Header.EventType,
			_binlogEventPos.GetLogFilePosTimeStamp(),
		)

		// 获取该行应该应该放入那个chan
		slot := binlogRowInfo.GetChanSlotByAfter(table.SourcePKColumns, this.Parser.ApplyBinlogParaller)
		this.Distribute2ApplyChans[slot] <- binlogRowInfo
	}

	return nil
}

/* 消费事件 每一行数据
Params:
	_slot: 通道号
*/
func (this *ApplyBinlog) ConsumeEevetRows(wg *sync.WaitGroup, _slot int) {
	defer wg.Done()
	log.Infof("%v: 协程%v. 开始应用每一行.", common.CurrLine(), _slot)

	for binlogRowInfo := range this.Distribute2ApplyChans[_slot] {
		errCNT := 0
		for {
			switch binlogRowInfo.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				err := this.ConsumeInsertRows(binlogRowInfo)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 协程%v. 发生错误超过上线: %v次. 退出迁移. %v",
							common.CurrLine(), _slot, errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 协程%v. 应用数据错误, 第%v次错误. %v",
						common.CurrLine(), _slot, errCNT, err)
					time.Sleep(time.Second)
					continue
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				err := this.ConsumeUpdateRows(binlogRowInfo)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 协程%v. 发生错误超过上线: %v次. 退出迁移. %v",
							common.CurrLine(), _slot, errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 协程%v. 应用数据错误, 第%v次错误. %v",
						common.CurrLine(), _slot, errCNT, err)
					time.Sleep(time.Second)
					continue
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				err := this.ConsumeDeleteRows(binlogRowInfo)
				if err != nil {
					errCNT++
					if errCNT > this.Parser.ErrRetryCount {
						log.Errorf("%v: 协程%v. 发生错误超过上线: %v次. 退出迁移. %v",
							common.CurrLine(), _slot, errCNT, err)
						syscall.Exit(1)
					}
					log.Errorf("%v: 协程%v. 应用数据错误, 第%v次错误. %v",
						common.CurrLine(), _slot, errCNT, err)
					time.Sleep(time.Second)
					continue
				}
			}

			// 减少该事件的行数
			addOrDeleteNeedApplyBinlog := NewAddOrDeleteNeedApplyBinlog(
				binlogRowInfo.ApplyRowKey,
				AODNAB_TYPE_DELETE,
				1,
			)
			this.AddOrDeleteNeedApplyBinlogChan <- addOrDeleteNeedApplyBinlog

			break
		}
	}
}

/* 消费insert行
Params:
	_binlogRowInfo: 相关行数据信息
*/
func (this *ApplyBinlog) ConsumeInsertRows(_binlogRowInfo *BinlogRowInfo) error {
	// 获取需要迁移的表的元信息
	table, err := matemap.GetMigrationTableBySchemaTable(_binlogRowInfo.Schema, _binlogRowInfo.Table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 获取迁移的表失败(应用行insert). %v",
			common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 需要用于repalce into 的数据
	afterRow := _binlogRowInfo.GetAfterRow(table.SourceUsefulColumns)

	// 获取数据库并且执行 REPLACE INTO SQL
	instance, ok := gdbc.GetDynamicDBByHostPort(this.ConfigMap.Target.Host.String, this.ConfigMap.Target.Port.Int64)
	if !ok {
		return fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取目标数据库实例出错", common.CurrLine(), this.ConfigMap.Target.Host.String, this.ConfigMap.Target.Port.Int64)
	}

	// 开启事物执行sql
	_, err = instance.Exec(table.GetRepPerBatchSqlTpl(1), afterRow...)
	if err != nil {
		return err
	}

	return nil
}

/* 消费update行
Params:
	_binlogRowInfo: 相关行数据信息
*/
func (this *ApplyBinlog) ConsumeUpdateRows(_binlogRowInfo *BinlogRowInfo) error {
	// 获取需要迁移的表的元信息
	table, err := matemap.GetMigrationTableBySchemaTable(_binlogRowInfo.Schema, _binlogRowInfo.Table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 获取迁移的表失败(应用行insert). %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 如果唯一键有修改则变成 delete 和 insert 操作
	if _binlogRowInfo.IsDiffBeforeAndAfter(table.SourceAllUKColumns) {
		// 删除数据
		err := this.ConsumeDeleteRows(_binlogRowInfo)
		if err != nil {
			errMSG := fmt.Sprintf("%v: 消费update行, update 转化为 delete/insert(delete). %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}

		// 插入数据
		err = this.ConsumeInsertRows(_binlogRowInfo)
		if err != nil {
			errMSG := fmt.Sprintf("%v: 消费update行, update 转化为 delete/insert(insert). %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}
	} else { // 如果唯一键列值没有修改. 则变成insert
		// 插入数据
		err = this.ConsumeInsertRows(_binlogRowInfo)
		if err != nil {
			errMSG := fmt.Sprintf("%v: 消费update行, update 转化为 insert. %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}
	}

	return nil
}

/* 消费delete行
Params:
	_binlogRowInfo: 相关行数据信息
*/
func (this *ApplyBinlog) ConsumeDeleteRows(_binlogRowInfo *BinlogRowInfo) error {
	// 获取需要迁移的表的元信息
	table, err := matemap.GetMigrationTableBySchemaTable(_binlogRowInfo.Schema, _binlogRowInfo.Table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 获取迁移的表失败(应用行delete). %v",
			common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 需要用于repalce into 的数据
	beforeRow := _binlogRowInfo.GetBeforeRow(table.TargetPKColumns)

	// 获取数据库并且执行 REPLACE INTO SQL
	instance, ok := gdbc.GetDynamicDBByHostPort(this.ConfigMap.Target.Host.String, this.ConfigMap.Target.Port.Int64)
	if !ok {
		return fmt.Errorf("%v: 缓存中不存在该实例(%v:%v). 获取目标数据库实例出错", common.CurrLine(), this.ConfigMap.Target.Host.String, this.ConfigMap.Target.Port.Int64)
	}

	// 开启事物执行sql
	_, err = instance.Exec(table.GetDelSqlTpl(), beforeRow...)
	if err != nil {
		return err
	}

	return nil
}

func (this *ApplyBinlog) LoopSaveApplyBinlogProgress(wg *sync.WaitGroup) {
	defer wg.Done()

	saveDelayTicker := time.NewTicker(time.Second * 5)
	saveBinlogProgressTicker := time.NewTicker(time.Second * 5)
	tmpMinLogFilePos := LogFilePos{
		LogFile: "",
		LogPos:  -1,
	}

	for {
		select {
		case <-saveBinlogProgressTicker.C: // 保存应用binlog进度
			// 如果当前没有binlog event 需要应用, 将最小位点设置为最大位点.
			if this.GetNeedApplyEventCount() == 0 {
				this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX] = this.AppliedMinMaxLogPos[APPLIED_MAX_VALUE_INDEX]
			}

			// 保存当前位点进度信息
			UpdateSourceLogPosInfo(
				this.ConfigMap.TaskUUID,
				"", // 不进行更新
				-1, // 不进行更新
				this.ParsedLogFile,
				this.ParsedLogPos,
				this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogFile,
				this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogPos,
				"",
				-1,
			)

			log.Infof("%v: binlog 位点信息. 开始位点: %v:%v, 解析到位点: %v:%v, 应用到位点: %v:%v. %v",
				common.CurrLine(),
				this.Parser.StartLogFile,
				this.Parser.StartLogPos,
				this.ParsedLogFile,
				this.ParsedLogPos,
				this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogFile,
				this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogPos,
				this.ConfigMap.TaskUUID,
			)

			// 比较当前应用最小位点信息是否和临时位点信息相等. 如果不相等将通知. 收集目标实例 show master status 信息.
			// 并更新临时位点 为当前最小位点
			if tmpMinLogFilePos.LogFile != this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogFile ||
				tmpMinLogFilePos.LogPos != this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogPos {

				this.NotifySaveTargetLogFilePos <- true
				tmpMinLogFilePos.LogFile = this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogFile
				tmpMinLogFilePos.LogPos = this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX].LogPos
			}

		case <-saveDelayTicker.C:
			// 记录解析binlog延时信息
			currTimestamp := uint32(time.Now().Unix())
			log.Infof("%v: 当前延时为: %vs. 计算的是解析binlog的时间",
				common.CurrLine(), int(currTimestamp)-int(this.ParseTimestamp))

		case addOrDeleteNeedApplyBinlog := <-this.AddOrDeleteNeedApplyBinlogChan:
			switch addOrDeleteNeedApplyBinlog.Type {
			case AODNAB_TYPE_ADD: // 添加需要应用的binlog event标记
				this.NeedApplyBinlogMap.Set(addOrDeleteNeedApplyBinlog.Key, addOrDeleteNeedApplyBinlog.Num)

			case AODNAB_TYPE_DELETE: // 减少需要应用binlog event row 标记
				eventRowCountInterface, ok := this.NeedApplyBinlogMap.Get(addOrDeleteNeedApplyBinlog.Key)
				if !ok {
					log.Errorf("%v: 错误. 没有发现需要应用的binlog. %v",
						common.CurrLine(), addOrDeleteNeedApplyBinlog.Key)
					continue
				}
				eventRowCount := eventRowCountInterface.(int)
				eventRowCount -= addOrDeleteNeedApplyBinlog.Num

				// 如果binlog event中的每一个行都被应用了则从还需要应用的binlog中移除
				if eventRowCount == 0 {
					// 设置当前应用最小的 位点信息
					eventRowCountIter := this.NeedApplyBinlogMap.IterFunc()
					eventRowCountItem, ok := eventRowCountIter()
					if ok {
						minLogFilePos := NewLogFilePosByKey(eventRowCountItem.Key.(string))
						this.AppliedMinMaxLogPos[APPLIED_MIN_VALUE_INDEX] = minLogFilePos
					}

					// 如果该应用完成的位点比保存的最大位点大则, 将该位点设置为最大位点
					currLogFilePos := NewLogFilePosByKey(addOrDeleteNeedApplyBinlog.Key)
					maxLogFilePos, ok := this.AppliedMinMaxLogPos[APPLIED_MAX_VALUE_INDEX]
					if !ok { // 如果之前没有设置最大应用binlog位点, 将本次位点设置为最大的位点
						this.AppliedMinMaxLogPos[APPLIED_MAX_VALUE_INDEX] = currLogFilePos
					} else { // 如果有最大位点, 则比较位点大小, 选择最大
						if currLogFilePos.IsRatherThan(maxLogFilePos) {
							this.AppliedMinMaxLogPos[APPLIED_MAX_VALUE_INDEX] = currLogFilePos
						}
					}

					// 将需要还需要应用binlog event数减1
					this.DecrNeedApplyEventCount()

					// 该binlog 位点已经应用完毕, 可以清除
					this.NeedApplyBinlogMap.Delete(addOrDeleteNeedApplyBinlog.Key)
				} else { // 该binlog event中还有行没有被应用
					this.NeedApplyBinlogMap.Set(addOrDeleteNeedApplyBinlog.Key, eventRowCount)
				}

			}

		}
	}
}

// 重新去数据库中获取一下stop位点信息
func (this *ApplyBinlog) LoopGetAndSetStopLogFilePos(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		this.StopLogFile, this.StopLogPos = GetStopLogFilePos(this.Parser.TaskUUID)
		log.Infof("%v: 获取停止位点信息. %v:%v",
			common.CurrLine(), this.StopLogFile, this.StopLogPos)
		time.Sleep(time.Second * 30)
	}
}

// 循环获取目标实例的master位点信息
func (this *ApplyBinlog) LoopSaveTargetLogFilePos(wg *sync.WaitGroup) {
	defer wg.Done()

	for _ = range this.NotifySaveTargetLogFilePos {
		// 获取 show master status 信息
		logFile, logPos, err := ShowMasterStatus(this.ConfigMap.Target.Host.String,
			int(this.ConfigMap.Target.Port.Int64))
		if err != nil {
			log.Errorf("%v: 错误. 获取目标数据库 show master status 值 失败. %v",
				common.CurrLine(), err)
		}

		// 保存目标数据库 show master status 位点信息
		UpdateTargetLogFilePos(this.ConfigMap.TaskUUID, logFile, logPos)
		log.Infof("%v: 更新目标实例位点信息(回滚问点)成功. %v:%v",
			common.CurrLine(), logFile, logPos)
	}
}
