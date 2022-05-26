package mysqlchecksum

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/daiguadaidai/go-d-bus/parser"
	"github.com/daiguadaidai/go-d-bus/service/mysqlrowcopy"
	"github.com/juju/errors"
	"github.com/outbrain/golib/log"
	"sync"
	"syscall"
	"time"
)

// 对MySQL数据进行校验
type Checksum struct {
	Parser    *parser.RunParser
	ConfigMap *config.ConfigMap

	ChecksumRowsChan     chan *matemap.PrimaryRangeValue // 进行多行校验的chan
	NotifySecondChecksum chan bool
	FixDiffRecordChan    chan model.DataChecksum // 传输fix数据的chan

	NeedFixRecordCounter        int
	NeedFixRecordCounterRWMutex sync.RWMutex
}

/* 创建一个 row Copy 对象
Params
	_parser: 命令行解析的信息
	_configMap: 配置信息
	_wg: 并发控制参数
	_checksumRowsChan: row copy 完成通知 checksum 的 checksum chan
	_notifySecondChecksum: 通知可以进行第二次checksum
*/
func NewChecksum(
	_parser *parser.RunParser,
	_configMap *config.ConfigMap,
	_checksumRowsChan chan *matemap.PrimaryRangeValue,
	_nodifySecondChecksum chan bool,
) (*Checksum, error) {

	checksum := new(Checksum)

	checksum.Parser = _parser
	checksum.ConfigMap = _configMap

	checksum.ChecksumRowsChan = _checksumRowsChan
	checksum.NotifySecondChecksum = _nodifySecondChecksum // 初始化通知可以进行第二次checksum

	// 初始化传输fix数据的通道
	checksum.FixDiffRecordChan = make(chan model.DataChecksum, 1000)

	return checksum, nil
}

func (this *Checksum) Start() {
	wg := new(sync.WaitGroup)

	// 并发进行第一波的checksum
	for parallerTag := 0; parallerTag < this.Parser.ChecksumParaller; parallerTag++ {
		wg.Add(1)
		go this.LoopFirstChecksum(wg, parallerTag)
	}

	// 获取二次校验需要的数据
	wg.Add(1)
	go this.EmitDiffRecords(wg)

	// 并发修复校验数据
	for parallerTag := 0; parallerTag < this.Parser.ChecksumFixParaller; parallerTag++ {
		wg.Add(1)
		go this.LoopFixDiffRows(wg, parallerTag)
	}

	wg.Wait()

	log.Infof("%v: !!!!!!!!!!!!! checksum任务总体完成 !!!!!!!!!!!!!", common.CurrLine())
}

/* 循环进行checksum校验
_parallerTag: 协程编号
*/
func (this *Checksum) LoopFirstChecksum(wg *sync.WaitGroup, _parallerTag int) {
	defer wg.Done()

	log.Infof("%v: 开始进行第一波数据校验, 启动协程%v", common.CurrLine(), _parallerTag)

	for primaryRangeValue := range this.ChecksumRowsChan {
		isError := false
		for i := 0; i < this.Parser.ErrRetryCount; i++ {
			is_consistent, err := this.RowsChecksum(primaryRangeValue, _parallerTag)
			if err != nil {
				log.Errorf("%v: %v", common.CurrLine(), err)
				time.Sleep(time.Second)
				continue
			}

			// 有不一致的情况就记录数据库
			if !is_consistent {
				// 保存数据不一致, 会再次进行检测
				saveDiffRecordError := false
				for j := 0; j < this.Parser.ErrRetryCount; j++ {
					err = CreateDiffRecord(this.ConfigMap.TaskUUID, primaryRangeValue)
					if err != nil {
						log.Errorf("%v. checksum 协程%v. %v", common.CurrLine(), _parallerTag, err)
						saveDiffRecordError = true
						time.Sleep(time.Second)
						continue
					}

					saveDiffRecordError = false
					break
				}
				// 如果保存不一致数据失败, 并达到上线, 退出程序
				if saveDiffRecordError {
					log.Errorf("%v: 错误, 进行checksum错误(发现多上数据不一致, 并且保存到数据库失败), 并且重试次数已经达到上线:%v. 将退出迁移. %v",
						common.CurrLine(), this.Parser.ErrRetryCount, this.Parser.TaskUUID)
					syscall.Exit(1)
				}
			} // 结束保存不一致信息

			isError = false
			break
		} // 结束 checksum不一致比较

		if isError {
			// 只有发生了错误, 并且重试了指定的次数还是有错, 才会走到这一步. 直接就退出迁移了
			log.Errorf("%v: 错误, 进行多行数据checksum错误(第一波), 并且重试次数已经达到上线:%v. 将退出迁移. %v",
				common.CurrLine(), this.Parser.ErrRetryCount, this.Parser.TaskUUID)
			syscall.Exit(1)
		}
	}
}

/* 进行多行checksum
Params:
	_primaryRangeValue: 数据范围
	_parallerTag: 第几个协程
Return:
    1. 是否一致
    2. 错误
*/
func (this *Checksum) RowsChecksum(
	_primaryRangeValue *matemap.PrimaryRangeValue,
	_parallerTag int,
) (bool, error) {

	// 获取需要迁移的表
	table, err := matemap.GetMigrationTableBySchemaTable(_primaryRangeValue.Schema, _primaryRangeValue.Table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 执行多行数据checksum 协程%v. 获取需要迁移的表失败. %v",
			common.CurrLine(), _parallerTag, err)
		return false, errors.New(errMSG)
	}

	// 1. 在源实例上获取数据的 checksum 值
	sourceChecksumCode, err := GetSourceRowsChecksumCode(this.ConfigMap.Source.Host.String,
		int(this.ConfigMap.Source.Port.Int64), _primaryRangeValue, table)
	if err != nil {
		errMSG := fmt.Sprintf("%v. checksum 协程%v. %v", common.CurrLine(), _parallerTag, err)
		return false, errors.New(errMSG)
	}

	// 2. 在目标实例上获取数据的 checksum 值
	targetChecksumCode, err := GetTargetRowsChecksumCode(this.ConfigMap.Target.Host.String,
		int(this.ConfigMap.Target.Port.Int64), _primaryRangeValue, table)
	if err != nil {
		errMSG := fmt.Sprintf("%v. checksum 协程%v. %v", common.CurrLine(), _parallerTag, err)
		return false, errors.New(errMSG)
	}

	// 3. 比较 源和目标的值是否相等, 不相等则记录到数据
	if sourceChecksumCode != targetChecksumCode {
		log.Warningf("%v: checksum 协程%v. 多行数据校验, 发现不一致数据. "+
			"%v:%v. min:%v, max:%v",
			common.CurrLine(), _parallerTag, _primaryRangeValue.Schema, _primaryRangeValue.Table,
			_primaryRangeValue.MinValue, _primaryRangeValue.MaxValue)
		return false, nil
	} else {
		log.Infof("%v: 校验成功 checksum 协程%v. %v.%v. min:%v, max:%v",
			common.CurrLine(), _parallerTag, _primaryRangeValue.Schema, _primaryRangeValue.Table,
			_primaryRangeValue.MinValue, _primaryRangeValue.MaxValue)
	}

	return true, nil
}

// 从数据库中获取需要再次校验的数据
func (this *Checksum) EmitDiffRecords(wg *sync.WaitGroup) {
	defer wg.Done()

	// 一进来就进行一次判断 任务 row copy 是否完成
	firstCheckRowCopyComplete := make(chan bool, 2) // 容量设置成2为了防止程序hang在通道这边
	firstCheckRowCopyComplete <- true

	ticker := time.NewTicker(time.Second * 5) // 每分钟定时器
	defer ticker.Stop()

	// 判断是否能够直接进行row copy 操作
	// 判断有两种办法:
	// 1. 收到了 row copy 完成的通知
	// 2. 每分钟获取任务 row copy 是否完成的状态
CHECK_TASK_ROW_COPY_COMPLETE_LOOP:
	for {
		select {
		case <-firstCheckRowCopyComplete: // 一进来就检测一下 任务 row copy 是否完成
			if ok, _ := mysqlrowcopy.TaskRowCopyIsComplete(this.ConfigMap.TaskUUID); ok {
				log.Infof("%v: 成功, 检测到任务row copy已经完成(任务一开始). 可以开始进行获取二次检验数据",
					common.CurrLine())
				break CHECK_TASK_ROW_COPY_COMPLETE_LOOP
			}

		case <-ticker.C:
			if ok, _ := mysqlrowcopy.TaskRowCopyIsComplete(this.ConfigMap.TaskUUID); ok {
				log.Infof("%v: 成功, 检测到任务row copy已经完成(定时检测). 可以开始进行获取二次检验数据",
					common.CurrLine())
				break CHECK_TASK_ROW_COPY_COMPLETE_LOOP
			}

			break
		case <-this.NotifySecondChecksum: // row copy 完成通知开始进行第二波校验
			log.Infof("%v: 成功, 接收到任务row copy已经完成通知. 可以开始进行获取二次检验数据",
				common.CurrLine())
			break CHECK_TASK_ROW_COPY_COMPLETE_LOOP
		}
	}

	// 能到这里, 说明就能开始进行第二波获取所有的不一致数据了
checkSecondCheckSumLoop:
	for {
		select {
		case <-ticker.C: // 每60s进行检测一次看看是不是还有未修复的数据
			if this.GetNeedFixRecordCounter() > 0 {
				log.Warningf("%v: 还有需要修复的数据未完成. 60s 后再进行获取新的未修复数据",
					common.CurrLine())
				break
			}

			isError := false
			var records []model.DataChecksum
			var err error

			for i := 0; i < this.Parser.ErrRetryCount; i++ {
				// 1. 获取所有不一致数据
				records, err = FindNoFixDiffRecords(this.ConfigMap.TaskUUID)
				if err != nil {
					log.Errorf("%v: 失败, 获取没有修复的数据失败. %v",
						common.CurrLine(), err)
					isError = true
					time.Sleep(time.Second)
					continue
				}
				if len(records) == 0 {
					close(this.FixDiffRecordChan)
					break checkSecondCheckSumLoop
				}

				// 2. 将不一致数据记录发送通知
				for _, record := range records {
					this.FixDiffRecordChan <- record
					this.IncrNeedFixRecordCounter()
				}

				isError = false
				break
			}
			if isError {
				log.Errorf("%v: 获取未修复校验数据失败(已经达到重试上线值: %v). 退出迁移",
					common.CurrLine(), this.Parser.ErrRetryCount)
				syscall.Exit(1)
			}
		} // 结束一次发送需要 fix 的记录
	}
}

/* 进行修复不同的行
Params:
	_parallerTag: 并发标记
*/
func (this *Checksum) LoopFixDiffRows(wg *sync.WaitGroup, _parallerTag int) {
	defer wg.Done()

	log.Infof("%v: 开始修复数据, 启动协程%v", common.CurrLine(), _parallerTag)

	for diffRecord := range this.FixDiffRecordChan {
		isError := false

		// 进行再次数据校验已经修复
		for i := 0; i < this.Parser.ErrRetryCount; i++ {
			err := this.FixDiffRows(diffRecord, _parallerTag)
			if err != nil {
				log.Errorf("%v: %v", common.CurrLine(), err)
				isError = true
				time.Sleep(time.Second)
			}

			isError = false
			break
		}
		if isError {
			log.Errorf("%v: 错误, 进行checksum修复数据(再次checksum)错误, 并且重试次数已经达到上线:%v. 将退出迁移. %v",
				common.CurrLine(), this.Parser.ErrRetryCount, this.Parser.TaskUUID)
			syscall.Exit(1)
		}
	}
}

/* 通过不一致记录修复数据
Params:
	_diffRecord: 不一致数据,
	_parallerTag: 协程号
*/
func (this *Checksum) FixDiffRows(_diffRecord model.DataChecksum, _parallerTag int) error {
	// 获取需要迁移的表的元数据
	table, err := matemap.GetMigrationTableBySchemaTable(_diffRecord.SourceSchema.String,
		_diffRecord.SourceTable.String)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 失败. 获取目标需要迁移的表(修复数据). %v:%v. %v",
			common.CurrLine(), _diffRecord.SourceSchema.String, _diffRecord.SourceTable.String, err)
		return errors.New(errMSG)
	}

	// 1. 通过不一致记录生成, 需要进行修复的主键范围值
	primaryRangeValue, err := diffRecord2PrimaryRangeValue(_diffRecord, table)

	// 2. 再次比较范围数据是否一致
	is_consistent, err := this.RowsChecksum(primaryRangeValue, _parallerTag)
	if err != nil {
		errMSG := fmt.Sprintf("%v: 修复数据时. %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 3. 如果不一致就开始进行逐行的修复
	if !is_consistent {
		err := this.FixDiffRowsStepFix(primaryRangeValue, table, _parallerTag)
		if err != nil {
			errMSG := fmt.Sprintf("%v: %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}
	}

	// 标记该记录修复完成
	affected := TagDiffRecordFixed(_diffRecord.Id.Int64)
	if affected >= 1 {
		log.Infof("%v: 已经标记不一致数据修复完成. %v.%v. min: %v max: %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table,
			primaryRangeValue.MinValue, primaryRangeValue.MaxValue)
	} else {
		log.Warningf("%v: 标记不一致数据修复(未成功). %v.%v. min: %v max: %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table,
			primaryRangeValue.MinValue, primaryRangeValue.MaxValue)
	}
	this.DecrNeedFixRecordCounter()

	if !is_consistent {
		// 再次进行检测通道
		this.ChecksumRowsChan <- primaryRangeValue
	}

	return nil
}

/* 真正开始修复数据
Params:
	_primaryRangeValue: 修复的数据范围值
	_parallerTag: 并发标记
*/
func (this *Checksum) FixDiffRowsStepFix(
	_primaryRangeValue *matemap.PrimaryRangeValue,
	_table *matemap.Table,
	_parallerTag int,
) error {
	// 1. 获取源表id范围所有值
	// 获取源数据所有主键值
	rows, err := FindSourcePKRows(this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64),
		_primaryRangeValue, _table)
	if err != nil {
		errMSG := fmt.Sprintf("%v: %v", common.CurrLine(), err)
		return errors.New(errMSG)
	}

	// 2. 比较每一行的checksum数据
	for _, pkValues := range rows {
		// 获取源数据 checksum 值
		sourceCode, err := GetSourceRowChecksumCode(this.ConfigMap.Source.Host.String,
			int(this.ConfigMap.Source.Port.Int64), pkValues, _table)
		if err != nil {
			errMSG := fmt.Sprintf("%v: %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}
		// 获取目标数据 checksum 值
		targetCode, err := GetTargetRowChecksumCode(this.ConfigMap.Target.Host.String,
			int(this.ConfigMap.Target.Port.Int64), pkValues, _table)
		if err != nil {
			errMSG := fmt.Sprintf("%v: %v", common.CurrLine(), err)
			return errors.New(errMSG)
		}

		// 不一致的情况需要进行修复
		if sourceCode != targetCode {
			// 源没有数据, 目标有数据. 在目标端把数据删了
			if sourceCode == 0 && targetCode != 0 {
				err = DeleteTargetRow(this.ConfigMap.Target.Host.String,
					int(this.ConfigMap.Target.Port.Int64), pkValues, _table)
				if err != nil {
					errMSG := fmt.Sprintf("%v: 修复数据, 删除目标行失败. "+
						"%v.%v -> %v.%v. Primary: %v. %v",
						common.CurrLine(), _table.SourceSchema, _table.SourceName, _table.TargetSchema,
						_table.TargetName, pkValues, err)
					return errors.New(errMSG)
				}

				log.Warningf("%v: 数据不一致, 删除目标多余行 %v.%v -> %v.%v. Primary: %v",
					common.CurrLine(), _table.SourceSchema, _table.SourceName, _table.TargetSchema,
					_table.TargetName, pkValues)
			} else { // 其他情况变成replace into 语句直接在 目标段执行
				// 通过主键值对源表进行select操作
				sourceRow, err := GetSourceRowByPK(this.ConfigMap.Source.Host.String,
					int(this.ConfigMap.Source.Port.Int64), pkValues, _table)
				if err != nil {
					errMSG := fmt.Sprintf("%v: 数据不一致. 正在修复数据."+
						" 通过主键值获取源表数据失败. %v.%v. %v. %v",
						common.CurrLine(), _table.SourceSchema, _table.SourceName, pkValues, err)
					return errors.New(errMSG)
				}
				// 如没有数据, 可能是源表有delete操作. 就不用任何操作了, 本行就不修复了
				if sourceRow == nil || len(sourceRow) == 0 {
					log.Warningf("%v: 在修复数据准备替换目标数据是, 发现不能获取到源表数据." +
						"有可能是刚好碰到源表数据被删除. 本行数据库可以不用修复.")
					continue
				}

				// 对目标表进行 replace into 操作
				err = ReplaceTargetRow(this.ConfigMap.Target.Host.String,
					int(this.ConfigMap.Target.Port.Int64), sourceRow, _table)
				if err != nil {
					errMSG := fmt.Sprintf("%v: 数据不一致, 正在修复数据."+
						"对目标表进行Replace into时失败. %v.%v -> %v.%v. %v. %v",
						common.CurrLine(), _table.SourceSchema, _table.SourceName, _table.TargetSchema,
						_table.TargetName, pkValues, err)
					return errors.New(errMSG)
				}
				log.Warningf("%v: 数据不一致, 使用源数据替换目标数据行 %v.%v -> %v.%v. Primary: %v",
					common.CurrLine(), _table.SourceSchema, _table.SourceName, _table.TargetSchema,
					_table.TargetName, pkValues)
			}
		} // 完成单行不一致的修复
	}

	return nil
}
