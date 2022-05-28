package mysqlrowcopy

import (
	"fmt"
	"github.com/cevaris/ordered_map"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/parser"
	"github.com/juju/errors"
	"github.com/outbrain/golib/log"
	"go.uber.org/atomic"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
)

type RowCopy struct {
	Parser    *parser.RunParser
	ConfigMap *config.ConfigMap

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

	// 传输表的 主键 范围值
	PrimaryRangeValueChan chan *matemap.PrimaryRangeValue

	// 还需要生成主键范围数据的表 map: {"schema.table": true}
	NeedRowCopyTableMap map[string]bool

	// 每个表当前生成生成到的主键范围 map: {"schema.table": PrimaryRangeValue}
	CurrentPrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue

	// 每个表已经完成的主键范围值 sync.map: {"schema.table": PrimaryRangeValue}
	CompletePrimaryRangeValueMap sync.Map

	// 每个表最大的主键范围值, rowCopy截止的id范围 map: {"schema.table": PrimaryRangeValue}
	MaxPrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue

	/* 等待确认消费的 主键范围值, 确认消费一个就在其中删除一个
	   map: {
	       "schema.table": {
	           "timestampHash": PrimaryRangeValue,
	           "timestampHash": PrimaryRangeValue,
	       }
	   }
	*/
	WaitingTagCompletePrirmaryRangeValueMap map[string]*ordered_map.OrderedMap
	// 用于传输删除还是添加 需要标记完成的 PrimaryRangeValue
	AddOrDelWatingTagCompleteChan chan *AddOrDelete

	// RowCopy消费并发数
	RowCopyComsumerCount      *atomic.Int64
	RowCopyConsumerCountMutex sync.RWMutex
	RowCopyConsumeMinMaxValue map[string]sync.Map // 用于保存当前已经消费的最小的和最大的主键消费值

	CloseSaveRowCopyProgressChan chan bool // 用来通知是否需要关闭保存row copy进度协程

	RowCopyNoComsumeTimes map[string]int // 每个表行拷贝还有几个没有消费

	ToChecksumChan chan *matemap.PrimaryRangeValue // row copy 完成后通知checksum

	// 当所有的 row copy 完成通知可以进行二次checksum
	// 第一次checksum是每一次rowcopy完都进行, 如果发生了数据不一致,
	// 会在最后所有的rowcopy完成后再次对第一次不一致的进行checksum操作
	NotifySecondChecksum chan bool
}

/* 创建一个 row Copy 对象
Params
    _parser: 命令行解析的信息
    _configMap: 配置信息
    _wg: 并发控制参数
	_toChecksumChan: row copy 完成通知 checksum 的 checksum chan
*/
func NewRowCopy(
	parser *parser.RunParser,
	configMap *config.ConfigMap,
	toChecksumChan chan *matemap.PrimaryRangeValue,
	notifySecondChecksum chan bool,
) (*RowCopy, error) {

	rowCopy := new(RowCopy)

	// 初始化配置控制信息
	rowCopy.ConfigMap = configMap
	rowCopy.Parser = parser
	rowCopy.RowCopyComsumerCount = atomic.NewInt64(0)

	// 初始化 需要迁移的表名映射信息
	rowCopy.MigrationTableNameMap = matemap.FindAllMigrationTableNameMap()
	log.Infof("%v: 成功. 初始化 row copy 所有迁移的表名. 包含了可以不用迁移的", common.CurrLine())

	// 初始化 传输表的 主键 范围值
	rowCopy.PrimaryRangeValueChan = make(chan *matemap.PrimaryRangeValue, parser.RowCopyHighWaterMark)
	log.Infof("%v: 成功. 初始化传输 row copy 主键值的 通道", common.CurrLine())

	// 获取 还需要生成主键范围数据的表 map: {"schema.table": true}
	needRowCopyTableMap, err := rowCopy.GetNeedGetPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.NeedRowCopyTableMap = needRowCopyTableMap
	log.Infof("%v: 成功. 初始化还需要迁移的表: %v", common.CurrLine(), needRowCopyTableMap)

	// 初始化每个表最大的主键范围值, rowCopy截止的id范围 map: {"schema.table": PrimaryRangeValue}
	// MaxPrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue
	maxPrimaryRangeValueMap, maxNoDataTables, err := rowCopy.GetMaxPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.MaxPrimaryRangeValueMap = maxPrimaryRangeValueMap
	rowCopy.TagCompleteNeedRowCopyTables(maxNoDataTables) // 将没数据的表直接标记完成
	log.Infof("%v: 成功. 初始化需要迁移的表row copy的截止主键值. 没有数据的表: %v", common.CurrLine(), maxNoDataTables)

	// 每个表当前生成生成到的主键范围 map: {"schema.table": PrimaryRangeValue}
	currentPrimaryRangeValueMap, currNoDataTables, err := rowCopy.GetCurrentPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.CurrentPrimaryRangeValueMap = currentPrimaryRangeValueMap
	rowCopy.TagCompleteNeedRowCopyTables(currNoDataTables) // 将没数据的表直接标记完成
	log.Infof("%v: 成功. 初始化需要迁移的表当前row copy到的主键值. 没有数据的表: %v", common.CurrLine(), currNoDataTables)

	// 如果表的当前 row copy 到的主键值 >= 表 row copy 截止的 主键值
	greaterTables := rowCopy.FindCurrGreaterMaxPrimaryTables()
	rowCopy.TagCompleteNeedRowCopyTables(greaterTables) // 将当前rowcopy的值 >= 截止的rowcopy表直接标记完成
	log.Infof("%v: 成功. 过滤需要迁移的表中 当前ID >= 截止ID 的表有: %v", common.CurrLine(), greaterTables)

	// 初始化每个表已经完成到的主键范围值 map: {"schema.table": PrimaryRangeValue}
	// 初始化的时候 已经完成的row copy 范围和 当前需要进行 row copy 的是一样的
	for key, value := range rowCopy.CurrentPrimaryRangeValueMap {
		rowCopy.CompletePrimaryRangeValueMap.Store(key, value)
	}
	log.Infof("%v: 成功. 初始化已经完成的", common.CurrLine())

	// 初始化 cache row copy 的主键
	rowCopy.WaitingTagCompletePrirmaryRangeValueMap = make(map[string]*ordered_map.OrderedMap)
	for tableName, _ := range rowCopy.NeedRowCopyTableMap {
		rowCopy.WaitingTagCompletePrirmaryRangeValueMap[tableName] = ordered_map.NewOrderedMap()
	}

	// 初始化 每个表当前消费的最小 和最大 的主键值变量
	rowCopy.RowCopyConsumeMinMaxValue = make(map[string]sync.Map)
	for tableName, _ := range rowCopy.NeedRowCopyTableMap {
		rowCopy.RowCopyConsumeMinMaxValue[tableName] = sync.Map{}
	}

	// 初始化用于传输删除还是添加 需要标记完成的 PrimaryRangeValue
	rowCopy.AddOrDelWatingTagCompleteChan = make(chan *AddOrDelete, 2*parser.RowCopyHighWaterMark)
	log.Infof("%v: 成功. 初始化添加还是删除需要标记id值的通道", common.CurrLine())

	// 初始化通知关闭保存 row copy 进度的协程  的通道
	rowCopy.CloseSaveRowCopyProgressChan = make(chan bool)

	// 初始化表还有几个row copy没有消费
	rowCopy.RowCopyNoComsumeTimes = make(map[string]int)
	for tableName, _ := range rowCopy.NeedRowCopyTableMap {
		rowCopy.RowCopyNoComsumeTimes[tableName] = 0
	}

	// 初始化通知checksum通道
	rowCopy.ToChecksumChan = toChecksumChan

	// 初始化row copy 完成通知checksum进行二次checksum
	rowCopy.NotifySecondChecksum = notifySecondChecksum

	return rowCopy, nil
}

// 开始进行row copy
func (this *RowCopy) Start() {
	defer func() {
		close(this.ToChecksumChan)
		log.Infof("%v: row copy 完成, 关闭 checksum 接受通道", common.CurrLine())
		this.NotifySecondChecksum <- true // 通知checksum可以进行二次校验了
		log.Infof("%v: row copy 完成, 通知可以进行二次校验了", common.CurrLine())

		log.Infof("%v, !!!!!!!!!!!!! 整个row copy完成. !!!!!!!!!!!!", common.CurrLine())
	}()

	isComplete, err := TaskRowCopyIsComplete(this.ConfigMap.TaskUUID)
	if err != nil {
		log.Errorf("%v: 失败. 获取任务 row copy 是否完成失败. 将不进行row copy行为. %v. %v", common.CurrLine(), this.ConfigMap.TaskUUID, err)
		return
	}
	if isComplete {
		log.Warningf("%v: 警告. row copy 任务已经完成. 不需要进行row copy 操作. %v", common.CurrLine(), this.ConfigMap.TaskUUID)
		return
	}

	wg := new(sync.WaitGroup)

	// 循环生成 row copy 需要的主键值, 并将值放入通道中PrimaryRangeValueChan
	wg.Add(1)
	go this.LoopGeneratePrimaryRangeValue(wg)

	// 消费 PrimaryRangeValueChan 通道中的主键方位值
	log.Infof("%v: 设置了 %v 个并发执行 row copy 操作.", common.CurrLine(), this.Parser.RowCopyParaller)
	this.RowCopyComsumerCount.Add(int64(this.Parser.RowCopyParaller)) // 记录当前有多少个 row copy 并发
	for parallerTag := 0; parallerTag < this.Parser.RowCopyParaller; parallerTag++ {
		wg.Add(1)
		go this.LoopConsumePrimaryRangeValue(wg, parallerTag)
	}

	// 循环, 缓存和删除主键值
	wg.Add(1)
	go this.LoopAddOrDeleteCache(wg)

	// 循环保存 row copy 进度
	wg.Add(1)
	go this.LoopSaveRowCopyProgress(wg)

	wg.Wait()
}

// 循环生成主键值
func (this *RowCopy) LoopGeneratePrimaryRangeValue(wg *sync.WaitGroup) {
	defer func() {
		close(this.PrimaryRangeValueChan)
		wg.Done()
	}()

	// 当前错误重试次数
	errRetryCount := 0

	for {
		if errRetryCount > this.Parser.ErrRetryCount {
			log.Errorf("%v: 错误. row copy 生成主键值发生错误, 并且超过重试上线值: %v. 将退出生成主键值.", common.CurrLine(), this.Parser.ErrRetryCount)
			return
		}

		ok, err := this.GeneratePrimaryRangeValue()
		if err != nil {
			errRetryCount++
			log.Errorf("%v: 错误. 第 %v 次, 允许重试次数: %v. %v", common.CurrLine(), errRetryCount, this.Parser.ErrRetryCount, err)
			continue
		}
		if ok { // 已经完成所有的生成生成主键值
			break
		}

		errRetryCount = 0
	}

}

/* 随机生成一个表的主键范围值
1. 随机生成id
2. 将id放入PrimaryRangeValueChan中
3. 设置CurrentPrimaryValueMap的值为当前
Return:
    是否都完成了 row copy
*/
func (this *RowCopy) GeneratePrimaryRangeValue() (bool, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("%v: 错误. 生成 row copy 主键值发生错误. %v. %v", common.CurrLine(), err, string(debug.Stack()))

			syscall.Exit(1)
		}
	}()

	tableName, ok := common.GetRandomMapKey(this.NeedRowCopyTableMap)
	if !ok { // 所有的表的 row copy 主键值范围数据都已经生成完了
		log.Infof("%v: 所有表的主键值已经生成完. 退出生成主键值的协程 %v %v:%v",
			common.CurrLine(), this.ConfigMap.TaskUUID, this.ConfigMap.Source.Host.String, this.ConfigMap.Source.Port.Int64,
		)

		return true, nil
	}

	// 获取该表当前的 row copy 主键值
	currPrimaryRangeValue := this.CurrentPrimaryRangeValueMap[tableName]
	// 获取表的下一个主键方位值
	nextPrimaryRangeValue, err := currPrimaryRangeValue.GetNextPrimaryRangeValue(
		this.Parser.RowCopyLimit,
		this.ConfigMap.Source.Host.String,
		int(this.ConfigMap.Source.Port.Int64))
	if err != nil {
		errMSG := fmt.Sprintf("%v: row copy 生成表的下一个主键值失败. 停止产生相关表主键值. %v. %v", common.CurrLine(), tableName, err)

		return false, errors.New(errMSG)
	}
	log.Infof("%v: 成功. 生成主键ID值. 表: %v. 最小值: %v, 最大值: %v, 截止值: %v",
		common.CurrLine(), tableName, nextPrimaryRangeValue.MinValue, nextPrimaryRangeValue.MaxValue, this.MaxPrimaryRangeValueMap[tableName].MaxValue)

	// 比较但前生成的主键范围值的最小值是否 >= row copy 截止的值,
	if common.MapAGreaterOrEqualMapB(nextPrimaryRangeValue.MinValue,
		this.MaxPrimaryRangeValueMap[tableName].MaxValue) {

		// 新生成的主键值 的最小值 大于 row copy 截止的主键值, 该表从需要迁移的变量中移除
		log.Warningf("%v: 警告. 检测到新生成的主键范围值的最小值 >= row copy 截止的主键值. 该新生成的主键值不要进行 row copy, 标记该表已经row copy 完成. 表: %v: 最小值: %v, 截止值: %v",
			common.CurrLine(), tableName, nextPrimaryRangeValue.MinValue, this.MaxPrimaryRangeValueMap[tableName].MaxValue)

		delete(this.NeedRowCopyTableMap, tableName)

		return false, nil
	}

	// 先将该主键值传输给缓存通道中.
	addOrDelete := NewAddOrDelete(nextPrimaryRangeValue.Schema, nextPrimaryRangeValue.Table,
		nextPrimaryRangeValue.TimestampHash, AOD_TYPE_ADD, nextPrimaryRangeValue)
	this.AddOrDelWatingTagCompleteChan <- addOrDelete

	// 将该主键信息传输给消费者
	this.PrimaryRangeValueChan <- nextPrimaryRangeValue

	// 设置该表当前主键值已经生成到
	this.CurrentPrimaryRangeValueMap[tableName] = nextPrimaryRangeValue

	// 比较新生成的主键值是否 >= 最大的主键值
	if common.MapAGreaterOrEqualMapB(nextPrimaryRangeValue.MaxValue,
		this.MaxPrimaryRangeValueMap[tableName].MaxValue) {

		// 新生成的主键值大于 row copy 截止的主键值, 该表从需要迁移的变量中移除
		log.Infof("%v: 完成. 表: %v, 需要迁移的主键值已经全部生成完毕. 要求生成到 %v, 实际生成到 %v",
			common.CurrLine(), tableName, nextPrimaryRangeValue.MaxValue, this.MaxPrimaryRangeValueMap[tableName].MaxValue)

		delete(this.NeedRowCopyTableMap, tableName)
	}

	return false, nil
}

/* 循环消费, 进行row copy
Params:
    _parallerTag: 并发标签, 代表是第几个并发协程的操作
*/
func (this *RowCopy) LoopConsumePrimaryRangeValue(wg *sync.WaitGroup, _parallerTag int) {
	defer wg.Done()
	defer func() { // 完成后, 协程数减1
		this.RowCopyComsumerCount.Dec()
		if this.RowCopyComsumerCount.Load() == 0 { // 如果都消费完了,可以关闭掉row copy 主键处理的缓存
			close(this.AddOrDelWatingTagCompleteChan)
		}
	}()

	log.Infof("%v: 成功. 启动第 %v 个并发进行消费", common.CurrLine(), _parallerTag)

	errRetryCount := 0

	// 循环获取主键值
	for primaryRangeValue := range this.PrimaryRangeValueChan {
		for {
			if errRetryCount > this.Parser.ErrRetryCount {
				log.Errorf("%v: 错误. 协程 %v, row copy 消费发生错误. 并且重试次数已经达到上线 %v. 将退出消费 表: %v.%v, 最小值: %v, 最大值: %v.",
					common.CurrLine(), _parallerTag, this.Parser.ErrRetryCount, primaryRangeValue.Schema, primaryRangeValue.Table,
					primaryRangeValue.MinValue, primaryRangeValue.MaxValue)
				return
			}

			// 真正进行 row copy 操作
			err := this.ConsumePrimaryRangeValue_V2(_parallerTag, primaryRangeValue)
			if err != nil {
				errRetryCount++
				log.Errorf("%v: 错误. 协程 %v, 重试第%v次. 需要重试%v次. 表: %v.%v, 最小值: %v, 最大值: %v. %v",
					common.CurrLine(), _parallerTag, errRetryCount, this.Parser.ErrRetryCount, primaryRangeValue.Schema, primaryRangeValue.Table,
					primaryRangeValue.MinValue, primaryRangeValue.MaxValue, err)

				time.Sleep(time.Second * 1)
				continue
			}

			// 一个范围的row copy完成后将通知checksum
			if this.Parser.EnableChecksum {
				this.ToChecksumChan <- primaryRangeValue
			}

			errRetryCount = 0 // 设置当前错误重试次数为0, 代表没有错误
			break
		}
	}

	log.Infof("%v: 完成. 已经没有需要进行 row copy 的主键范围值了. 协程 %v 退出. 消费", common.CurrLine(), _parallerTag)
}

/* 消费row copy 的主键值
Params:
    _parallerTag: 并发标记
    _primaryRangeValue: 并发标签, 代表是第几个并发协程的操作
1. 对PrimaryRangeValueChan进行循环获取,
2. 对源表进行select 操作
3. 将数据 insert 到目标表中
4. 通知, 该主键范围消费完成
*/
func (this *RowCopy) ConsumePrimaryRangeValue(parallerTag int, primaryRangeValue *matemap.PrimaryRangeValue) error {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("%v: 错误. row copy 消费主键值发生错误. 表: %v.%v, 最小值: %v, 最大值: %v. %v. 退出 go-d-bus 程序, %v",
				common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, err, string(debug.Stack()))

			syscall.Exit(1)
		}
	}()

	// 获取源表数据
	data, rowCount, err := SelectRowCopyData(this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), primaryRangeValue)
	if err != nil {
		return fmt.Errorf("%v: 失败. row copy 获取源表数据错误. 表: %v.%v 最小值: %v, 最大值: %v. %v:%v, %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), err)
	}
	if rowCount < 1 { // 没有数据
		log.Warningf("%v: 警告. row copy 没有获取到表数据. 默认此次row copy 完成. 表: %v.%v. 最小值: %v, 最大值: %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue)

		return nil
	}

	// 向目标表插入数据
	err = InsertRowCopyData(this.ConfigMap.Target.Host.String, int(this.ConfigMap.Target.Port.Int64), primaryRangeValue.Schema, primaryRangeValue.Table, rowCount, data)
	if err != nil {
		return fmt.Errorf("%v: 失败. row copy 向目标数据库插入数据 表: %v.%v, 最小值: %v, 最大值: %v. %v:%v. %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), err)
	}

	log.Infof("%v: 完成. 协程%v, 范围 row copy 已经完成. 表: %v.%v. 最小值: %v, 最大值 %v",
		common.CurrLine(), parallerTag, primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue)

	// 通知删除缓存中的值
	addOrDelete := NewAddOrDelete(primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.TimestampHash, AOD_TYPE_DELETE, primaryRangeValue)
	this.AddOrDelWatingTagCompleteChan <- addOrDelete

	return nil
}

/* 消费row copy 的主键值
Params:
    _parallerTag: 并发标记
    _primaryRangeValue: 并发标签, 代表是第几个并发协程的操作
1. 对PrimaryRangeValueChan进行循环获取,
2. 对源表进行select 操作
3. 将数据 insert 到目标表中
4. 通知, 该主键范围消费完成
*/
func (this *RowCopy) ConsumePrimaryRangeValue_V2(parallerTag int, primaryRangeValue *matemap.PrimaryRangeValue) error {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("%v: 错误. row copy 消费主键值发生错误. 表: %v.%v, 最小值: %v, 最大值: %v. %v. 退出 go-d-bus 程序, %v",
				common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, err, string(debug.Stack()))

			syscall.Exit(1)
		}
	}()

	// 获取源表数据
	rows, err := SelectRowCopyData_V3(this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), primaryRangeValue)
	if err != nil {
		return fmt.Errorf("%v: 失败. row copy 获取源表数据错误. 表: %v.%v 最小值: %v, 最大值: %v. %v:%v, %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), err)
	}
	if len(rows) < 1 { // 没有数据
		log.Warningf("%v: 警告. row copy 没有获取到表数据. 默认此次row copy 完成. 表: %v.%v. 最小值: %v, 最大值: %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue)

		return nil
	}

	// 向目标表插入数据
	err = InsertRowCopyData_V2(this.ConfigMap.Target.Host.String, int(this.ConfigMap.Target.Port.Int64), primaryRangeValue.Schema, primaryRangeValue.Table, rows)
	if err != nil {
		return fmt.Errorf("%v: 失败. row copy 向目标数据库插入数据 表: %v.%v, 最小值: %v, 最大值: %v. %v:%v. %v",
			common.CurrLine(), primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue, this.ConfigMap.Source.Host.String, int(this.ConfigMap.Source.Port.Int64), err)
	}

	log.Infof("%v: 完成. 协程%v, 范围 row copy 已经完成. 表: %v.%v. 最小值: %v, 最大值 %v",
		common.CurrLine(), parallerTag, primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.MinValue, primaryRangeValue.MaxValue)

	// 通知删除缓存中的值
	addOrDelete := NewAddOrDelete(primaryRangeValue.Schema, primaryRangeValue.Table, primaryRangeValue.TimestampHash, AOD_TYPE_DELETE, primaryRangeValue)
	this.AddOrDelWatingTagCompleteChan <- addOrDelete

	return nil
}

/* 将刚刚生成的 row copy 主键值 cache起来
   将已经完成的 row copy 主键值 从cache中删除
*/
func (this *RowCopy) LoopAddOrDeleteCache(wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { // 通关闭保存 row copy 进度 协程
		this.CloseSaveRowCopyProgressChan <- true
	}()

	for addOrDelete := range this.AddOrDelWatingTagCompleteChan {
		tableName := common.FormatTableName(addOrDelete.Schema, addOrDelete.Table, "") // schema.table 没有带 反引号

		switch addOrDelete.Type {
		case AOD_TYPE_ADD: // 将刚刚生成的主键值保存起来, 等待完成后删除
			this.WaitingTagCompletePrirmaryRangeValueMap[tableName].Set(addOrDelete.TimestampHash, addOrDelete.PrimaryRangeValue)

			// 表还需要row copy +1
			this.RowCopyNoComsumeTimes[tableName]++
		case AOD_TYPE_DELETE: // 将已完成的row copy主键值删除

			var minPrimaryValue *matemap.PrimaryRangeValue
			var maxPrimaryValue *matemap.PrimaryRangeValue

			// 获取当前表的第一个主键值, 该值就是当前表 row copy 完成的值
			minMaxPrimaryValue := this.RowCopyConsumeMinMaxValue[tableName]

			// 获取当前表的row copy到的最小值, 并保存
			primaryValueCache := this.WaitingTagCompletePrirmaryRangeValueMap[tableName]
			primaryValueCacheIter := primaryValueCache.IterFunc()
			minData, ok := primaryValueCacheIter()
			if !ok {
				minPrimaryValue = addOrDelete.PrimaryRangeValue
			} else {
				minPrimaryValue = minData.Value.(*matemap.PrimaryRangeValue)
			}

			// 保存当前最小的 主键范围值
			minMaxPrimaryValue.Store("minValue", minPrimaryValue)

			// 获取当前表的row copy到的最大值, 没有则保存
			maxData, ok := minMaxPrimaryValue.Load("maxValue")
			if !ok {
				maxPrimaryValue = addOrDelete.PrimaryRangeValue
			} else {
				maxPrimaryValue = maxData.(*matemap.PrimaryRangeValue)
			}

			// 比较当前消费的主键范围最大值 是否 >= 当前row copy中的最大值
			// 是: 将当前 row copy 的值设置成 该表row copy 消费的最大值
			if common.MapAGreaterOrEqualMapB(addOrDelete.PrimaryRangeValue.MaxValue, maxPrimaryValue.MaxValue) {
				maxPrimaryValue = addOrDelete.PrimaryRangeValue
			}
			// 保存最大 row copy 范围值
			minMaxPrimaryValue.Store("maxValue", maxPrimaryValue)

			// 表还需要row copy +1
			this.RowCopyNoComsumeTimes[tableName]--
			// 如果该表还需要消费的 row copy 数量为0 则说明, row copy到的最小值和最大值相等
			if this.RowCopyNoComsumeTimes[tableName] == 0 {
				minMaxPrimaryValue.Store("minValue", maxPrimaryValue)
			}

			this.RowCopyConsumeMinMaxValue[tableName] = minMaxPrimaryValue

			// 将该主键值从 cache 中移除
			this.WaitingTagCompletePrirmaryRangeValueMap[tableName].Delete(addOrDelete.TimestampHash)
		}
	}
}

// 循环保存当前row copy 进度
func (this *RowCopy) LoopSaveRowCopyProgress(wg *sync.WaitGroup) {
	defer wg.Done()

	tableNames := make([]string, 0, 1)
	for tableName, _ := range this.RowCopyConsumeMinMaxValue {
		tableNames = append(tableNames, tableName)
	}

	isClose := false
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
ExistSaveProgress:
	for {
		select {
		case <-ticker.C:
			if isClose { // 如果需要关闭则再保存所有表的进度之后再退出, 保存缓存中最大的row copy进度
				for _, tableName := range tableNames {
					minMaxPrimaryRangeValue := this.RowCopyConsumeMinMaxValue[tableName]
					maxData, ok := minMaxPrimaryRangeValue.Load("maxValue")
					if !ok {
						log.Errorf("%v 失败. 保存表 row copy 进度(%v). 没有获取到已经消费的最大范围值. 将跳过该表进度保存",
							common.CurrLine(), tableName)
						continue
					}

					// 获取row copy 最大值
					maxPrimaryRangeValue := maxData.(*matemap.PrimaryRangeValue)
					maxValueJson, err := common.Map2Json(maxPrimaryRangeValue.MaxValue)
					if err != nil {
						log.Errorf("%v: 失败. 保存表row copy 进度(%v). 将进度数据转化为json失败. %v", common.CurrLine(), tableName, err)
						continue
					}

					// 保存 row copy 最后的进度数据
					schemaTable := strings.Split(tableName, ".")
					schema := schemaTable[0]
					table := schemaTable[1]
					UpdateTableCurrPrimaryValue(this.ConfigMap.TaskUUID, schema, table, maxValueJson)

					// 标记该表 row copy 完成
					TagTableRowCopyComplete(this.ConfigMap.TaskUUID, schema, table)
					log.Infof("%v: 完成. 标记表 row copy 完成. %v", common.CurrLine(), tableName)
				}

				// 标记该任务 row copy 完成
				TagTaskRowCopyComplete(this.ConfigMap.TaskUUID)
				log.Infof("%v: 完成. 标记任务 row copy 完成. %v", common.CurrLine(), this.ConfigMap.TaskUUID)
				break ExistSaveProgress
			} else { // 保存缓存中最小的 row copy 进度数据
				for _, tableName := range tableNames {
					minMaxPrimaryRangeValue := this.RowCopyConsumeMinMaxValue[tableName]
					minData, ok := minMaxPrimaryRangeValue.Load("minValue")
					if !ok {
						log.Errorf("%v 失败. 保存表 row copy 进度(%v). 没有获取到已经消费的最大范围值. 将跳过该表进度保存", common.CurrLine(), tableName)
						continue
					}

					// 获取row copy 最大值
					maxPrimaryRangeValue := minData.(*matemap.PrimaryRangeValue)
					minValueJson, err := common.Map2Json(maxPrimaryRangeValue.MaxValue)
					if err != nil {
						log.Errorf("%v: 失败. 保存表row copy 进度(%v). 将进度数据转化为json失败. %v", common.CurrLine(), tableName, err)
						continue
					}

					// 保存 row copy 最后的进度数据
					schemaTable := strings.Split(tableName, ".")
					schema := schemaTable[0]
					table := schemaTable[1]
					UpdateTableCurrPrimaryValue(this.ConfigMap.TaskUUID, schema, table, minValueJson)
				}
			}
		case <-this.CloseSaveRowCopyProgressChan:
			isClose = true
			log.Warningf("%v: 警告, 接收到关闭保存 row copy 进度通知, 即将关闭该协程.", common.CurrLine())
		}
	}
}
