package service

import (
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/parser"
	"sync"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/outbrain/golib/log"
	"fmt"
	"github.com/liudng/godump"
)

type RowCopy struct {
	WG        *sync.WaitGroup
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
	// 每个表已经完成的主键范围值 map: {"schema.table": PrimaryRangeValue}
	CompletePrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue
	// 每个表最大的主键范围值, rowCopy截止的id范围 map: {"schema.table": PrimaryRangeValue}
    MaxPrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue
    /* 等待确认消费的 主键范围值, 确认消费一个就在其中删除一个
    map: {
        "schema.table": {
            timestampHash: PrimaryRangeValue,
            timestampHash: PrimaryRangeValue,
        }
    }
      */
    WaitingTagCompletePrirmaryRangeValueMap map[string]map[string]*matemap.PrimaryRangeValue
    // 用于传输删除还是添加 需要标记完成的 PrimaryRangeValue
    AddOrDelWatingTagCompleteChan chan *AddOrDelete
}

/* 创建一个 row Copy 对象
Params
    _parser: 命令行解析的信息
    _configMap: 配置信息
    _wg: 并发控制参数
 */
func NewRowCopy(_parser *parser.RunParser, _configMap *config.ConfigMap,
	_wg *sync.WaitGroup) (*RowCopy, error) {

	rowCopy := new(RowCopy)

	// 初始化配置控制信息
	rowCopy.ConfigMap = _configMap
	rowCopy.Parser = _parser
	rowCopy.WG = _wg

	// 初始化 需要迁移的表名映射信息
    rowCopy.MigrationTableNameMap = matemap.FindAllMigrationTableNameMap()
    log.Infof("%v: 成功. 初始化 row copy 所有迁移的表名. 包含了可以不用迁移的",
    	common.CurrLine())

	// 初始化 传输表的 主键 范围值
	rowCopy.PrimaryRangeValueChan = make(chan *matemap.PrimaryRangeValue, _parser.RowCopyHighWaterMark)
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
	log.Infof("%v: 成功. 初始化需要迁移的表row copy的截止主键值. 没有数据的表: %v",
		common.CurrLine(), maxNoDataTables)

	// 每个表当前生成生成到的主键范围 map: {"schema.table": PrimaryRangeValue}
	currentPrimaryRangeValueMap, currNoDataTables, err := rowCopy.GetCurrentPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.CurrentPrimaryRangeValueMap = currentPrimaryRangeValueMap
	rowCopy.TagCompleteNeedRowCopyTables(currNoDataTables) // 将没数据的表直接标记完成
	log.Infof("%v: 成功. 初始化需要迁移的表当前row copy到的主键值. 没有数据的表: %v",
		common.CurrLine(), currNoDataTables)

	// 如果表的当前 row copy 到的主键值 >= 表 row copy 截止的 主键值
	greaterTables := rowCopy.FindCurrGreaterMaxPrimaryTables()
	rowCopy.TagCompleteNeedRowCopyTables(greaterTables) // 将当前rowcopy的值 >= 截止的rowcopy表直接标记完成
	log.Infof("%v: 成功. 过滤需要迁移的表中 当前ID >= 截止ID 的表有: %v",
		common.CurrLine(), greaterTables)

	// 初始化每个表已经完成到的主键范围值 map: {"schema.table": PrimaryRangeValue}
	// 初始化的时候 已经完成的row copy 范围和 当前需要进行 row copy 的是一样的
	rowCopy.CompletePrimaryRangeValueMap = make(map[string]*matemap.PrimaryRangeValue)
	for key, value := range rowCopy.CurrentPrimaryRangeValueMap {
        rowCopy.CompletePrimaryRangeValueMap[key] = value
	}
	log.Infof("%v: 成功. 初始化已经完成的",
		common.CurrLine())

	// 初始化用于传输删除还是添加 需要标记完成的 PrimaryRangeValue
	rowCopy.AddOrDelWatingTagCompleteChan = make(chan *AddOrDelete)
	log.Infof("%v: 成功. 初始化添加还是删除需要标记id值的通道",
		common.CurrLine())

	return rowCopy, nil
}

// 开始进行row copy
func (this *RowCopy) Start() {
	defer this.WG.Done()

	// 循环生成 row copy 需要的主键值, 并将值放入通道中PrimaryRangeValueChan
    this.WG.Add(1)
	go this.GeneratePrimaryRangeValue()

	// 消费 PrimaryRangeValueChan 通道中的主键方位值
	log.Infof("%v: 设置了 %v 个并发执行 row copy 操作.",
		common.CurrLine(), this.Parser.RowCopyParaller)
	for parallerTag := 0; parallerTag < this.Parser.RowCopyParaller; parallerTag++ {
		this.WG.Add(1)
		go this.ConsumePrimaryRangeValue(parallerTag)
	}

}

/* 随机生成一个表的主键范围值
1. 随机生成id
2. 将id放入PrimaryRangeValueChan中
3. 设置CurrentPrimaryValueMap的值为当前
 */
func (this *RowCopy) GeneratePrimaryRangeValue() {
	defer this.WG.Done()

    tableName, ok := common.GetRandomMapKey(this.NeedRowCopyTableMap)
    if !ok { // 所有的表的 row copy 主键值范围数据都已经生成完了
        log.Infof(
        	"%v: 所有表的主键值已经生成完. 退出生成主键值的协程 %v %v:%v",
        	common.CurrLine(),
			this.ConfigMap.TaskUUID,
       		this.ConfigMap.Source.Host.String,
			this.ConfigMap.Source.Port.Int64,
        )

        close(this.PrimaryRangeValueChan)
        return
	}

	// 获取该表当前的 row copy 主键值
	currPrimaryRangeValue := this.CurrentPrimaryRangeValueMap[tableName]
	// 获取表的下一个主键方位值
	nextPrimaryRangeValue, err := currPrimaryRangeValue.GetNextPrimaryRangeValue(
		this.Parser.RowCopyLimit,
        this.ConfigMap.Source.Host.String,
        int(this.ConfigMap.Source.Port.Int64))
	if err != nil {
		errMSG := fmt.Sprintf("%v: row copy 生成表的下一个主键值失败. 停止产生相关表主键值. %v. %v",
			common.CurrLine(), tableName, err)
		log.Errorf(errMSG)

		close(this.PrimaryRangeValueChan)
        return
	}

	this.PrimaryRangeValueChan <- nextPrimaryRangeValue

	close(this.PrimaryRangeValueChan)
}

/* 消费row copy 的主键值
Params:
    _parallerTag: 并发标签, 代表是第几个并发协程的操作
1. 对PrimaryRangeValueChan进行循环获取,
2. 对源表进行select 操作
3. 将数据 insert 到目标表中
4. 通知, 该主键范围消费完成
 */
func (this *RowCopy) ConsumePrimaryRangeValue(_parallerTag int) {
    defer this.WG.Done()

    log.Infof("%v: 成功. 启动第 %v 个并发进行消费", common.CurrLine(), _parallerTag)

    for primaryRangeValue := range this.PrimaryRangeValueChan {
    	godump.Dump(primaryRangeValue)
	}

	log.Infof("%v: 完成. 已经没有需要进行 row copy 的主键范围值了. 协程 %v 退出. 消费",
		common.CurrLine(), _parallerTag)
}
