package service

import (
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/parser"
	"sync"
	"github.com/daiguadaidai/go-d-bus/matemap"
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
	// 还需要生成主键范围数据的表 map: {"schema.table": PrimaryRangeValue}
	NeedGetPrimaryRangeValueMap map[string]bool
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
    WaitingTagCompletePrirmaryRangeValueMap map[string]map[int]*matemap.PrimaryRangeValue
    // 用于传输删除还是添加 需要标记完成的 PrimaryRangeValue
    AddOrDelWatingTagCompleteChan chan AddOrDelete
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

	// 初始化 传输表的 主键 范围值
	rowCopy.PrimaryRangeValueChan = make(chan *matemap.PrimaryRangeValue, _parser.RowCopyHighWaterMark)

	// 获取 还需要生成主键范围数据的表 map: {"schema.table": PrimaryRangeValue}
	needGetPrimaryRangeValueMap, err := rowCopy.GetNeedGetPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.NeedGetPrimaryRangeValueMap = needGetPrimaryRangeValueMap

	// 每个表当前生成生成到的主键范围 map: {"schema.table": PrimaryRangeValue}
	currentPrimaryRangeValueMap, err := rowCopy.GetCurrentPrimaryRangeValueMap()
	if err != nil {
		return nil, err
	}
	rowCopy.CurrentPrimaryRangeValueMap = currentPrimaryRangeValueMap
	/*
	// 每个表已经完成的主键范围值 map: {"schema.table": PrimaryRangeValue}
	CompletePrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue
	// 每个表最大的主键范围值, rowCopy截止的id范围 map: {"schema.table": PrimaryRangeValue}
	MaxPrimaryRangeValueMap map[string]*matemap.PrimaryRangeValue
	*/

	return rowCopy, nil
}

// 开始进行row copy
func (this *RowCopy) Start() {
	defer this.WG.Done()
}

/* 随机生成一个表的主键范围值
1. 随机生成id
2. 将id放入PrimaryRangeValueChan中
3. 设置CurrentPrimaryValueMap的值为当前
 */
func (this *RowCopy) GeneratePrimaryRangeValue() {

}
