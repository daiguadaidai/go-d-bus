package service

import (
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/parser"
	mysqlab "github.com/daiguadaidai/go-d-bus/service/mysqlapplybinlog"
	mysqlcs "github.com/daiguadaidai/go-d-bus/service/mysqlchecksum"
	mysqlrc "github.com/daiguadaidai/go-d-bus/service/mysqlrowcopy"
	"github.com/outbrain/golib/log"
	"sync"
)

func StartMigration(_parser *parser.RunParser) {
	// 获取配置映射信息
	configMap, err := config.NewConfigMap(_parser.TaskUUID)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// 设置源和目标实例配置信息
	err = configMap.SetSourceDBConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}
	err = configMap.SetTargetDBConfig()
	if err != nil {
		log.Fatalf("%v", err)
	}

	// 如果没有设置binglog开始位点则show master status 找
	if _parser.StartLogFile == "" || _parser.StartLogPos < 0 {
		if err := _parser.SetStartBinlogInfoByHostAndPort(configMap.Source.Host.String, int(configMap.Source.Port.Int64)); err != nil {
			log.Fatalf("实时获取主库 位点信息出错. %v, 退出迁移", err.Error())
		}
	}

	// 初始化需要迁移的表
	err = matemap.InitMigrationTableMap(configMap)
	if err != nil {
		log.Fatalf("%v", err)
	}
	// 打印需要迁移的表信息
	matemap.ShowAllMigrationTableNames()
	matemap.ShowAllIgnoreMigrationTableNames(configMap)

	wg := new(sync.WaitGroup)

	// 用于每次row copy 完成后告诉checksum需要对哪个范围进行checksum
	rowCopy2CheksumChan := make(chan *matemap.PrimaryRangeValue)
	// 当所有的 row copy 完成通知可以进行二次checksum
	// 第一次checksum是每一次rowcopy完都进行, 如果发生了数据不一致,
	// 会在最后所有的rowcopy完成后再次对第一次不一致的进行checksum操作
	notifySecondChecksum := make(chan bool)

	// 开启了 checksum功能, 需要进行checksum
	if _parser.EnableChecksum {
		wg.Add(1)
		go StartChecksum(_parser, configMap, wg, rowCopy2CheksumChan, notifySecondChecksum)
	} else {
		log.Warningf("%v: 没有指定checksum, 本次迁移将不会进行数据校验", common.CurrLine())
	}

	// 开始进行 row copy
	if _parser.EnableRowCopy {
		err = StartRowCopy(_parser, configMap, rowCopy2CheksumChan, notifySecondChecksum)
		if err != nil {
			log.Fatalf("%v: %v", common.CurrLine(), err)
		}
	} else {
		log.Warningf("%v: 没有指定row copy, 本次迁移将不会进行表拷贝操作", common.CurrLine())
	}

	// 开始应用binlog
	if _parser.EnableApplyBinlog {
		err = StartApplyBinlog(_parser, configMap)
		if err != nil {
			log.Fatalf("%v", err)
		}
	} else {
		log.Warningf("%v: 没有指定应用binlog, 本次迁移将不会进行binlog的应用", common.CurrLine())
	}

	wg.Wait()
}

/* 开始对 binlog 进行应用
Params:
    _parser: 启动参数
    _configMap: 需要迁移的表的配置映射信息
    _wg: 并发参数
*/
func StartApplyBinlog(_parser *parser.RunParser, _configMap *config.ConfigMap) error {
	applyBinlog, err := mysqlab.NewApplyBinlog(_parser, _configMap)
	if err != nil {
		return err
	}

	applyBinlog.Start()

	return nil
}

/* 开始行拷贝
Params:
    _parser: 启动参数
    _configMap: 需要迁移的表的配置映射信息
    _wg: 并发参数
	_rowCopy2ChecksumChan: 行拷贝到checksum
	_notifySecondChecksum: 通知可以进行二次checksum了
*/
func StartRowCopy(
	_parser *parser.RunParser,
	_configMap *config.ConfigMap,
	_rowCopy2ChecksumChan chan *matemap.PrimaryRangeValue,
	_notifySecondChecksum chan bool,
) error {

	isComplete, err := mysqlrc.TaskRowCopyIsComplete(_configMap.TaskUUID)
	if err != nil {
		log.Errorf("%v: 失败. 获取任务 row copy 是否完成失败. 将不进行row copy行为. %v. %v",
			common.CurrLine(), _configMap.TaskUUID, err)
		return nil
	}
	if isComplete {
		log.Warningf("%v: 警告. row copy 任务已经完成. 不需要进行row copy 操作. %v",
			common.CurrLine(), _configMap.TaskUUID)
		return nil
	}

	rowCopy, err := mysqlrc.NewRowCopy(_parser, _configMap, _rowCopy2ChecksumChan, _notifySecondChecksum)
	if err != nil {
		return err
	}

	rowCopy.Start()

	return nil
}

/* 开始进行数据校验
Params:
    _parser: 启动参数
    _configMap: 需要迁移的表的配置映射信息
    _wg: 并发参数
	_rowCopy2ChecksumChan: 行拷贝到checksum
	_notifySecondChecksum: 通知可以进行二次checksum了
*/
func StartChecksum(
	_parser *parser.RunParser,
	_configMap *config.ConfigMap,
	_wg *sync.WaitGroup,
	_rowCopy2ChecksumChan chan *matemap.PrimaryRangeValue,
	_notifySecondChecksum chan bool,
) error {
	defer _wg.Done()

	checksum, err := mysqlcs.NewChecksum(_parser, _configMap, _rowCopy2ChecksumChan, _notifySecondChecksum)
	if err != nil {
		return err
	}

	checksum.Start()

	return nil
}
