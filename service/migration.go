package service

import (
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/logger"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/model"
	"github.com/daiguadaidai/go-d-bus/parser"
	mysqlab "github.com/daiguadaidai/go-d-bus/service/mysqlapplybinlog"
	mysqlcs "github.com/daiguadaidai/go-d-bus/service/mysqlchecksum"
	mysqlrc "github.com/daiguadaidai/go-d-bus/service/mysqlrowcopy"
	"github.com/daiguadaidai/go-d-bus/setting"
	"sync"
)

func StartMigration(runParser *parser.RunParser) {
	// 获取配置映射信息
	configMap, err := config.NewConfigMap(runParser.TaskUUID)
	if err != nil {
		logger.M.Fatal(err)
	}

	// 获取随机其中一个schemaMap
	randSchemaMap := configMap.GetRandSchemaMap()
	if randSchemaMap == nil {
		logger.M.Fatal("随机获取一个数据库映射信息失败, 没有数据库映射信息")
	}

	// 链接原数据数据库
	if err := InitSourceDB(configMap.Source, randSchemaMap.Source.String); err != nil {
		logger.M.Fatalf("初始化(源)数据库链接出错, %v", err)
	}

	// 初始化目标连接数
	if err := InitTargetDB(configMap.Target, randSchemaMap.Target.String); err != nil {
		logger.M.Fatalf("初始化(目标)数据库链接出错, %v", err)
	}

	// 如果没有设置binglog开始位点则show master status 找
	if runParser.StartLogFile == "" || runParser.StartLogPos < 0 {
		if err := runParser.SetStartBinlogInfoByHostAndPort(configMap.Source.Host.String, int(configMap.Source.Port.Int64)); err != nil {
			logger.M.Fatalf("实时获取主库 位点信息出错. %v, 退出迁移", err.Error())
		}
	}

	// 保存binglog开始位点
	if err := new(dao.SourceDao).UpdateStartLogPosInfo(runParser.TaskUUID, runParser.StartLogFile, runParser.StartLogPos); err != nil {
		logger.M.Fatalf("迁移启动保存位点信息出错 %v", err)
	}

	// 初始化需要迁移的表
	err = matemap.InitMigrationTableMap(configMap)
	if err != nil {
		logger.M.Fatal(err)
	}
	// 打印需要迁移的表信息
	matemap.ShowAllMigrationTableNames()
	matemap.ShowAllIgnoreMigrationTableNames(configMap)

	// 用于每次row copy 完成后告诉checksum需要对哪个范围进行checksum
	rowCopy2CheksumChan := make(chan *matemap.PrimaryRangeValue, 1000)
	// 当所有的 row copy 完成通知可以进行二次checksum
	// 第一次checksum是每一次rowcopy完都进行, 如果发生了数据不一致,
	// 会在最后所有的rowcopy完成后再次对第一次不一致的进行checksum操作
	notifySecondChecksum := make(chan bool, 1000)

	wg := new(sync.WaitGroup)
	// 开启了 checksum功能, 需要进行checksum
	if runParser.EnableChecksum {
		wg.Add(1)
		go StartChecksum(runParser, configMap, wg, rowCopy2CheksumChan, notifySecondChecksum)
	} else {
		logger.M.Warn("没有指定checksum, 本次迁移将不会进行数据校验")
	}

	// 开始进行 row copy
	if runParser.EnableRowCopy {
		err = StartRowCopy(runParser, configMap, rowCopy2CheksumChan, notifySecondChecksum)
		if err != nil {
			logger.M.Fatal(err)
		}
	} else {
		logger.M.Warn("没有指定row copy, 本次迁移将不会进行表拷贝操作")
	}

	// 开始应用binlog
	if runParser.EnableApplyBinlog {
		err = StartApplyBinlog(runParser, configMap)
		if err != nil {
			logger.M.Fatal(err)
		}
	} else {
		logger.M.Warn("没有指定应用binlog, 本次迁移将不会进行binlog的应用")
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
	parser *parser.RunParser,
	configMap *config.ConfigMap,
	rowCopy2ChecksumChan chan *matemap.PrimaryRangeValue,
	notifySecondChecksum chan bool,
) error {
	rowCopy, err := mysqlrc.NewRowCopy(parser, configMap, rowCopy2ChecksumChan, notifySecondChecksum)
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
	parser *parser.RunParser,
	configMap *config.ConfigMap,
	wg *sync.WaitGroup,
	rowCopy2ChecksumChan chan *matemap.PrimaryRangeValue,
	notifySecondChecksum chan bool,
) error {
	defer wg.Done()

	checksum, err := mysqlcs.NewChecksum(parser, configMap, rowCopy2ChecksumChan, notifySecondChecksum)
	if err != nil {
		return err
	}

	checksum.Start()

	return nil
}

func InitSourceDB(source *model.Source, dbName string) error {
	cfg := setting.NewMysqlConfig(
		source.Host.String,
		source.Port.Int64,
		source.UserName.String,
		source.Password.String,
		dbName,
		100,
		99,
	)

	db, err := gdbc.GetMySQLDB(cfg)
	if err != nil {
		return err
	}

	gdbc.AddInstanceToCache(source.Host.String, source.Port.Int64, db)

	return nil
}

func InitTargetDB(target *model.Target, dbName string) error {
	cfg := setting.NewMysqlConfig(
		target.Host.String,
		target.Port.Int64,
		target.UserName.String,
		target.Password.String,
		dbName,
		100,
		99,
	)

	db, err := gdbc.GetMySQLDB(cfg)
	if err != nil {
		return err
	}

	gdbc.AddInstanceToCache(target.Host.String, target.Port.Int64, db)

	return nil
}
