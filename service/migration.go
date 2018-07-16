package service

import (
	"github.com/daiguadaidai/go-d-bus/config"
	"github.com/daiguadaidai/go-d-bus/matemap"
	"github.com/daiguadaidai/go-d-bus/parser"
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

	// 初始化需要迁移的表
	err = matemap.InitMigrationTableMap(configMap)
	if err != nil {
		log.Fatalf("%v", err)
	}
	// 打印需要迁移的表信息
	matemap.ShowAllMigrationTableNames()
	matemap.ShowAllIgnoreMigrationTableNames(configMap)

	wg := new(sync.WaitGroup)

	/* 开始应用binlog
	err = StartApplyBinlog(_parser, configMap, wg)
	if err != nil {
		log.Fatalf("%v", err)
	}
	*/

	// 开始进行 row copy
	err = StartRowCopy(_parser, configMap, wg)
	if err != nil {
		log.Fatalf("%v", err)
	}

	wg.Wait()
}

/* 开始对 binlog 进行应用
Params:
    _parser: 启动参数
    _configMap: 需要迁移的表的配置映射信息
    _wg: 并发参数
*/
func StartApplyBinlog(_parser *parser.RunParser, _configMap *config.ConfigMap,
	_wg *sync.WaitGroup) error {

	applyBinlog, err := NewApplyBinlog(_parser, _configMap, _wg)
	if err != nil {
		return err
	}

	applyBinlog.WG.Add(1)
	go applyBinlog.Start()

	return nil
}

/*

 */
func StartRowCopy(_parser *parser.RunParser, _configMap *config.ConfigMap,
	_wg *sync.WaitGroup) error {

	rowCopy, err := NewRowCopy(_parser, _configMap, _wg)
	if err != nil {
		return err
	}

	rowCopy.WG.Add(1)
	rowCopy.Start()

	return nil
}
