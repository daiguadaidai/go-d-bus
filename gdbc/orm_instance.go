package gdbc

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/setting"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/outbrain/golib/log"
)

var ormDB *gorm.DB

func SetOrmDB(mysqlConfig *setting.MysqlConfig) error {
	dataSource, err, _ := mysqlConfig.GetDataSource()
	if err != nil {
		return fmt.Errorf("orm 获取数据源(d_bus)出错: %s", err.Error())
	}
	ormDB, err = gorm.Open("mysql", dataSource)
	if err != nil {
		log.Errorf("%v: 打开ORM数据库实例错误, %v", common.CurrLine(), err)
	}

	ormDB.DB().SetMaxOpenConns(mysqlConfig.MysqlMaxOpenConns)
	ormDB.DB().SetMaxIdleConns(mysqlConfig.MysqlMaxIdleConns)

	if err := ormDB.DB().Ping(); err != nil {
		log.Errorf("%v: ping 数据库(d_bus)出错 orm , %v", common.CurrLine(), err)
	}

	return nil
}

func GetOrmInstance() *gorm.DB {
	return ormDB
}
