package gdbc

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/logger"
	"github.com/daiguadaidai/go-d-bus/setting"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

var ormDB *gorm.DB

func SetOrmDB(mysqlConfig *setting.MysqlConfig) error {
	dataSource, err, _ := mysqlConfig.GetDataSource()
	if err != nil {
		return fmt.Errorf("orm 获取数据源(d_bus)出错: %s", err.Error())
	}
	ormDB, err = gorm.Open("mysql", dataSource)
	if err != nil {
		logger.M.Errorf("打开ORM数据库实例错误, %v", err)
	}

	ormDB.DB().SetMaxOpenConns(mysqlConfig.MysqlMaxOpenConns)
	ormDB.DB().SetMaxIdleConns(mysqlConfig.MysqlMaxIdleConns)

	if err := ormDB.DB().Ping(); err != nil {
		logger.M.Errorf("ping 数据库(d_bus)出错 orm , %v", err)
	}

	return nil
}

func GetOrmInstance() *gorm.DB {
	return ormDB
}
