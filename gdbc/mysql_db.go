package gdbc

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/setting"
	_ "github.com/go-sql-driver/mysql"
)

func GetMySQLDB(mysqlConfig *setting.MysqlConfig) (*sql.DB, error) {
	dataSource, err, _ := mysqlConfig.GetDataSource()
	if err != nil {
		return nil, fmt.Errorf("获取数据源出错: %s", err.Error())
	}

	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		return nil, fmt.Errorf("获取打开数据库失败. %s", err.Error())
	}
	db.SetMaxIdleConns(mysqlConfig.MysqlMaxIdleConns)
	db.SetMaxOpenConns(mysqlConfig.MysqlMaxOpenConns)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping数据库失败. %s", err.Error())
	}

	return db, nil
}
