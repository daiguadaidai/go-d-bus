package parser

import (
	"github.com/daiguadaidai/go-d-bus/gdbc"
	"github.com/daiguadaidai/go-d-bus/setting"
)

func InitOrmDB(mysqlConfig *setting.MysqlConfig) error {
	return gdbc.SetOrmDB(mysqlConfig)
}
