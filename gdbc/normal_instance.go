package gdbc

import (
    "database/sql"
    "sync"

    "github.com/daiguadaidai/go-d-bus/setting"
    _ "github.com/go-sql-driver/mysql"
    "github.com/outbrain/golib/log"
    "github.com/daiguadaidai/go-d-bus/common"
)

var normalInstance *NormalInstance

type NormalInstance struct {
    DB  *sql.DB
    sync.Once
}

// 单例模式获取原生数据库链接
func GetNormalInstance() *NormalInstance {
    if normalInstance.DB == nil {
        // 实例化元数据库实例
        normalInstance.Once.Do(func() {
            // 获取元数据配置信息
            dbConfig := setting.DBConfigs[setting.MetaDBKey]

            // 链接数据库
            var err error
            normalInstance.DB, err = sql.Open("mysql", dbConfig.GetDataSource())
            if err != nil {
                log.Errorf("%v: 打开普通数据库实例错误, %v", common.CurrLine(), err)
            }

            normalInstance.DB.SetMaxOpenConns(dbConfig.MaxOpenConns)
            normalInstance.DB.SetMaxIdleConns(dbConfig.MaxIdelConns)
        })
    }

    return normalInstance
}

func init() {
    // 初始化NormalInstance 实例
    normalInstance = new(NormalInstance)
}
