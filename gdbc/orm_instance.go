package gdbc

import (
    "sync"

    "github.com/daiguadaidai/go-d-bus/setting"
    _ "github.com/go-sql-driver/mysql"
    "github.com/jinzhu/gorm"
    "github.com/ngaut/log"
)

var ormInstance *OrmInstance

type OrmInstance struct {
    DB  *gorm.DB
    sync.Once
}

// 单例模式获取原生数据库链接
func GetOrmInstance() *OrmInstance {
    if ormInstance.DB == nil {
        // 实例化元数据库实例
        ormInstance.Once.Do(func() {
            // 获取元数据配置信息
            dbConfig := setting.DBConfigs[setting.MetaDBKey]

            // 链接数据库
            var err error
            ormInstance.DB, err = gorm.Open("mysql", dbConfig.GetDataSource())
            if err != nil {
                log.Errorf("打开ORM数据库实例错误, %v", err)
            }

            ormInstance.DB.DB().SetMaxOpenConns(dbConfig.MaxOpenConns)
            ormInstance.DB.DB().SetMaxIdleConns(dbConfig.MaxIdelConns)
            ormInstance.DB.DB().Ping()
        })
    }

    return ormInstance
}

func init() {
    // 初始化OrmInstance 实例
    ormInstance = new(OrmInstance)
}
