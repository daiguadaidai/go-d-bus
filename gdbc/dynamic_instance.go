package gdbc

import (
    "fmt"
    "sync"
    "database/sql"

    "github.com/daiguadaidai/go-d-bus/setting"
    "github.com/juju/errors"
    _ "github.com/go-sql-driver/mysql"
    "github.com/outbrain/golib/log"
    "github.com/daiguadaidai/go-d-bus/common"
)

var dynamicConfigMap sync.Map
var dynamicInstanceMap DynamicInstanceMap

type DynamicInstance struct {
    DB *sql.DB
}

type DynamicInstanceMap struct {
    DBs sync.Map
    sync.Once
}

/* 保存数据库配置信息,
 将配置信息的 host:port 为 key 将配置信息保存到 sync.Map 中
 */
func SetDynamicConfig(_dbConfig *setting.DBConfig) error {
    if _dbConfig == nil {
        return errors.New("配置文件不能为 nil\n")
    }

    dynamicKey := _dbConfig.GetHostPortKey()
    dynamicConfigMap.Store(dynamicKey, _dbConfig)

    return nil
}

/* 通过host, port 获取数据库配置信息
  Params:
      _host: ip
          string-> 127.0.0.1
      _port: 端口
          int-> 3306
 */
func GetDynamicConifgByHostPort(_host string, _port int) (*setting.DBConfig, bool) {
    dynamicKey := fmt.Sprintf("%v:%v", _host, _port)
    dbConfig, ok := GetDynamicConfig(dynamicKey)

    return dbConfig, ok
}

/* 获取数据库配置文件
  通过给定的 key 从 sync.Map 中获取一个数据库配置信息
  Params:
      _dynamicKey: 获取配置文件的key
          string-> 127.0.0.1:3306
  Return:
      *setting.DBConfig: 数据库配置信息
      bool: 是否获取成功
 */
func GetDynamicConfig(_dynamicKey string) (*setting.DBConfig, bool) {
    dbConfigInterface, ok := dynamicConfigMap.Load(_dynamicKey)
    if !ok {
        return nil, ok
    }

    dbConfig := dbConfigInterface.(interface{}).(*setting.DBConfig)

    return dbConfig, ok
}

/* 单例模式获取原生数据库链接
  Params:
      _host: ip
          string-> 127.0.0.1
      _port: 端口
          int-> 3306
  Return:
      *DynamicInstance: 动态数据实例
      error: 错误信息
 */
func GetDynamicInstanceByHostPort(_host string, _port int) (*DynamicInstance, error){
    dynamicKey := fmt.Sprintf("%v:%v", _host, _port)
    dynamicInstance, err := GetDynamicInstance(dynamicKey)

    return dynamicInstance, err
}

/* 单例模式获取原生数据库链接
  Params:
      _dynamicKey: 获取配置文件的key
          string-> 127.0.0.1:3306
  Return:
      *DynamicInstance: 动态数据实例
      error: 错误信息
 */
func GetDynamicInstance(_dynamicKey string) (*DynamicInstance, error) {
    var dynamicInstance *DynamicInstance
    dynamicInstanceInterface, ok := dynamicInstanceMap.DBs.Load(_dynamicKey) // 获取动态实例

    if !ok { // 获取不到动态实例, 需要创建一个
        // 获取数据库实例配置信息
        dynamicConfig, ok := GetDynamicConfig(_dynamicKey)
        if !ok {
            errMsg := fmt.Sprintf("获取动态实例失败, 没有找到相关的实例配置信息, %v\n", _dynamicKey)
            return nil, errors.New(errMsg)
        }

        // 实例化元数据库实例
        dynamicInstanceMap.Once.Do(func() {
            // 获取元数据配置信息
            dbConfig := setting.DBConfigs[setting.MetaDBKey]

            // 链接数据库
            var err error
            dynamicInstance = new(DynamicInstance)

            log.Infof("%v: 数据库链接描述符: %v", common.CurrLine(), dynamicConfig.GetDataSource())

            dynamicInstance.DB, err = sql.Open("mysql", dynamicConfig.GetDataSource())
            if err != nil { // 打开数据库失败
                log.Errorf("%v: 打开动态数据库实例错误, key:%v, %v", common.CurrLine(),
                    _dynamicKey, err)
                return
            }

            dynamicInstance.DB.SetMaxOpenConns(dbConfig.MaxOpenConns)
            dynamicInstance.DB.SetMaxIdleConns(dbConfig.MaxIdelConns)

            // 将该实例链接保存在字典中
            dynamicInstanceMap.DBs.Store(_dynamicKey, dynamicInstance)
        })

        // 创建动态实例失败
        if dynamicInstance == nil {
            return nil, errors.New("获取动态实例失败, 不能创建动态实例")
        }
    } else { // 将动态实例接口类型转化成动态实例类型
        dynamicInstance = dynamicInstanceInterface.(interface{}).(*DynamicInstance)
    }


    return dynamicInstance, nil
}
