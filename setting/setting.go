package setting

import (
    "fmt"
)

const (
    DEV_META_DB_CONFIG     string = "devMeta-d_bus"
    TEST_META_DB_CONFIG    string = "testMeta-d_bus"
    PRODUCT_META_DB_CONFIG string = "productMeta-d_bus"
)

var DBConfigs map[string]*DBConfig
var MetaDBKey string = DEV_META_DB_CONFIG

type DBConfig struct {
    Username          string
    Password          string
    Database          string
    CharSet           string
    Host              string
    TimeOut           int
    Port              int
    MaxOpenConns      int
    MaxIdelConns      int
    AllowOldPasswords int
    AutoCommit        bool
}

/* 通过配置文件生成数据库链接描述符并且返回
Return:
    string-> username:password@tcp(host:port)/database?charset=utf-8,&allowOldPasswords=1&timeout=10s& \
             autocommit=1&parseTime=True&loc=Local
 */
func (this *DBConfig) GetDataSource() string {
    dataSource := fmt.Sprintf(
        "%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
        this.Username,
        this.Password,
        this.Host,
        this.Port,
        this.Database,
        this.CharSet,
        this.AllowOldPasswords,
        this.TimeOut,
        this.AutoCommit,
    )
    return dataSource
}

/* 通过配置文件的 host, port 获取一个组合key
  Return:
      string-> 127.0.0.1:3306
 */
func (this *DBConfig) GetHostPortKey() string {
    return fmt.Sprintf("%v:%v", this.Host, this.Port)
}

func init() {
    DBConfigs = map[string]*DBConfig{
        // 开发库链接
        "devMeta-d_bus": {
            Username:          "HH",
            Password:          "oracle",
            Host:              "192.167.137.12",
            Port:              3306,
            Database:          "d_bus",
            CharSet:           "utf8,utf8mb4,latin1",
            MaxOpenConns:      500,
            MaxIdelConns:      250,
            TimeOut:           60,
            AllowOldPasswords: 1,
            AutoCommit:        true,
        },

        // 测试线d_bus库
        "testMeta-d_bus": {
            Username:          "HH",
            Password:          "oracle",
            Host:              "192.167.137.12",
            Port:              3306,
            Database:          "d_bus",
            CharSet:           "utf8,utf8mb4,latin1",
            MaxOpenConns:      500,
            MaxIdelConns:      250,
            TimeOut:           60,
            AllowOldPasswords: 1,
            AutoCommit:        true,
        },

        // 生产线d_bus库
        "productMeta-d_bus": {
            Username:          "HH",
            Password:          "oracle",
            Host:              "192.167.137.12",
            Port:              3306,
            Database:          "d_bus",
            CharSet:           "utf8,utf8mb4,latin1",
            MaxOpenConns:      500,
            MaxIdelConns:      250,
            TimeOut:           60,
            AllowOldPasswords: 1,
            AutoCommit:        true,
        },
    }
}
