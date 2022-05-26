package setting

import (
	"encoding/json"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
)

const (
	DefaultMysqlHost              = "127.0.0.1"
	DefaultMysqlPort              = 3306
	DefaultMysqlUsername          = "easydb"
	DefaultMysqlPassword          = "5c4b972f4e66929da2be30dec377d1ac0c81df4f5d1eaf57fbf0f67c70664ff309b6b40ca35781083305bb0d19dfb7f6"
	DefaultMysqlDatabase          = ""
	DefaultMysqlConnTimeout       = 5
	DefaultMysqlCharset           = "utf8mb4"
	DefaultMysqlMaxOpenConns      = 1
	DefaultMysqlMaxIdleConns      = 1
	DefaultMysqlAllowOldPasswords = 1
	DefaultMysqlAutoCommit        = true
)

type MysqlConfig struct {
	MysqlHost              string `json:"mysql_host" toml:"mysql_host"`                               // 数据库host
	MysqlPort              int64  `json:"mysql_port" toml:"mysql_port"`                               // 数据库端口
	MysqlUsername          string `json:"mysql_username" toml:"mysql_username"`                       // 数据库用户名
	MysqlPassword          string `json:"mysql_password" toml:"mysql_password"`                       // 数据库密码
	MysqlDatabase          string `json:"mysql_database" toml:"mysql_database"`                       // 链接数据库
	MysqlConnTimeout       int    `json:"mysql_conn_timeout" toml:"mysql_conn_timeout"`               // 数据库链接超时
	MysqlCharset           string `json:"mysql_charset" toml:"mysql_charset"`                         // 字符集
	MysqlMaxOpenConns      int    `json:"mysql_max_open_conns" toml:"mysql_max_open_conns"`           // 最大链接数
	MysqlMaxIdleConns      int    `json:"mysql_max_idel_conns" toml:"mysql_max_idel_conns"`           // 空闲链接数
	MysqlAllowOldPasswords int    `json:"mysql_allow_old_passwords" toml:"mysql_allow_old_passwords"` // 是否允许oldpassword
	MysqlAutoCommit        bool   `json:"mysql_auto_commit" toml:"mysql_auto_commit"`                 // 是否自动提交
}

func NewMysqlConfig(host string, port int64, username, password, database string, maxOpenConns, maxIdleConns int64) *MysqlConfig {
	return &MysqlConfig{
		MysqlHost:              host,
		MysqlPort:              port,
		MysqlUsername:          username,
		MysqlPassword:          password,
		MysqlDatabase:          database,
		MysqlCharset:           DefaultMysqlCharset,
		MysqlConnTimeout:       DefaultMysqlConnTimeout,
		MysqlMaxOpenConns:      int(maxOpenConns),
		MysqlMaxIdleConns:      int(maxIdleConns),
		MysqlAllowOldPasswords: DefaultMysqlAllowOldPasswords,
		MysqlAutoCommit:        DefaultMysqlAutoCommit,
	}
}

func (this *MysqlConfig) DeepClone() (*MysqlConfig, error) {
	raw, err := json.Marshal(this)
	if err != nil {
		return nil, fmt.Errorf("启动配置DeepClone出错, MysqlConfig -> Json: %v", err.Error())
	}

	var mysqlConfig MysqlConfig
	err = json.Unmarshal(raw, &mysqlConfig)
	if err != nil {
		return nil, fmt.Errorf("启动配置DeepClone出错, Json -> MysqlConfig: %v", err.Error())
	}

	return &mysqlConfig, nil
}

func (this *MysqlConfig) GetPassword() (string, error) {
	password, err := common.Decrypt(this.MysqlPassword)
	if err != nil {
		return this.MysqlPassword, fmt.Errorf("数据库密码解密失败, 使用未解密密码: %s", err.Error())
	}

	return password, nil
}

func (this *MysqlConfig) GetDataSource() (string, error, error) {
	password, err := this.GetPassword()

	dataSource := fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
		this.MysqlUsername,
		password,
		this.MysqlHost,
		this.MysqlPort,
		this.MysqlDatabase,
		this.MysqlCharset,
		this.MysqlAllowOldPasswords,
		this.MysqlConnTimeout,
		this.MysqlAutoCommit,
	)

	return dataSource, nil, err
}

// 获取模糊数据源
func (this *MysqlConfig) GetFuzzyDataSource() string {
	dataSource := fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
		this.MysqlUsername,
		"***",
		this.MysqlHost,
		this.MysqlPort,
		this.MysqlDatabase,
		this.MysqlCharset,
		this.MysqlAllowOldPasswords,
		this.MysqlConnTimeout,
		this.MysqlAutoCommit,
	)

	return dataSource
}
