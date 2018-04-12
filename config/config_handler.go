package config

import "fmt"

const (
    DB_CONFIG   = "dev"
    USER_CONFIG = "dev"
)

func CreateDBConfig() (*DBConfig, error) {
    dbConfig := getDBConfig(DB_CONFIG)
    if dbConfig == nil {
        return nil, fmt.Errorf("没有获取到指定的元数据库配置, db_config: %#v", DB_CONFIG)
    }

    return dbConfig, nil
}

func CreateUserConfig() (*UserConfig, error) {
    userConfig := getUserConfig(USER_CONFIG)
    if userConfig == nil {
        return nil, fmt.Errorf("没有获取到指定的用户配置, user_config: %#v", USER_CONFIG)
    }

    return userConfig, nil
}

/*
获取指定的数据库配置信息
Args:
    key: 指定的配置key
*/
func getDBConfig(key string) *DBConfig {
    dbConfigs := map[string]*DBConfig{
        "default": &DBConfig{
            "127.0.0.1",
            3306,
            "d_bus",
            "root",
            "root",
        },
        "dev": &DBConfig{
            "192.167.137.12",
            3306,
            "d_bus",
            "HH",
            "oracle",
        },
        "product": &DBConfig{
            "127.0.0.1",
            3306,
            "d_bus",
            "root",
            "root",
        },
    }

    return dbConfigs[key]
}

/*
获取指定的用户配置信息
Args:
    key: 指定的配置key
*/
func getUserConfig(key string) *UserConfig {
    userConfigs := map[string]*UserConfig{
        "default": &UserConfig{
            "root",
            "root",
        },
        "dev": &UserConfig{
            "HH",
            "oracle",
        },
        "product": &UserConfig{
            "root",
            "root",
        },
    }

    return userConfigs[key]
}
