package config

import "testing"

func TestCreateDBConfig(t *testing.T) {
    _, err := CreateDBConfig()
    if err != nil {
        t.Errorf("创建数据库配置错误: %#v", err)
    }
}

func TestCreateUserConfig(t *testing.T) {
    _, err := CreateUserConfig()
    if err != nil {
        t.Errorf("创建用户配置错误: %#v", err)
    }
}
