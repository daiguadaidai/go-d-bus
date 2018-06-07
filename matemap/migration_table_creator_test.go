package matemap

import (
	"testing"
	"github.com/daiguadaidai/go-d-bus/config"
	"fmt"
)

func TestInitMigrationTableMap(t *testing.T) {
	taskUUID := "20180204151900nb6VqFhl"

	// 获取需要迁移的表配置信息
	configMap, err := config.NewConfigMap(taskUUID)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// 设置源和目标实例配置信息
	err = configMap.SetSourceDBConfig()
	if err != nil {
		t.Fatalf("%v", err)
	}
	err = configMap.SetTargetDBConfig()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// 初始化具体需要迁移的表映射信息
	err = InitMigrationTableMap(configMap)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}

	// 获取表
	schemaName := "test"
	tableName := "store"
    table, err := GetMigrationTableBySchemaTable(schemaName, tableName)
    if err != nil {
    	t.Fatalf("%v", err)
	}

	fmt.Println(table)
}
