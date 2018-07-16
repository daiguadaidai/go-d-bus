package config

import (
	"fmt"
	"testing"
)

func TestNewConfigMap(t *testing.T) {
	taskUUID := "20180204151900nb6VqFhl"

	configMap, err := NewConfigMap(taskUUID)
	if err != nil {
		t.Errorf("%v", err)
	}

	fmt.Println("task UUID: ", configMap.TaskUUID)
	fmt.Println("source: ", configMap.Source, "source key:", configMap.Source.GetHostPortStr())
	fmt.Println("target: ", configMap.Target, "target key:", configMap.Target.GetHostPortStr())
	fmt.Println("run quota: ", configMap.RunQuota)
	fmt.Println("schemaMap map: ", configMap.SchemaMapMap)
	fmt.Println("tableMap map: ", configMap.TableMapMap)
	fmt.Println("columnMap map: ", configMap.ColumnMapMap)
}
