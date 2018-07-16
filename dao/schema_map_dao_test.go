package dao

import (
	"fmt"
	"testing"
)

func TestSchemaMapDao_FindByTaskUUID(t *testing.T) {
	schemaMapDao := &SchemaMapDao{}

	var taskUUID string = "20180204151900nb6VqFhl"
	var columnStr string = "*"
	schemaMaps, err := schemaMapDao.FindByTaskUUID(taskUUID, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(schemaMaps)
}

func TestSchemaMapDao_Count(t *testing.T) {
	schemaMapDao := &SchemaMapDao{}

	var taskUUID string = "20180204151900nb6VqFhl"
	count := schemaMapDao.Count(taskUUID)

	fmt.Println(count)
}
