package dao

import (
	"fmt"
	"testing"
)

func TestColumnMapDao_FindByTaskUUID(t *testing.T) {
	columnMapDao := &ColumnMapDao{}

	var taskUUID string = "20180204151900nb6VqFhl"
	var columnStr string = "*"
	columnMaps, err := columnMapDao.FindByTaskUUID(taskUUID, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(columnMaps)
}
