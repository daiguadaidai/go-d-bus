package dao

import (
    "fmt"
    "testing"
)

func TestTableMapDao_FindByTaskUUID(t *testing.T) {
    tableMapDao := &TableMapDao{}

    var taskUUID string = "20180204151900nb6VqFhl"
    var columnStr string = "*"
    tableMaps, err := tableMapDao.FindByTaskUUID(taskUUID, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(tableMaps)
}
