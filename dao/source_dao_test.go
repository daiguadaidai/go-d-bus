package dao

import (
    "fmt"
    "testing"
)

func TestSourceDao_GetByID(t *testing.T) {
    sourceDao := &SourceDao{}

    var id int64 = 2
    var columnStr string = "*"
    source, err := sourceDao.GetByID(id, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(source)
}

func TestSourceDao_GetByTaskUUID(t *testing.T) {
    sourceDao := &SourceDao{}

    var taskUUID string = "20180204151900nb6VqFhl"
    var columnStr string = "*"
    source, err := sourceDao.GetByTaskUUID(taskUUID, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(source)
}

func TestSourceDao_FindByTaskUUID(t *testing.T) {
    sourceDao := &SourceDao{}

    var taskUUID string = "20180204151900nb6VqFhl"
    var columnStr string = "*"
    sources, err := sourceDao.FindByTaskUUID(taskUUID, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(sources)
}
