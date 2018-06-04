package dao

import (
    "fmt"
    "testing"
)

func TestTaskDao_GetByID(t *testing.T) {
    taskDao := &TaskDao{}

    var id int64 = 3
    var columnStr string = "*"
    task, err := taskDao.GetByID(id, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(task)
}

func TestTaskDao_GetByTaskUUID(t *testing.T) {
    taskDao := &TaskDao{}

    var taskUUID string = "20180204151900nb6VqFhl"
    var columnStr string = "*"
    task, err := taskDao.GetByTaskUUID(taskUUID, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(task)
}
