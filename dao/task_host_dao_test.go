package dao

import (
    "fmt"
    "testing"
)

func TestTaskHostDao_GetByID(t *testing.T) {
    taskHostDao := &TaskHostDao{}

    var id int64 = 2
    var columnStr string = "*"
    taskHost, err := taskHostDao.GetByID(id, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(taskHost)
}

func TestTaskHostDao_FindByAvailable(t *testing.T) {
    taskHostDao := &TaskHostDao{}

    var isAvailable int64 = 1
    var columnStr string = "*"
    taskHosts, err := taskHostDao.FindByAvailable(isAvailable, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(taskHosts)
}

func TestTaskHostDao_FindByAvailableAndIDC(t *testing.T) {
    taskHostDao := &TaskHostDao{}

    var isAvailable int64 = 1
    var idc string = "XG"
    var columnStr string = "*"
    taskHosts, err := taskHostDao.FindByAvailableAndIDC(isAvailable, idc, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(taskHosts)
}

func TestTaskHostDao_GetLeastAvailable(t *testing.T) {
    taskHostDao := &TaskHostDao{}

    var idc string = ""
    var columnStr string = "*"
    taskHost, err := taskHostDao.GetLeastAvailable(idc, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(taskHost)
}

func TestTaskHostDao_FindByHost(t *testing.T) {
    taskHostDao := &TaskHostDao{}

    var host string = "192.167.137.21"
    var columnStr string = "*"
    taskHosts, err := taskHostDao.FindByHost(host, columnStr)
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println(taskHosts)
}
