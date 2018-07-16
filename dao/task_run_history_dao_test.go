package dao

import (
	"fmt"
	"testing"
)

func TestTaskRunHistoryDao_GetByID(t *testing.T) {
	taskRunHistoryDao := &TaskRunHistoryDao{}

	var id int64 = 2
	var columnStr string = "*"
	taskRunHistory, err := taskRunHistoryDao.GetByID(id, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(taskRunHistory)
}

func TestTaskRunHistoryDao_FindByTaskUUID(t *testing.T) {
	taskRunHistoryDao := &TaskRunHistoryDao{}

	var task_uuid string = "20180204151900nb6VqFhl"
	var columnStr string = "*"
	TaskRunHistorys, err := taskRunHistoryDao.FindByTaskUUID(task_uuid, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(TaskRunHistorys)
}
