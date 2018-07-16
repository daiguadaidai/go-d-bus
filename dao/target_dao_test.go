package dao

import (
	"fmt"
	"testing"
)

func TestTargetDao_GetByID(t *testing.T) {
	targetDao := &TargetDao{}

	var id int64 = 2
	var columnStr string = "*"
	target, err := targetDao.GetByID(id, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(target)
}

func TestTargetDao_GetByTaskUUID(t *testing.T) {
	targetDao := &TargetDao{}

	var taskUUID string = "20180204151900nb6VqFhl"
	var columnStr string = "*"
	target, err := targetDao.GetByTaskUUID(taskUUID, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(target)
}

func TestTargetDao_FindByTaskUUID(t *testing.T) {
	targetDao := &TargetDao{}

	var taskUUID string = "20180204151900nb6VqFhl"
	var columnStr string = "*"
	targets, err := targetDao.FindByTaskUUID(taskUUID, columnStr)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(targets)
}
