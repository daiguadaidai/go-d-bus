package dao

import (
	"testing"
	"github.com/daiguadaidai/go-d-bus/model"
	"database/sql"
	"fmt"
)

func TestDataChecksumDao_Create(t *testing.T) {
	dataChecksumDao := new(DataChecksumDao)

	data := model.DataChecksum{
		TaskUUID: sql.NullString{"111", true},
		SourceSchema: sql.NullString{"test", true},
		SourceTable: sql.NullString{"test", true},
		TargetSchema: sql.NullString{"test", true},
		TargetTable: sql.NullString{"test", true},
	}

	err := dataChecksumDao.Create(data)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

func TestDataChecksumDao_FindByTaskUUID(t *testing.T) {
	dataChecksumDao := new(DataChecksumDao)

	columnStr := "id, task_uuid, is_fix"
	rows, err := dataChecksumDao.FindByTaskUUID("111", columnStr)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	fmt.Println(rows)
	for i, row := range rows {
		fmt.Println(i, row)
	}
}

func TestDataChecksumDao_FixCompletedByID(t *testing.T) {
	dataChecksumDao := new(DataChecksumDao)

	affected := dataChecksumDao.FixCompletedByID(5, "test", "test")

	fmt.Println(affected)
}
