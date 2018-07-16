package parser

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/dao"
	"github.com/juju/errors"
)

/* 检测需要运行的任务
Params:
    _taskUUID: 任务ID
*/
func DetectTask(_taskUUID string) error {
	// 检测 task uuid 是否正确
	if err := DetectTaskUUIDInput(_taskUUID); err != nil {
		return err
	}

	// 检测 指定的任务是否存在
	if err := DetectTaskExists(_taskUUID); err != nil {
		return err
	}

	return nil
}

/* 检测 task uuid 是否正确
Params:
    _taskUUID: 任务ID
*/
func DetectTaskUUIDInput(_taskUUID string) error {
	if len(_taskUUID) < 10 {
		errMSG := fmt.Sprintf("失败. 输入的任务ID长度小于10个字符, "+
			"被判定为无效任务ID. Task UUID: %v %v", _taskUUID, common.CurrLine())
		return errors.New(errMSG)
	}

	return nil
}

/* 检测 task 是否存在
Params:
    _taskUUID: 任务ID
*/
func DetectTaskExists(_taskUUID string) error {
	taskDao := new(dao.TaskDao)

	if count := taskDao.Count(_taskUUID); count <= 0 {
		errMSG := fmt.Sprintf("失败. 指定任务不存在, 在数据库中没有找到. Task UUID: %v %v",
			_taskUUID, common.CurrLine())
		return errors.New(errMSG)
	}

	return nil
}

/* 检测 任务是否已经在运行
Params:
    _taskUUID: 任务ID
*/
func DetectTaskRunning(_taskUUID string) error {
	taskDao := new(dao.TaskDao)
	task, err := taskDao.GetByTaskUUID(_taskUUID, "*")
	if err != nil {
		errMSG := fmt.Sprintf("失败. 检测任务是否在运行(获取数据库错误). Task UUID: %v %v %v",
			_taskUUID, err, common.CurrLine())
		return errors.New(errMSG)
	}
	if task == nil {
		errMSG := fmt.Sprintf("失败. 检测任务是否在运行(没有获取到任务). Task UUID: %v %v %v",
			_taskUUID, err, common.CurrLine())
		return errors.New(errMSG)
	}

	if task.RunStatus.Int64 == 3 {
		errMSG := fmt.Sprintf("失败. 检测任务正在运行(没有获取到任务). Task UUID: %v, "+
			"runHost: %v %v",
			_taskUUID, task.RunHost.String, common.CurrLine())
		return errors.New(errMSG)
	}

	return nil
}
