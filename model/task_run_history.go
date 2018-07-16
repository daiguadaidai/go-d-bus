package model

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

type TaskRunHistory struct {
	Id              sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
	TaskUUID        sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
	Type            sql.NullInt64  `gorm:"not null;default:1"`                                                               // 任务类型: 1.普通迁移, 2.sharding_o2m, 3.sharding_m2m
	Name            sql.NullString `gorm:"type:varchar(30)"`                                                                 // 迁移名称, 用来描述一个迁移任务
	HeartbeatShema  sql.NullString `gorm:"column:heartbeat_shema;type:varchar(30);not null;default:'dbmonitor'"`             //心跳检测数据库
	HeartbeatTable  sql.NullString `gorm:"column:heartbeat_table;type:varchar(30);not null;default:'slave_delay_time'"`      //心跳检测表
	Pause           sql.NullString `gorm:"type:varchar(10)"`                                                                 // 暂停: NULL/immediate/normal
	RunStatus       sql.NullInt64  `gorm:"column:run_status;not null;default:4"`                                             // '1.receive(刚接收到), 2.ready, 3.running, 4.stop, 11.停滞接收, 12.停滞准备',
	IsComplete      sql.NullInt64  `gorm:"column:is_complete;not null;default:0"`                                            // 迁移是否完成: 0:否, 1:是
	UpdatedAt       mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreateAt        mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
	RowCopyLimit    sql.NullInt64  `gorm:"column:row_copy_limit;not null;default:1000"`                                      // row copy每批应用的行数
	RunHost         sql.NullString `gorm:"column:run_host;type:varchar(15)"`                                                 //任务运行在哪个机器上
	RowCopyComplete sql.NullInt64  `gorm:"column:row_copy_complete;not null;default:0"`                                      //row copy 是否完成:0否 1是
	RHWM            sql.NullInt64  `gorm:"column:row_high_water_mark;not null;default:10000"`                                //队列中超过多少数据, 进行等待, 默认 1w
	RLWM            sql.NullInt64  `gorm:"column:row_low_water_mark;not null;default:2000"`                                  //队列中少于2000, 进行开始继续解析, 默认 2k
	StartTime       mysql.NullTime `gorm:"column:start_time"`                                                                //任务开始时间
	RowCopyParaller sql.NullInt64  `gorm:"column:row_copy_paraller;not null;default:10"`                                     //row copy 并发数
	BinlogParaller  sql.NullInt64  `gorm:"column:binlog_paraller;not null;default:15"`                                       //应用binlog 并发数
}

func (TaskRunHistory) TableName() string {
	return "task_run_history"
}
