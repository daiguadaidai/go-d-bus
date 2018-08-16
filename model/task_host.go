package model

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

type TaskHost struct {
	Id             sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
	Host           sql.NullString `gorm:"type:varchar(15);not null"`                                                        // Host
	UpdatedAt      mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreatedAt      mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
	CurrProcessCnt sql.NullInt64  `gorm:"not null;default:0"`                                                               // 该host运行个数
	IsAvailable    sql.NullInt64  `gorm:"not null;default:1"`                                                               // 是否可用:0.否, 1.是
	IDC            sql.NullString `gorm:"not null;default:''"`                                                              // host所在IDC
}

func (TaskHost) TableName() string {
	return "task_host"
}
