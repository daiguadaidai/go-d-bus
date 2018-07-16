package model

import (
	"database/sql"

	"fmt"
	"github.com/go-sql-driver/mysql"
)

type Target struct {
	Id              sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
	TaskUUID        sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
	Host            sql.NullString `gorm:"type:varchar(15);not null"`                                                        // 链接数据库 host
	Port            sql.NullInt64  `gorm:"not null"`                                                                         // 链接数据库 port
	UserName        sql.NullString `gorm:"column:user;type:varchar(30);not null"`                                            // 链接数据库 user
	Password        sql.NullString `gorm:"column:passwd;type:varchar(30);not null"`                                          // 链接数据库 password
	UpdatedAt       mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreateAt        mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
	RollbackLogFile sql.NullString `gorm:"column:log_file;type:varchar(20)"`                                                 // 回滚binlog应用位点文件
	RollbackLogPos  sql.NullInt64  `gorm:"column:log_pos"`                                                                   // 回滚binlog应用位点
}

func (Target) TableName() string {
	return "target"
}

func (this *Target) GetHostPortStr() string {
	return fmt.Sprintf("%v:%v", this.Host.String, this.Port.Int64)
}
