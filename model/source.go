package model

import (
    "database/sql"

    "github.com/go-sql-driver/mysql"
    "fmt"
)

type Source struct {
    Id           sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
    TaskUUID     sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
    Host         sql.NullString `gorm:"type:varchar(15);not null"`                                                        // 链接数据库 host
    Port         sql.NullInt64  `gorm:"not null"`                                                                         // 链接数据库 port
    UserName     sql.NullString `gorm:"column:user;type:varchar(30);not null"`                                            // 链接数据库 user
    Password     sql.NullString `gorm:"column:passwd;type:varchar(30);not null"`                                          // 链接数据库 password
    UpdatedAt    mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
    CreateAt     mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
    ApplyLogFile sql.NullString `gorm:"column:log_file;type:varchar(20)"`                                                 // 当前binlog应用位点文件
    ApplyLogPos  sql.NullInt64  `gorm:"column:log_pos"`                                                                   // 当前binlog应用位点
    StartLogFile sql.NullString `gorm:"type:varchar(20)"`                                                                 // 开始binlog应用位点文件
    StartLogPos  sql.NullInt64  // 开始binlog应用位点
    ParseLogFile sql.NullString `gorm:"type:varchar(20)"` // 解析到binlog应用位点文件
    ParseLogPos  sql.NullInt64  // 解析到binlog应用位点
    StopLogFile  sql.NullString `gorm:"type:varchar(20)"` // 停止binlog应用位点文件
    StopLogPos   sql.NullInt64  // 停止binlog应用位点
}

func (Source) TableName() string {
    return "source"
}

func (this *Source) GetHostPortStr() string {
    return fmt.Sprintf("%v:%v", this.Host.String, this.Port.Int64)
}
