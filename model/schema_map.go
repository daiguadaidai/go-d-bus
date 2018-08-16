package model

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

type SchemaMap struct {
	Id        sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
	TaskUUID  sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
	Source    sql.NullString `gorm:"column:source;type:varchar(100);not null"`                                         // 源 字段 名称
	Target    sql.NullString `gorm:"column:target;type:varchar(100);not null"`                                         // 目标 字段 名称
	UpdatedAt mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreatedAt mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
}

func (SchemaMap) TableName() string {
	return "schema_map"
}
