package model

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

type DataChecksum struct {
	Id              sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
	TaskUUID        sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
	SourceSchema    sql.NullString `gorm:"column:source_schema;type:varchar(100);not null"`                                         // 源 schema 名称
	SourceTable     sql.NullString `gorm:"column:source_table;type:varchar(100);not null"`                                         // 源 字段 名称
	TargetSchema    sql.NullString `gorm:"column:target_schema;type:varchar(100);not null"`                                         // 源 schema 名称
	TargetTable     sql.NullString `gorm:"column:target_table;type:varchar(100);not null"`                                         // 源 字段 名称
	IsFix           sql.NullInt64  `gorm:"column:is_fix;not null;default:0"`                                      // 表 row copy 是否完成
	MinIDValue      sql.NullString `gorm:"column:min_id_value;type:varchar(200)"`                                            // 表需要row copy 到哪一行
	MaxIDValue      sql.NullString `gorm:"column:max_id_value;type:varchar(200)"`                                           // 表当前row copy到哪一行
	UpdatedAt       mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
	CreatedAt       mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
}

func (DataChecksum) TableName() string {
	return "data_checksum"
}
