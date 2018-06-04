package model

import (
    "database/sql"

    "github.com/go-sql-driver/mysql"
)

type TableMap struct {
    Id              sql.NullInt64  `gorm:"primary_key;not null;AUTO_INCREMENT"`                                              // 主键ID
    TaskUUID        sql.NullString `gorm:"column:task_uuid;type:varchar(22);not null"`                                       // 任务UUID
    Schema          sql.NullString `gorm:"column:schema;type:varchar(100);not null"`                                         // 源 schema 名称
    Source          sql.NullString `gorm:"column:source;type:varchar(100);not null"`                                         // 源 字段 名称
    Target          sql.NullString `gorm:"column:target;type:varchar(100);not null"`                                         // 目标 字段 名称
    RowCopyComplete sql.NullInt64  `gorm:"column:row_copy_complete;not null;default:0"`                                      // 表 row copy 是否完成
    MaxIDValue      sql.NullString `gorm:"column:max_id_value;type:varchar(200)"`                                            // 表需要row copy 到哪一行
    CurrIDValue     sql.NullString `gorm:"column:curr_id_value;type:varchar(200)"`                                           // 表当前row copy到哪一行
    UpdatedAt       mysql.NullTime `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` // 更新时间
    CreateAt        mysql.NullTime `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP"`                             // 创建时间
}

func (TableMap) TableName() string {
    return "table_map"
}
