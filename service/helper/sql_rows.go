package helper

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
)
import _ "github.com/go-sql-driver/mysql"

func GetRows(rows *sql.Rows) ([][]interface{}, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("获取查询结果字段类型出错. %s", err.Error())
	}

	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns)) // 扫描使用
	values := make([]sql.RawBytes, len(columns))  // 映射使用
	for i := range values {
		scanArgs[i] = &values[i]
	}

	resultsz := make([][]interface{}, 0, 100)
	for rows.Next() {
		results := make([]interface{}, len(columns))
		//将行数据保存到record字典
		if err = rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("%v: scan字段出错. %v", common.CurrLine(), err)
		}

		for i, value := range values {
			if value == nil {
				results[i] = nil
				continue
			}

			// 将 rowbyte转化称想要的类型
			results[i], err = common.ConvertAssign(value, colTypes[i])
			if err != nil {
				return nil, fmt.Errorf("将 rowbytes 结果转化为需要的类型的值出错. value: %v. type: %s. %s", value, colTypes[i], err.Error())
			}
		}

		resultsz = append(resultsz, results)
	}

	return resultsz, nil
}

func GetRow(rows *sql.Rows) ([]interface{}, error) {
	newRows, err := GetRows(rows)
	if err != nil {
		return nil, err
	}

	if len(newRows) == 0 {
		return nil, nil
	}

	return newRows[0], nil
}
