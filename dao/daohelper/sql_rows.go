package daohelper

import (
	"database/sql"
	"fmt"
	"github.com/daiguadaidai/go-d-bus/common"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

func RowsToMaps(rows *sql.Rows, columnNames []string, columnTypes []int) ([]map[string]interface{}, error) {
	rowMaps := make([]map[string]interface{}, 0, 2)

	scanArgs := make([]interface{}, len(columnNames)) // 扫描使用
	values := make([]sql.RawBytes, len(columnNames))  // 映射使用
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rowMap := make(map[string]interface{})
		//将行数据保存到record字典
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("scan字段出错. %v", err)
		}

		for i, value := range values {
			columnData, err := common.GetColumnData(value, columnTypes[i])
			if err != nil {
				return nil, fmt.Errorf("转化字段数据出错 column Name: %v. column type: %v. %v", columnNames[i], columnTypes[i], err)
			}
			rowMap[columnNames[i]] = columnData
		}

		rowMaps = append(rowMaps, rowMap)
	}

	return rowMaps, nil
}

func Row2Map(row *sql.Row, columnNames []string, columnTypes []int) (map[string]interface{}, error) {
	rowMap := make(map[string]interface{})
	columnLen := len(columnTypes)

	values := make([]interface{}, columnLen)   // 数据库原生二进制值
	scanArgs := make([]interface{}, columnLen) // 接收数据库原生二进制值，该值和上面定义的values进行关联
	for i := range values {
		scanArgs[i] = &values[i]
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			return nil, nil
		}

		return nil, fmt.Errorf("scan 字段数据错误 %v", err)
	}

	// 开始生成字段数据
	// 真的是让人摸不着头脑. 有时候 values 是 []int8, 有时候是其他基本类型,如 int, string.
	// 同样是从数据库中查询出来的. 为啥会这样
	for i, value := range values {
		columnData, err := common.GetColumnData(value, columnTypes[i])
		if err != nil {
			return nil, fmt.Errorf("转化字段数据出错 column Name: %v. column type: %v. %v", columnNames[i], columnTypes[i], err)
		}
		rowMap[columnNames[i]] = columnData
	}

	return rowMap, nil
}
