package sql

import (
    "fmt"
    "testing"
)

func TestMySQLTool_Open(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := ""

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    mysqlTool.Close()
}

func TestMySQLTool_FetchAllMap(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `SELECT * FROM t_num`
    rows, err := mysqlTool.FetchAllMap(sql)
    if err != nil {
        t.Errorf("FetchAllMap发生错误. err: %v, sql: %v", err, sql)
    }
    for _, row := range rows {
        for key, value := range row {
            fmt.Println(key, " => ", value)
        }
    }

    mysqlTool.Close()
}

func TestMySQLTool_FetchAllSlice(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `SELECT * FROM t_num`
    rows, err := mysqlTool.FetchAllSlice(sql)
    if err != nil {
        t.Errorf("FetchAllSlice发生错误. err: %v, sql: %v", err, sql)
    }
    for _, row := range rows {
        fmt.Println(row)
    }

    mysqlTool.Close()
}

func TestMySQLTool_FetchOneMap(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `SELECT * FROM t_num`
    row, err := mysqlTool.FetchOneMap(sql)

    if err != nil {
        t.Errorf("FetchOneMap发生错误. err: %v, sql: %v", err, sql)
    }
    for key, value := range row {
        fmt.Println(key, " => ", value)
    }

    mysqlTool.Close()
}

func TestMySQLTool_FetchOneSlice(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `SELECT * FROM t_num`
    row, err := mysqlTool.FetchOneSlice(sql)
    if err != nil {
        t.Errorf("FetchOneSlice发生错误. err: %v, sql: %v", err, sql)
    }
    fmt.Println(row)

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteInsertMap(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}
    values := []map[string]interface{}{
        {"c2": 1, "c3": 2, "c4": 3},
        {"c2": 4, "c3": 5, "c4": 6},
        {"c2": 7, "c3": 8, "c4": 9},
    }

    _, err = mysqlTool.ExecuteInsertMap(database, table, columnNames, values, "")
    if err != nil {
        t.Errorf("执行 INSERT SQL 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteInsertSlice(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}

    values := make([][]interface{}, 3)
    value1 := []interface{}{1, 2, 3}
    value2 := []interface{}{4, 5, 6}
    value3 := []interface{}{7, 8, 9}
    values[0] = value1
    values[1] = value2
    values[2] = value3

    _, err = mysqlTool.ExecuteInsertSlice(database, table, columnNames, values, "")
    if err != nil {
        t.Errorf("执行 ExecuteInsertNormalSlice 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteDML(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `insert into test.t_num(id) values(NULL)`
    _, err = mysqlTool.ExecuteDML(sql)
    if err != nil {
        t.Errorf("执行 ExcuteDML 时发生了错误. sql: %v, err: %v", sql, err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteDMLPlaceholder(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    sql := `insert into test.t_num(c4) values(?)`
    _, err = mysqlTool.ExecuteDMLPlaceholder(sql, []interface{}{1})
    if err != nil {
        t.Errorf("执行 ExcuteDMLPlaceholder 时发生了错误. sql: %v, err: %v", sql, err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteReplaceMap(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}
    values := []map[string]interface{}{
        {"c2": 1, "c3": 2, "c4": 3},
        {"c2": 4, "c3": 5, "c4": 6},
        {"c2": 7, "c3": 8, "c4": 9},
    }

    _, err = mysqlTool.ExecuteReplaceMap(database, table, columnNames, values, "")
    if err != nil {
        t.Errorf("执行 INSERT SQL 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteInsertIgnoreMap(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}
    values := []map[string]interface{}{
        {"c2": 1, "c3": 2, "c4": 3},
        {"c2": 4, "c3": 5, "c4": 6},
        {"c2": 7, "c3": 8, "c4": 9},
    }

    _, err = mysqlTool.ExecuteInsertIgnoreMap(database, table, columnNames, values, "")
    if err != nil {
        t.Errorf("执行 INSERT SQL 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteReplaceSlice(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}

    values := make([][]interface{}, 3)
    value1 := []interface{}{1, 2, 3}
    value2 := []interface{}{4, 5, 6}
    value3 := []interface{}{7, 8, 9}
    values[0] = value1
    values[1] = value2
    values[2] = value3

    _, err = mysqlTool.ExecuteReplaceSlice(database, table, columnNames, values, "D-BUS")
    if err != nil {
        t.Errorf("执行 ExecuteInsertNormalSlice 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}

func TestMySQLTool_ExecuteInsertIgnoreSlice(t *testing.T) {
    host := "192.167.137.12"
    port := 3306
    username := "HH"
    password := "oracle"
    database := "test"

    mysqlTool := new(MySQLTool)
    err := mysqlTool.Open(host, port, username, password, database)
    if err != nil {
        t.Errorf(fmt.Sprintf("open db error: %v", err))
    }

    table := "t_num"
    columnNames := []string{"c2", "c3", "c4"}

    values := make([][]interface{}, 3)
    value1 := []interface{}{1, 2, 3}
    value2 := []interface{}{4, 5, 6}
    value3 := []interface{}{7, 8, 9}
    values[0] = value1
    values[1] = value2
    values[2] = value3

    _, err = mysqlTool.ExecuteInsertIgnoreSlice(database, table, columnNames, values, "D-BUS")
    if err != nil {
        t.Errorf("执行 ExecuteInsertNormalSlice 时发生了错误. %v", err)
    }

    mysqlTool.Close()
}
