package gdbc

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"testing"
)

func Test_ShardingProxy_ReplaceInto(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	sqlStr := `REPLACE INTO employees VALUES (10001, '1953-09-02', 'Georgi', 'Facello', 'M', '1986-06-26')`

	if _, err := db.Exec(sqlStr); err != nil {
		t.Fatal(err.Error())
	}
}

func Test_ShardingProxy_Prepare_ReplaceInto(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	emp_no := 10001
	d := "1953-09-02"
	firstName := "Georgi"
	lastName := "Facello"
	male := "M"
	d2 := "1986-06-26"

	sqlStr := `REPLACE INTO employees VALUES (?, ?, ?, ?, ?, ?)`

	if _, err := db.Exec(sqlStr, emp_no, d, firstName, lastName, male, d2); err != nil {
		t.Fatal(err.Error())
	}
}

func Test_ShardingProxy_Prepare_ReplaceIntoMulti(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	emp_no := 10001
	d := "1953-09-02"
	firstName := "Georgi"
	lastName := "Facello"
	male := "M"
	d2 := "1986-06-26"

	a_emp_no := 10002
	a_d := "1953-09-02"
	a_firstName := "Georgi"
	a_lastName := "Facello"
	a_male := "M"
	a_d2 := "1986-06-26"

	// sqlStr := `REPLACE INTO employees VALUES (?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?)`
	sqlStr := `INSERT IGNORE INTO employees VALUES (?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?)`

	if _, err := db.Exec(sqlStr, emp_no, d, firstName, lastName, male, d2, a_emp_no, a_d, a_firstName, a_lastName, a_male, a_d2); err != nil {
		t.Fatal(err.Error())
	}
}

func Test_ShardingProxy_Prepare_ReplaceIntoMulti1(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	datas := make([]interface{}, 0, 20)
	datas = append(datas, 10001)
	datas = append(datas, "1953-09-02")
	datas = append(datas, "Georgi")
	datas = append(datas, "Facello")
	datas = append(datas, "M")
	datas = append(datas, "1986-06-26")
	datas = append(datas, 10002)
	datas = append(datas, "1953-09-02")
	datas = append(datas, "Georgi")
	datas = append(datas, "Facello")
	datas = append(datas, "M")
	datas = append(datas, "1986-06-26")

	// sqlStr := `REPLACE INTO employees VALUES (?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?)`
	sqlStr := `REPLACE INTO employees VALUES (?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?)`

	if _, err := db.Exec(sqlStr, datas...); err != nil {
		t.Fatal(err.Error())
	}
}

func Test_ShardingProxy_Prepare_ReplaceIntoMulti2(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	datas := make([]interface{}, 0, 20)

	size := 1
	plasholds := make([]string, 0, size)
	for i := 0; i < size; i++ {
		datas = append(datas, i)
		datas = append(datas, "1953-09-02")
		datas = append(datas, "Georgi")
		datas = append(datas, "Facello")
		datas = append(datas, "M")
		datas = append(datas, "1986-06-26")
		plasholds = append(plasholds, "(?, ?, ?, ?, ?, ?)")
	}

	// sqlStr := `REPLACE INTO employees VALUES (?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?)`
	sqlStr := fmt.Sprintf(`REPLACE INTO employees VALUES %v`, strings.Join(plasholds, ", "))
	fmt.Println(sqlStr)
	fmt.Println(datas)

	if _, err := db.Exec(sqlStr, datas...); err != nil {
		t.Fatal(err.Error())
	}
}

func Test_ShardingProxy_Prepare_ReplaceIntoMulti3(t *testing.T) {
	host := "127.0.0.1"
	port := 13306
	userName := "root"
	password := "123456"
	database := "_open_trade_data_center_test"
	dns := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&allowOldPasswords=0", userName, password, host, port, database)

	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Errorf("Error: %v", err)
		return
	}
	defer db.Close()

	datas := make([]interface{}, 0, 20)

	size := 100
	plasholds := make([]string, 0, size)
	for i := 0; i < size; i++ {
		datas = append(datas, i)
		datas = append(datas, "1953-09-02")
		datas = append(datas, "Georgi")
		datas = append(datas, "Facello")
		datas = append(datas, "M")
		datas = append(datas, "1986-06-26")
		plasholds = append(plasholds, "(?, ?, ?, ?, ?, ?)")
	}

	sqlStr := fmt.Sprintf( /* go-d-bus */ ` REPLACE INTO _open_trade_data_center_test.employees VALUES %v`, strings.Join(plasholds, ", "))
	fmt.Println(sqlStr)
	fmt.Println(datas)

	if _, err := db.Exec(sqlStr, datas...); err != nil {
		t.Fatal(err.Error())
	}
}
