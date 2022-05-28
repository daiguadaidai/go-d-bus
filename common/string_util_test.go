package common

import (
	"fmt"
	"testing"
)

func TestJson2Map1(t *testing.T) {
	jsonStr := "{\"address_id\":605}"
	jsonType := map[string]int{
		"address_id": GO_TYPE_INT,
	}

	rs, err := Json2Map(jsonStr, jsonType)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestJson2Map2(t *testing.T) { // 这样的字符串是不行的, 测试通不过
	jsonStr := "{'address_id':605}"
	jsonType := map[string]int{
		"address_id": GO_TYPE_INT,
	}

	rs, err := Json2Map(jsonStr, jsonType)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestJson2Map3(t *testing.T) {
	jsonStr := `
    {"id1":123, "name": "HH"}
    `
	jsonType := map[string]int{
		"id1":  GO_TYPE_INT,
		"name": GO_TYPE_STRING,
	}

	rs, err := Json2Map(jsonStr, jsonType)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestMap2Json(t *testing.T) {
	testMap := map[string]interface{}{
		"id":   123,
		"Name": "HH",
	}

	rs, err := Map2Json(testMap)
	if err != nil {
		t.Fatalf("map -> string 转换失败: %v. %v", testMap, err)
	}
	fmt.Println(rs)
}

func TestJson2MapBySqlType(t *testing.T) {
	jsonStr := `
    {"id1":123, "name": "HH"}
    `
	sqlType := map[string]int{
		"id1":  MYSQL_TYPE_BIGINT,
		"name": MYSQL_TYPE_VARCHAR,
	}

	rs, err := Json2MapBySqlType(jsonStr, sqlType)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}
