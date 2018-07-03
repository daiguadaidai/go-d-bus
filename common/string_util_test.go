package common

import (
	"testing"
	"fmt"
)

func TestJson2Map1(t *testing.T) {
	jsonStr := "{\"address_id\":605}"

	rs, err := Json2Map(jsonStr)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestJson2Map2(t *testing.T) { // 这样的字符串是不行的, 测试通不过
	jsonStr := "{'address_id':605}"

	rs, err := Json2Map(jsonStr)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestJson2Map3(t *testing.T) {
	jsonStr := `
    {"id1":123, "name": "HH"}
    `

	rs, err := Json2Map(jsonStr)
	if err != nil {
		t.Fatalf("json -> map 转化失败: %#v. %v", jsonStr, err)
	}
	fmt.Println(rs)
}

func TestMap2Json(t *testing.T) {
	testMap := map[string]interface{} {
		"id": 123,
		"Name": "HH",
	}

	rs, err := Map2Json(testMap)
	if err != nil {
		t.Fatalf("map -> string 转换失败: %v. %v", testMap, err)
	}
	fmt.Println(rs)
}

func TestMapAGreaterOrEqualMapB(t *testing.T) {
	mapA := map[string]interface{} {
		"id": 110,
		"name": "HH",
	}
	mapB := map[string]interface{} {
		"id": 111,
		"name": "HH",
	}

	fmt.Printf("%v >= %v: %v", mapA, mapB, MapAGreaterOrEqualMapB(mapA, mapB))
}