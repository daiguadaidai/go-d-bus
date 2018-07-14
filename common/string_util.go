package common

import (
	"encoding/json"
	"github.com/outbrain/golib/log"
	"time"
	"fmt"
	"strings"
	"strconv"
	"github.com/juju/errors"
)

/* json 字符串转化成 Map
Params:
    _json: 需要转化的json字符串
    _typeMap: 数据库的类型
 */
func Json2Map(_json string, _typeMap map[string]int) (map[string]interface{}, error) {
	// 用于接收json转的值
	var result map[string]interface{}
	var err error

	d := json.NewDecoder(strings.NewReader(_json))
	d.UseNumber()

	if err = d.Decode(&result); err != nil {
		return nil, err
	}

	// 如果不需要进行类型转换直接返回 json -> map 的值
	if _typeMap == nil {
		return result, nil
	}

	// 用于最终返回的map值
    returnRS := make(map[string]interface{})
    for key, value := range result {
    	switch value.(type) {
		case string:
			returnRS[key], err = String2ValueByType(value.(string), _typeMap[key])
			if err != nil {
				return nil, err
			}

		case json.Number:
			returnRS[key], err = String2ValueByType(value.(json.Number).String(), _typeMap[key])
			if err != nil {
				return nil, err
			}
		}
	}

	return returnRS, nil
}

/* json 字符串转化成 Map 通过输入 sql的类型
Params:
    _json: 需要转化的json字符串
    _sqlTypeMap: 数据库的类型
 */
func Json2MapBySqlType(_json string, _sqlTypeMap map[string]int) (map[string]interface{}, error) {
	// 用于接收json转的值
	var result map[string]interface{}
	var err error

	d := json.NewDecoder(strings.NewReader(_json))
	d.UseNumber()

	if err = d.Decode(&result); err != nil {
		return nil, err
	}

	// 如果不需要进行类型转换直接返回 json -> map 的值
	if _sqlTypeMap == nil {
		return result, nil
	}

	// 用于最终返回的map值
	returnRS := make(map[string]interface{})
	for key, value := range result {
		switch value.(type) {
		case string:
			returnRS[key], err = String2GoValueBySqlType(value.(string), _sqlTypeMap[key])
			if err != nil {
				return nil, err
			}

		case json.Number:
			returnRS[key], err = String2GoValueBySqlType(value.(json.Number).String(), _sqlTypeMap[key])
			if err != nil {
				return nil, err
			}
		}
	}

	return returnRS, nil
}

/* map 转化成 json
Params:
    _map: 需要转化成 json 的map
 */
func Map2Json(_map map[string]interface{}) (string, error) {
	b, err := json.Marshal(_map)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

/* 比较 mapA 是否 >= mapB
1. 先比较元素个数, 元素多的更大
2. 循环 mapA 比较 mapB
Params:
    _mapA: 第一个map
    _mapB: 第二个map
 */
func MapAGreaterOrEqualMapB(_mapA map[string]interface{}, _mapB map[string]interface{}) bool {
	fmt.Println(_mapA, _mapB)
	mapALen := len(_mapA)
	mapBLen := len(_mapB)
    if mapALen == mapBLen { // 元素个数相等需要比较里面的值
        for keyA, valueA := range _mapA {
            if valueB, ok := _mapB[keyA]; ok { // 两个 map 中都有值
                if GreaterOrEqual(valueA, valueB) { // valueA >= valueB, 这是我们想要的
					continue
				} else {
					return false
				}
			} else {
				log.Warningf("%v: 失败. map比较, key: %v. mapA中有数据在mapB中找不到. %v <=> %v",
					CurrLine(), keyA, _mapA, _mapB)
				return false
			}
		}
	} else if mapALen < mapBLen {
		return true
	} else if mapALen > mapBLen {
		return false
	}

    return true
}

/* 比较 _dataA 是否小于 _dataB
Params:
    _dataA: 第一个值
    _dataB: 第二个值
 */
func GreaterOrEqual(_dataA, _dataB interface{}) bool {
	switch valueA := _dataA.(type) {
	case string:
		valueB := _dataB.(string)
		return valueA >= valueB
	case int8:
		valueB := _dataB.(int8)
		return valueA >= int8(valueB)
	case int16:
		valueB := _dataB.(int16)
		return valueA >= valueB
	case int32:
		valueB := _dataB.(int32)
		return valueA >= valueB
	case int:
		valueB := _dataB.(int)
		return valueA >= valueB
	case int64:
		valueB := _dataB.(int64)
		return valueA >= valueB
	case uint8:
		valueB := _dataB.(uint8)
		return valueA >= valueB
	case uint16:
		valueB := _dataB.(uint16)
		return valueA >= valueB
	case uint32:
		valueB := _dataB.(uint32)
		return valueA >= valueB
	case uint:
		valueB := _dataB.(uint)
		return valueA >= valueB
	case uint64:
		valueB := _dataB.(uint64)
		return valueA >= valueB
	case float32:
		valueB := _dataB.(float32)
		return valueA >= valueB
	case float64:
		valueB := _dataB.(float64)
		return valueA >= valueB
	case complex64:
		valueB := _dataB.(complex64)
		if real(valueA) == real(valueB) && imag(valueA) == imag(valueB) {
			return true
		} else if real(valueA) > real(valueB) && imag(valueA) >= imag(valueB) {
			return true
		} else if real(valueA) >= real(valueB) && imag(valueA) > imag(valueB) {
			return true
    	} else if real(valueA) > real(valueB) && imag(valueA) > imag(valueB) {
            return true
		} else {
			return false
		}
	case complex128:
		valueB := _dataB.(complex128)
		if real(valueA) == real(valueB) && imag(valueA) == imag(valueB) {
			return true
		} else if real(valueA) > real(valueB) && imag(valueA) >= imag(valueB) {
			return true
		} else if real(valueA) >= real(valueB) && imag(valueA) > imag(valueB) {
			return true
		} else if real(valueA) > real(valueB) && imag(valueA) > imag(valueB) {
			return true
		} else {
			return false
		}
	}

	return false
}

// 获取当前时间戳 毫秒级别
func GetCurrentTimestampMS() string {
	t := time.Now()

	return fmt.Sprintf("%v", t.Format("20060102150405123456"))
}

/* string转化称相关类型的值
Params:
    _value: 传入的字符串值
    _type: 需要转化的类型
 */
func String2ValueByType(_value string, _type int) (interface{}, error) {
	switch _type {
	case GO_TYPE_INT, GO_TYPE_INT8, GO_TYPE_INT16, GO_TYPE_INT32, GO_TYPE_INT64:

		data, err := strconv.Atoi(_value)
		if err != nil {
			return nil, err
		}
		return data, nil

	case GO_TYPE_STRING:

		return _value, nil

	case GO_TYPE_FLOAT, GO_TYPE_FLOAT32, GO_TYPE_FLOAT64:

		return _value, nil

	case GO_TYPE_BOOL:

		if strings.ToUpper(_value) == "TRUE" {
			return true, nil
		} else if strings.ToUpper(_value) == "FALSE" {
			return false, nil
		} else {
			log.Warningf("%v: 将字符串转为化bool类型遇到(未知数据): %v," +
				"将此数据转化为 false",
				CurrLine(), _value)
			return false, nil
		}
	}

	errMSG := fmt.Sprintf("%v: 失败. 转化数据库字段信息出错遇到未知类型", CurrLine())
	return -1, errors.New(errMSG)
}
