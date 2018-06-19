package common

import (
	"encoding/json"
	"github.com/outbrain/golib/log"
)

/* json 字符串转化成 Map
Params:
    _json: 需要转化的json字符串
 */
func Json2Map(_json string) (map[string]interface{}, error) {
	var result map[string]interface{}

	if err := json.Unmarshal([]byte(_json), &result); err != nil {
		return nil, err
	}
	return result, nil
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

/* 比较 mapA 是否 小于 mapB
1. 先比较元素个数, 元素多的更大
2. 循环 mapA 比较 mapB
Params:

 */
func MapAGreaterOrEqualMapB(_mapA map[string]interface{}, _mapB map[string]interface{}) bool {
	mapALen := len(_mapA)
	mapBLen := len(_mapB)
    if mapALen == mapBLen { // 元素个数相等需要比较里面的值
        for keyA, valueA := range _mapA {
            if valueB, ok := _mapB[keyA]; ok { // 两个 map 中都有值
                if GreaterOrEqual(valueA, valueB) { // valueA 小于valueB, 这是我们想要的
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
		return valueA >= valueB
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