package helper

import (
	"github.com/daiguadaidai/go-d-bus/common"
	"github.com/daiguadaidai/go-d-bus/logger"
)

/* 比较 mapA 是否 >= mapB
1. 先比较元素个数, 元素多的更大
2. 循环 mapA 比较 mapB
Params:
    _mapA: 第一个map
    _mapB: 第二个map
*/
func MapAGreaterOrEqualMapB(_mapA map[string]interface{}, _mapB map[string]interface{}) bool {
	mapALen := len(_mapA)
	mapBLen := len(_mapB)
	if mapALen == mapBLen { // 元素个数相等需要比较里面的值
		for keyA, valueA := range _mapA {
			if valueB, ok := _mapB[keyA]; ok { // 两个 map 中都有值
				if common.GreaterOrEqual(valueA, valueB) { // valueA >= valueB, 这是我们想要的
					continue
				} else {
					return false
				}
			} else {
				logger.M.Warnf("失败. map比较, key: %v. mapA中有数据在mapB中找不到. %v <=> %v", keyA, _mapA, _mapB)
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

/* 比较 mapA 是否 >= mapB
1. 先比较元素个数, 元素多的更大
2. 循环 mapA 比较 mapB
Params:
    _mapA: 第一个map
    _mapB: 第二个map
*/
func MapAGreaterMapB(_mapA map[string]interface{}, _mapB map[string]interface{}) bool {
	mapALen := len(_mapA)
	mapBLen := len(_mapB)
	if mapALen == mapBLen { // 元素个数相等需要比较里面的值
		for keyA, valueA := range _mapA {
			if valueB, ok := _mapB[keyA]; ok { // 两个 map 中都有值
				if common.Greater(valueA, valueB) { // valueA >= valueB, 这是我们想要的
					continue
				} else {
					return false
				}
			} else {
				logger.M.Warnf("失败. map比较, key: %v. mapA中有数据在mapB中找不到. %v <=> %v", keyA, _mapA, _mapB)
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
