package common

import "math/rand"

/* 随机获取一个map的key
将map的key放在一个列表中, value为true的才放
Params:
    _data: 需要的需要的数据
 */
func GetRandomMapKey(_data map[string]bool) (string, bool) {
	keySlice := make([]string, 0, 10)
	for key, value := range _data {
        if value {
			keySlice = append(keySlice, key)
		}
	}

	keySliceLen := len(keySlice)
	if keySliceLen == 0 { // 没有元素放回失败
		return "", false
	}

	// 有元素获取随机元素
	return keySlice[rand.Intn(keySliceLen)], true
}
