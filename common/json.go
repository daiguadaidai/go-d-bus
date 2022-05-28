package common

import "encoding/json"

func ToJsonStr(data interface{}) string {
	raw, err := json.Marshal(data)
	if err != nil {
		return err.Error()
	}

	return string(raw)
}

func ToJsonStrPretty(data interface{}) string {
	raw, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return err.Error()
	}

	return string(raw)
}

func ObjToMap(data interface{}) (map[string]interface{}, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var dataMap map[string]interface{}
	if err := json.Unmarshal(raw, &dataMap); err != nil {
		return nil, err
	}

	return dataMap, nil
}
