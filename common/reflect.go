package common

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
)

const (
	JS_SAFE_MAX_INT = 9007199254740992
)

func CallFunction(fn interface{}, params []interface{}) []reflect.Value {
	f := reflect.ValueOf(fn)
	inputs := make([]reflect.Value, len(params))
	for k, in := range params {
		inputs[k] = reflect.ValueOf(in)
	}
	return f.Call(inputs)
}

func GetInterfaceFieldValue(v interface{}, fieldName string) (interface{}, error) {
	val := reflect.ValueOf(v)
	return reflectGetFieldValue(val, fieldName)
}

func reflectGetFieldValue(val reflect.Value, fieldName string) (interface{}, error) {
	if val.IsValid() && val.CanInterface() {
		typ := val.Type()
		switch typ.Kind() {
		case reflect.Struct:
			return val.FieldByName(fieldName).Interface(), nil
		case reflect.Ptr:
			return reflectGetFieldValue(val.Elem(), fieldName)
		}
	} else {
		return nil, fmt.Errorf("无效的数据结构: %v", val.String())
	}

	return nil, nil
}

func SetInterfaceFieldValue(v interface{}, fieldName string, fieldValue interface{}) error {
	val := reflect.ValueOf(v)
	return reflectSetFieldValue(val, fieldName, fieldValue)
}

func reflectSetFieldValue(val reflect.Value, fieldName string, fieldValue interface{}) error {
	if val.IsValid() && val.CanInterface() {
		typ := val.Type()
		switch typ.Kind() {
		case reflect.Struct:
			reflectSetValue(val, fieldName, fieldValue)
			return nil
		case reflect.Ptr:
			return reflectSetFieldValue(val.Elem(), fieldName, fieldValue)
		}
	} else {
		return fmt.Errorf("无效的数据结构: %v", val.String())
	}

	return nil
}

func reflectSetValue(val reflect.Value, fieldName string, fieldValue interface{}) error {

	switch v := fieldValue.(type) {
	case int8:
		val.FieldByName(fieldName).SetInt(int64(v))
	case int16:
		val.FieldByName(fieldName).SetInt(int64(v))
	case int32:
		val.FieldByName(fieldName).SetInt(int64(v))
	case int64:
		val.FieldByName(fieldName).SetInt(v)
	case int:
		val.FieldByName(fieldName).SetInt(int64(v))
	case uint8:
		val.FieldByName(fieldName).SetUint(uint64(v))
	case uint16:
		val.FieldByName(fieldName).SetUint(uint64(v))
	case uint32:
		val.FieldByName(fieldName).SetUint(uint64(v))
	case uint64:
		val.FieldByName(fieldName).SetUint(v)
	case uint:
		val.FieldByName(fieldName).SetUint(uint64(v))
	case float32:
		val.FieldByName(fieldName).SetFloat(float64(v))
	case float64:
		val.FieldByName(fieldName).SetFloat(v)
	case string:
		val.FieldByName(fieldName).SetString(v)
	case []byte:
		val.FieldByName(fieldName).SetBytes(v)
	case bool:
		val.FieldByName(fieldName).SetBool(v)
	default:
		return fmt.Errorf("字段:%s, 值:%v, 值类型:%T, 类型名:%s 无法设置值", fieldName, fieldValue, fieldValue, val.String())
	}

	return nil
}

func ConvertAssign(bytes sql.RawBytes, typ *sql.ColumnType) (interface{}, error) {
	switch typ.ScanType().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		d, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return nil, err
		}
		return d, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d, err := strconv.ParseUint(string(bytes), 10, 64)
		if err != nil {
			return nil, err
		}
		return d, nil
	case reflect.Float32, reflect.Float64:
		return strconv.ParseFloat(string(bytes), 64)
	case reflect.Struct:
		switch typ.ScanType().String() {
		case "sql.NullInt64":
			d, err := strconv.ParseInt(string(bytes), 10, 64)
			if err != nil {
				return nil, err
			}
			return d, nil
		case "sql.NullFloat64":
			return strconv.ParseFloat(string(bytes), 64)
		case "sql.NullString":
			return string(bytes), nil
		case "sql.NullBool":
			return strconv.ParseBool(string(bytes))
		case "sql.NullTime":
			return string(bytes), nil
		case "mysql.NullTime":
			return string(bytes), nil
		case "time.Time":
			return string(bytes), nil
		default:
			return string(bytes), nil
		}
	default:
		return string(bytes), nil
	}
}
