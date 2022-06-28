package common

import (
	"bytes"
	"fmt"
	"testing"
)

func TestInt2Bytes(t *testing.T) {
	var i8 int8 = -1
	b8 := Int82Bytes(i8)
	if !bytes.Equal(b8, []byte{0xff}) {
		t.Errorf("int8 不能转化成 bytes, int8: %v, bytes: %v", i8, b8)
	}

	var i16 int16 = -1
	b16 := Int162Bytes(i16)
	if !bytes.Equal(b16, []byte{0xff, 0xff}) {
		t.Errorf("int16 不能转化成 bytes, int16: %v, bytes: %v", i16, b16)
	}

	var i32 int32 = -1
	b32 := Int322Bytes(i32)
	if !bytes.Equal(b32, []byte{0xff, 0xff, 0xff, 0xff}) {
		t.Errorf("int32 不能转化成 bytes, int32: %v, bytes: %v", i32, b32)
	}

	var i64 int64 = -1
	b64 := Int642Bytes(i64)
	if !bytes.Equal(b64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) {
		t.Errorf("int64 不能转化成 bytes, int64: %v, bytes: %v", i64, b64)
	}

	var ui8 uint8 = 255
	ub8 := Uint82Bytes(ui8)
	if !bytes.Equal(ub8, []byte{0xff}) {
		t.Errorf("uint8 不能转化成 bytes, uint8: %v, bytes: %v", ui8, ub8)
	}

	var ui16 uint16 = 65535
	ub16 := Uint162Bytes(ui16)
	if !bytes.Equal(ub16, []byte{0xff, 0xff}) {
		t.Errorf("uint16 不能转化成 bytes, uint16: %v, bytes: %v", ui16, ub16)
	}

	var ui32 uint32 = 4294967295
	ub32 := Uint322Bytes(ui32)
	if !bytes.Equal(ub32, []byte{0xff, 0xff, 0xff, 0xff}) {
		t.Errorf("uint32 不能转化成 bytes, uint32: %v, bytes: %v", ui32, ub32)
	}

	var ui64 uint64 = 18446744073709551615
	ub64 := Uint642Bytes(ui64)
	if !bytes.Equal(ub64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) {
		t.Errorf("uint64 不能转化成 bytes, uint64: %v, bytes: %v", ui64, ub64)
	}
}

func TestBytes2Int(t *testing.T) {
	b8 := []byte{0xff}
	i8 := Bytes2Int8(b8)
	if i8 != -1 {
		t.Errorf("bytes 不能转化成 int8, bytes: %v, int8: %v", b8, i8)
	}

	b16 := []byte{0xff, 0xff}
	i16 := Bytes2Int16(b16)
	if i16 != -1 {
		t.Errorf("bytes 不能转化成 int16, bytes: %v, int16: %v", b16, i16)
	}

	b32 := []byte{0xff, 0xff, 0xff, 0xff}
	i32 := Bytes2Int32(b32)
	if i32 != -1 {
		t.Errorf("bytes 不能转化成 int32, bytes: %v, int32: %v", b32, i32)
	}

	b64 := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	i64 := Bytes2Int64(b64)
	if i64 != -1 {
		t.Errorf("bytes 不能转化成 int64, bytes: %v, int64: %v", b64, i64)
	}

	ub8 := []byte{0xff}
	ui8 := Bytes2Uint8(ub8)
	if ui8 != 255 {
		t.Errorf("bytes 不能转化成 uint8, bytes: %v, uint8: %v", ub8, ui8)

	}

	ub16 := []byte{0xff, 0xff}
	ui16 := Bytes2Uint16(ub16)
	if ui16 != 65535 {
		t.Errorf("bytes 不能转化成 uint16, bytes: %v, uint16: %v", ub16, ui16)
	}

	ub32 := []byte{0xff, 0xff, 0xff, 0xff}
	ui32 := Bytes2Uint32(ub32)
	if ui32 != 4294967295 {
		t.Errorf("bytes 不能转化成 uint32, bytes: %v, uint32: %v", ub32, ui32)
	}

	ub64 := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	ui64 := Bytes2Uint64(ub64)
	if ui64 != 18446744073709551615 {
		t.Errorf("bytes 不能转化成 uint64, bytes: %v, uint64: %v", ub64, ui64)
	}
}

func TestBytes2Float(t *testing.T) {
	b32 := []byte{0x00, 0xff, 0xff, 0xff}
	f32 := Bytes2Float32(b32)
	if f32 != 2.3509886e-38 {
		t.Errorf("bytes 不能转化成 float32, bytes: %v, float32: %v", b32, f32)
	}

	b64 := []byte{0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	f64 := Bytes2Float64(b64)
	if f64 != 7.291122019556397e-304 {
		t.Errorf("bytes 不能转化成 float64, bytes: %v, float64: %v", b64, f64)
	}
}

type TmpColumn struct {
	Name       string   // 字段名字
	IsSet      bool     // 是否是 集合
	EnumValues []string // 枚举的值
}

func TestConvter(t *testing.T) {
	tmpColumn1 := TmpColumn{
		Name:       "HH1",
		IsSet:      true,
		EnumValues: []string{"a1", "b1"},
	}

	var tmpColumn2 TmpColumn
	fmt.Println(tmpColumn1)
	fmt.Println(tmpColumn2)
	if err := DeepCopy(&tmpColumn2, tmpColumn1); err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(tmpColumn1)
	fmt.Println(tmpColumn2)
	tmpColumn2.Name = "HH2"
	tmpColumn2.EnumValues[0] = "a2"
	fmt.Println(tmpColumn1)
	fmt.Println(tmpColumn2)
}
