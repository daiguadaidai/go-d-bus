package common

import (
    "bytes"
    "encoding/binary"
    "math"
)

func Bytes2Int8(_b []byte) int8 {
    var i int8
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Int16(_b []byte) int16 {
    var i int16
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Int32(_b []byte) int32 {
    var i int32
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Int64(_b []byte) int64 {
    var i int64
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Uint8(_b []byte) uint8 {
    var i uint8
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Uint16(_b []byte) uint16 {
    var i uint16
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Uint32(_b []byte) uint32 {
    var i uint32
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Uint64(_b []byte) uint64 {
    var i uint64
    b_buf := bytes.NewBuffer(_b)
    binary.Read(b_buf, binary.BigEndian, &i)

    return i
}

func Bytes2Float32(_b []byte) float32 {
    bits := binary.BigEndian.Uint32(_b)

    return math.Float32frombits(bits)
}

func Bytes2Float64(_b []byte) float64 {
    bits := binary.BigEndian.Uint64(_b)

    return math.Float64frombits(bits)
}

func Int82Bytes(_i int8) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Int162Bytes(_i int16) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Int322Bytes(_i int32) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Int642Bytes(_i int64) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Uint82Bytes(_i uint8) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Uint162Bytes(_i uint16) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Uint322Bytes(_i uint32) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Uint642Bytes(_i uint64) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.BigEndian, _i)

    return bytesBuffer.Bytes()
}

func Float322Bytes(_f float32) []byte {
    bits := math.Float32bits(_f)
    bytes := make([]byte, 4)
    binary.BigEndian.PutUint32(bytes, bits)

    return bytes
}

func Float642Bytes(_f float64) []byte {
    bits := math.Float64bits(_f)
    bytes := make([]byte, 8)
    binary.BigEndian.PutUint64(bytes, bits)

    return bytes
}
