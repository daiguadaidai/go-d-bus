package sql

import (
	"database/sql"
	"strconv"
)

func RawBytes2Int8(_b sql.RawBytes) int8 {
	i, _ := strconv.ParseInt(string(_b), 10, 8)

	return int8(i)
}

func RawBytes2Int16(_b sql.RawBytes) int16 {
	i, _ := strconv.ParseInt(string(_b), 10, 16)

	return int16(i)
}

func RawBytes2Int32(_b sql.RawBytes) int32 {
	i, _ := strconv.ParseInt(string(_b), 10, 32)

	return int32(i)
}

func RawBytes2Int64(_b sql.RawBytes) int64 {
	i, _ := strconv.ParseInt(string(_b), 10, 64)

	return int64(i)
}

func RawBytes2Uint8(_b sql.RawBytes) uint8 {
	i, _ := strconv.ParseUint(string(_b), 10, 8)

	return uint8(i)
}

func RawBytes2Uint16(_b sql.RawBytes) uint16 {
	i, _ := strconv.ParseUint(string(_b), 10, 16)

	return uint16(i)
}

func RawBytes2Uint32(_b sql.RawBytes) uint32 {
	i, _ := strconv.ParseUint(string(_b), 10, 32)

	return uint32(i)
}

func RawBytes2Uint64(_b sql.RawBytes) uint64 {
	i, _ := strconv.ParseUint(string(_b), 10, 64)

	return uint64(i)
}

func RawBytes2Float32(_b sql.RawBytes) float32 {
	f, _ := strconv.ParseFloat(string(_b), 32)

	return float32(f)
}

func RawBytes2Float64(_b sql.RawBytes) float64 {
	f, _ := strconv.ParseFloat(string(_b), 64)

	return float64(f)
}
