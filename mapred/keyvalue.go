package mapred

import (
	"bytes"
	"encoding/binary"
)

//Integer64 int64
type Int64 struct{}

//UnsignedInteger64 int64
type Uint64 struct{}

//String string
type String struct{}

//Bytes []byte
type Bytes struct{}

//Int32 int32
type Int32 struct{}

//TypeClass Interface of both key and value
type TypeClass interface {
	FromBytes(b []byte) interface{}
	ToBytes(v interface{}) []byte
}

//FromBytes get value of int64, little endian
func (v Uint64) FromBytes(b []byte) interface{} {
	if len(b) != 8 {
		panic(b)
	} else {
		return binary.LittleEndian.Uint64(b)
	}
}

//ToBytes convert uint64 to bytes
func (v Uint64) ToBytes(val interface{}) []byte {
	if res, ok := val.(uint64); ok {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, res)
		return b
	}
	panic(v)
}

//FromBytes get value of int64, little endian
func (v Int64) FromBytes(b []byte) interface{} {
	if len(b) != 8 {
		panic(b)
	} else {
		var val int64
		if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val); err == nil {
			return val
		}
		panic(b)
	}
}

//ToBytes convert int64 to bytes
func (v Int64) ToBytes(val interface{}) []byte {
	if res, ok := val.(int64); ok {
		b := make([]byte, 8)
		binary.Write(bytes.NewBuffer(b), binary.LittleEndian, res)
		return b
	}
	panic(v)
}

//FromBytes get value of string
func (s String) FromBytes(b []byte) interface{} {
	return string(b)
}

//ToBytes convert string to bytes
func (s String) ToBytes(str interface{}) []byte {
	if res, ok := str.(string); ok {
		return []byte(res)
	}
	panic(str)
}

//FromBytes return bytes slice
func (bs Bytes) FromBytes(b []byte) interface{} {
	return b[:]
}

//ToBytes return bytes slice
func (bs Bytes) ToBytes(b interface{}) []byte {
	if res, ok := b.([]byte); ok {
		return res
	}
	panic(b)
}

//FromBytes int32 to bytes
func (Int32) FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		panic(b)
	} else {
		var val int32
		if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val); err == nil {
			return val
		}
		panic(b)
	}
}

//ToBytes bytes to int32
func (Int32) ToBytes(val interface{}) []byte {
	if res, ok := val.(int32); ok {
		b := make([]byte, 8)
		binary.Write(bytes.NewBuffer(b), binary.LittleEndian, res)
		return b
	}
	panic(val)
}
