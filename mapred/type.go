package mapred

import (
	"bytes"
	"encoding/binary"

	"github.com/naturali/kmr/util/log"
)

// typeInt64 int64
type typeInt64 struct{}
var Int64 typeInt64

// typeUint64 uint64
type typeUint64 struct{}
var Uint64 typeUint64

// typeString string
type typeString struct{}
var String typeString

// typeBytes []byte
type typeBytes struct{}
var Bytes typeBytes

// typeInt32 int32
type typeInt32 struct{}
var Int32 typeInt32

// typeUint32 uint32
type typeUint32 struct{}
var Uint32 typeUint32

// TypeConverter typeInterface of both key and value
type TypeConverter interface {
	FromBytes(b []byte) interface{}
	ToBytes(v interface{}) []byte
}

// FromtypeBytes get value of int64, little endian
func (v typeUint64) FromtypeBytes(b []byte) interface{} {
	if len(b) != 8 {
		log.Fatalf("Convert %v failed", b)
	} else {
		return binary.LittleEndian.Uint64(b)
	}
	return nil
}

// ToBytes convert uint64 to bytes
func (v typeUint64) ToBytes(val interface{}) []byte {
	if res, ok := val.(uint64); ok {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, res)
		return b
	}
	log.Fatalf("Convert %v failed", v)
	return nil
}

// FromtypeBytes get value of int64, little endian
func (v typeInt64) FromtypeBytes(b []byte) interface{} {
	if len(b) != 8 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val int64
		if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes convert int64 to bytes
func (v typeInt64) ToBytes(val interface{}) []byte {
	if res, ok := val.(int64); ok {
		b := make([]byte, 8)
		binary.Write(bytes.NewBuffer(b), binary.LittleEndian, res)
		return b
	}
	log.Fatalf("Convert %v failed", v)
	return nil
}

// FromtypeBytes convert bytes to string
func (s typeString) FromBytes(b []byte) interface{} {
	return string(b)
}

// ToBytes convert string to bytes
func (s typeString) ToBytes(str interface{}) []byte {
	if res, ok := str.(string); ok {
		return []byte(res)
	}
	log.Fatalf("Convert %v failed", str)
	return nil
}

// FromtypeBytes return bytes slice
func (bs typeBytes) FromBytes(b []byte) interface{} {
	return b[:]
}

// ToBytes return bytes self
func (bs typeBytes) ToBytes(b interface{}) []byte {
	if res, ok := b.([]byte); ok {
		return res
	}
	log.Fatalf("Convert %v failed", b)
	return nil
}

// FromtypeBytes int32 to bytes
func (typeInt32) FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val int32
		if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes bytes to int32
func (typeInt32) ToBytes(val interface{}) []byte {
	if res, ok := val.(int32); ok {
		wt := new(bytes.Buffer)
		binary.Write(wt, binary.LittleEndian, res)
		return wt.Bytes()
	}
	log.Fatalf("Convert %v failed", val)
	return nil
}

// FromtypeBytes uint32 to bytes
func (typeUint32) FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val uint32
		if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes bytes to int32
func (typeUint32) ToBytes(val interface{}) []byte {
	if res, ok := val.(uint32); ok {
		wt := new(bytes.Buffer)
		binary.Write(wt, binary.LittleEndian, res)
		return wt.Bytes()
	}
	log.Fatalf("Convert %v failed", val)
	return nil
}