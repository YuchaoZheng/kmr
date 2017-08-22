package mapred

import (
	"bytes"
	"encoding/binary"

	"github.com/naturali/kmr/util/log"
)

// Int64 int64
type Int64 struct{}

// Uint64 uint64
type Uint64 struct{}

// String string
type String struct{}

// Bytes []byte
type Bytes struct{}

// Int32 int32
type Int32 struct{}

// Uint32 uint32
type Uint32 struct{}

// TypeConverter Interface of both key and value
type TypeConverter interface {
	FromBytes(b []byte) interface{}
	ToBytes(v interface{}) []byte
}

// FromBytes get value of int64, little endian
func (v Uint64) FromBytes(b []byte) interface{} {
	if len(b) != 8 {
		log.Fatalf("Convert %v failed", b)
	} else {
		return binary.BigEndian.Uint64(b)
	}
	return nil
}

// ToBytes convert uint64 to bytes
func (v Uint64) ToBytes(val interface{}) []byte {
	if res, ok := val.(uint64); ok {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, res)
		return b
	}
	log.Fatalf("Convert %v failed", v)
	return nil
}

// FromBytes get value of int64, little endian
func (v Int64) FromBytes(b []byte) interface{} {
	if len(b) != 8 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val int64
		if err := binary.Read(bytes.NewBuffer(b), binary.BigEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes convert int64 to bytes
func (v Int64) ToBytes(val interface{}) []byte {
	if res, ok := val.(int64); ok {
		b := make([]byte, 8)
		binary.Write(bytes.NewBuffer(b), binary.BigEndian, res)
		return b
	}
	log.Fatalf("Convert %v failed", v)
	return nil
}

// FromBytes convert bytes to string
func (s String) FromBytes(b []byte) interface{} {
	return string(b)
}

// ToBytes convert string to bytes
func (s String) ToBytes(str interface{}) []byte {
	if res, ok := str.(string); ok {
		return []byte(res)
	}
	log.Fatalf("Convert %v failed", str)
	return nil
}

// FromBytes return bytes slice
func (bs Bytes) FromBytes(b []byte) interface{} {
	return b[:]
}

// ToBytes return bytes self
func (bs Bytes) ToBytes(b interface{}) []byte {
	if res, ok := b.([]byte); ok {
		return res
	}
	log.Fatalf("Convert %v failed", b)
	return nil
}

// FromBytes int32 to bytes
func (Int32) FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val int32
		if err := binary.Read(bytes.NewBuffer(b), binary.BigEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes bytes to int32
func (Int32) ToBytes(val interface{}) []byte {
	if res, ok := val.(int32); ok {
		wt := new(bytes.Buffer)
		binary.Write(wt, binary.BigEndian, res)
		return wt.Bytes()
	}
	log.Fatalf("Convert %v failed", val)
	return nil
}

// FromBytes uint32 to bytes
func (Uint32) FromBytes(b []byte) interface{} {
	if len(b) != 4 {
		log.Fatalf("Convert %v failed", b)
	} else {
		var val uint32
		if err := binary.Read(bytes.NewBuffer(b), binary.BigEndian, &val); err == nil {
			return val
		}
		log.Fatalf("Convert %v failed", b)
	}
	return nil
}

// ToBytes bytes to int32
func (Uint32) ToBytes(val interface{}) []byte {
	if res, ok := val.(uint32); ok {
		wt := new(bytes.Buffer)
		binary.Write(wt, binary.BigEndian, res)
		return wt.Bytes()
	}
	log.Fatalf("Convert %v failed", val)
	return nil
}
