package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util/log"
)

var (
	errIgnore = errors.New("ignore the value")
)

func convert(b []byte, t string) (string, error) {
	switch t {
	case "string":
		return string(b), nil
	case "uint32":
		return strconv.FormatUint(uint64(binary.BigEndian.Uint32(b)), 10), nil
	case "uint64":
		return strconv.FormatUint(binary.BigEndian.Uint64(b), 10), nil
	default:
		return "", errIgnore
	}
}

func main() {
	fs := flag.NewFlagSet("kvrecord", flag.ExitOnError)
	file := fs.String("f", "", "filename")
	ktype := fs.String("keytype", "string", "keytype: string|uint32|uint64|-\n	- is for ignore")
	vtype := fs.String("valuetype", "string", "valuetype: string|uint32|uint64|-\n	- is for ignore")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal("Parse cmd arguments err:", err)
	}
	rr := records.MakeRecordReader("file", map[string]interface{}{"filename": *file})
	for rr.HasNext() {
		r := rr.Pop()

		k, kIgnore := convert(r.Key, *ktype)
		v, vIgnore := convert(r.Value, *vtype)
		switch kIgnore {
		case nil:
			switch vIgnore {
			case nil:
				fmt.Println(k, v)
			case errIgnore:
				fmt.Println(k)
				break
			}
			break
		case errIgnore:
			if vIgnore == nil {
				fmt.Println(v)
			}
			break
		}
	}
}
