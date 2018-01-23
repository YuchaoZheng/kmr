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
	"github.com/syndtr/goleveldb/leveldb"
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

func Output(key []byte, value []byte, ktype *string, vtype *string) {
	k, kIgnore := convert(key, *ktype)
	v, vIgnore := convert(value, *vtype)
	switch kIgnore {
	case nil:
		switch vIgnore {
		case nil:
			fmt.Println(k, v)
		case errIgnore:
			fmt.Println(k)
			return
		}
		break
	case errIgnore:
		if vIgnore == nil {
			fmt.Println(v)
		}
		return
	}
}

func main() {
	fs := flag.NewFlagSet("kvrecord", flag.ExitOnError)
	file := fs.String("f", "", "filename")
	filetype := fs.String("filetype", "stream", "filetype: stream|leveldb\n")
	ktype := fs.String("keytype", "string", "keytype: string|uint32|uint64|-\n	- is for ignore")
	vtype := fs.String("valuetype", "string", "valuetype: string|uint32|uint64|-\n	- is for ignore")
	keyword := fs.String("keyword", "string", "keyword: string|-\n - is for ignore")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal("Parse cmd arguments err:", err)
	}
	switch *filetype {
	case "stream":
		rr := records.MakeRecordReader("file", map[string]interface{}{"filename": *file})
		for rr.HasNext() {
			r := rr.Pop()
			Output(r.Key, r.Value, ktype, vtype)
		}
	case "leveldb":
		switch (*keyword) {
		case "-":
			db, err := leveldb.OpenFile(*file, nil)
			defer db.Close()
			if err != nil {
				log.Fatal(err)
			}
			iter := db.NewIterator(nil, nil)
			for iter.Next() {
				Output(iter.Key(), iter.Value(), ktype, vtype)
			}
			iter.Release()
			err = iter.Error()
		default:
			db, err := leveldb.OpenFile(*file, nil)
			defer db.Close()
			if err != nil {
				log.Fatal(err)
			}
			iter := db.NewIterator(nil, nil)
			for ok := iter.Seek([]byte(*keyword)); ok; ok = iter.Next() {
				Output(iter.Key(), iter.Value(), ktype, vtype)
			}
			iter.Release()
			err = iter.Error()
		}
	default:
		log.Fatal("Invalid filetype, please choose stream or leveldb")
	}
}
