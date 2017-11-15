/*
CheckPoint file format:

map <id> <commitworker>
...
reduce <id> <commitworker>
...
*/
package master

import (
	"encoding/json"
	"io/ioutil"
	"sync"
	"fmt"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/util/log"
)

type CheckPoint struct {
	mutex sync.Mutex
	ckMap map[string]bool
	key   string
	bk    bucket.Bucket
}

func (c *CheckPoint) AddCompletedJob(description TaskDescription) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// don't care the ID
	description.ID = 0

	c.ckMap[fmt.Sprint(description)] = true

	writer, err := c.bk.OpenWrite(c.key)
	defer writer.Close()
	if err != nil {
		log.Error(err)
		return
	}

	res, err := json.MarshalIndent(c.ckMap, "", "\t")
	if err != nil {
		log.Error(res, err)
		return
	}

	_, err = writer.Write(res)
	return
}

func (c *CheckPoint) IsJobCompleted(description TaskDescription) (ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// don't care the ID
	description.ID = 0

	_, ok = c.ckMap[fmt.Sprint(description)]
	return
}

func OpenCheckPoint(bk bucket.Bucket, key string) (cp *CheckPoint, err error) {
	cp = &CheckPoint{
		bk:    bk,
		key:   key,
		ckMap: make(map[string]bool),
	}

	reader, err := bk.OpenRead(key)
	if err == nil {
		byteContent, e := ioutil.ReadAll(reader)
		if e != nil {
			return
		}

		tmpMap := make(map[string]bool)
		err = json.Unmarshal(byteContent, &tmpMap)
		if err == nil {
			cp.ckMap = tmpMap
		}
	}

	err = nil
	return
}
