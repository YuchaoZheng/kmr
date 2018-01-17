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
	"time"

	"github.com/naturali/kmr/bucket"
	"github.com/naturali/kmr/util/log"
)

type TaskState struct {
	succeeded   bool
	failureTime int
}

type CheckPoint struct {
	mutex sync.Mutex
	ckMap map[string]TaskState
	key   string
	bk    bucket.Bucket
	writeFlag bool
}

func (desc TaskDescription) mapKey() string {
	desc.ID = 0
	return fmt.Sprint(desc)
}

func (c *CheckPoint) SetTaskState(desc TaskDescription, state TaskState) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.ckMap[desc.mapKey()] = state

	if state.succeeded {
		c.writeFlag = true
	}
	return
}

func (c *CheckPoint) GetTaskState(desc TaskDescription) TaskState {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// don't care the ID
	desc.ID = 0

	s, ok := c.ckMap[fmt.Sprint(desc)]
	if ok {
		return s
	} else {
		return TaskState{false, 0}
	}
}

func (c *CheckPoint) IncreaseTaskFailureTime(desc TaskDescription) {
	s := c.GetTaskState(desc)
	s.failureTime++
	c.SetTaskState(desc, s)
}

func (c *CheckPoint) MarkTaskSucceeded(desc TaskDescription) {
	s := c.GetTaskState(desc)
	s.succeeded = true
	c.SetTaskState(desc, s)
}

func OpenCheckPoint(bk bucket.Bucket, key string) (cp *CheckPoint, err error) {
	cp = &CheckPoint{
		bk:    bk,
		key:   key,
		ckMap: make(map[string]TaskState),
	}

	reader, err := bk.OpenRead(key)
	if err == nil {
		byteContent, e := ioutil.ReadAll(reader)
		if e != nil {
			return
		}

		tmpMap := make(map[string]TaskState)
		err = json.Unmarshal(byteContent, &tmpMap)
		if err == nil {
			cp.ckMap = tmpMap
		}
	}

	go func() {
		timer := time.NewTicker(10 * time.Second)
		for {
			<-timer.C
			if cp.writeFlag {
				writer, err := cp.bk.OpenWrite(cp.key)
				defer writer.Close()
				if err != nil {
					log.Error(err)
					continue
				}

				cp.mutex.Lock()
				res, err := json.MarshalIndent(cp.ckMap, "", "\t")
				cp.mutex.Unlock()
				if err != nil {
					log.Error(res, err)
					continue
				}

				_, err = writer.Write(res)
				cp.writeFlag = false
			}
		}
	}()

	err = nil
	return
}
