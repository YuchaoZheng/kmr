package job

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"github.com/naturali/kmr/util"
)

type LocalConfig struct {
	MapBucket    *BucketDescription `json:"mapBucket"`
	InterBucket  *BucketDescription `json:"interBucket"`
	ReduceBucket *BucketDescription `json:"reduceBucket"`
}

type RemoteConfig struct {
	MapBucket    *BucketDescription `json:"mapBucket"`
	InterBucket  *BucketDescription `json:"interBucket"`
	ReduceBucket *BucketDescription `json:"reduceBucket"`
	PodDesc      *util.PodDescription    `json:"podDescription"`

	DockerRegistry *string `json:"dockerRegistry"`
	Namespace      *string `json:"namespace"`
	ServiceAccount *string `json:"serviceAccount"`
}

type Config struct {
	Local  *LocalConfig  `json:"local"`
	Remote *RemoteConfig `json:"remote"`
}

func overrideStructV(main reflect.Value, extra reflect.Value) {
	if main.Kind() == reflect.Ptr {
		if main.IsNil() && !extra.IsNil() {
			main.Set(extra)
			return
		}
		if !main.IsNil() && !extra.IsNil() && main.Elem().Kind() != reflect.Struct {
			main.Set(extra)
			return
		}
		main = main.Elem()
		extra = extra.Elem()
	}
	if extra.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < extra.NumField(); i++ {
		extraValueField := extra.Field(i)
		mainValueField := main.Field(i)

		if extraValueField.Kind() != reflect.Ptr {
			mainValueField.Set(extraValueField)
			continue
		}

		if mainValueField.IsNil() && !extraValueField.IsNil() {
			mainValueField.Set(extraValueField)
			continue
		}

		if !mainValueField.IsNil() && !extraValueField.IsNil() {
			overrideStructV(mainValueField, extraValueField)
		}
	}
}

func overrideStruct(main interface{}, extra interface{}) {
	if reflect.TypeOf(main).Name() != reflect.TypeOf(extra).Name() {
		panic("main and extra struct has different type")
	}
	overrideStructV(reflect.ValueOf(main).Elem(), reflect.ValueOf(extra).Elem())
}

// LoadConfigFromMultiFiles load config from files, right config will override left config
func (j *JobGraph) loadConfigFromMultiFiles(configFiles ...string) *Config {
	config := &Config{}
	for _, file := range configFiles {
		if f, err := os.Open(file); err == nil {
			newconfig := &Config{}
			b, err := ioutil.ReadAll(f)
			if err != nil {
				continue
			}
			repMap := map[string]string {
				"${JOBNAME}": strings.Replace(j.Name, " ", "", -1),
			}
			for k,v := range repMap {
				b = []byte(strings.Replace(string(b), k, v, -1))
			}
			json.Unmarshal(b, newconfig)
			overrideStruct(config, newconfig)
		}
	}
	return config
}

