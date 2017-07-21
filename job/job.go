package job

import "encoding/json"

// MapDescription MapDescription
type MapDescription struct {
	NWorker    int      `json:"nWorker"`
	Objects    []string `json:"objects"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	ReaderType string   `json:"readerType"`
	BatchSize  *int     `json:"batchSize,omitempty"` // use pointer to ensure empty is empty
}

// ReduceDescription ReduceDescription
type ReduceDescription struct {
	NWorker int      `json:"nWorker"`
	NReduce int      `json:"nReduce"`
	Image   string   `json:"image"`
	Command []string `json:"command"`
}

// BucketConfig Parameters of bucket
type BucketConfig map[string]interface{}

// BucketDescription BucketDescription
type BucketDescription struct {
	BucketType string       `json:"bucketType"`
	Config     BucketConfig `json:"config"`
}

// WorkerDescription  extra info of worker
type WorkerDescription struct {
	Volumes      []map[string]interface{} `json:"volumes,omitempty" patchStrategy:"merge"`
	VolumeMounts []map[string]interface{} `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath"`
}

// JobDescription description of a job
type JobDescription struct {
	MapBucket    BucketDescription `json:"mapBucket"`
	InterBucket  BucketDescription `json:"interBucket"`
	ReduceBucket BucketDescription `json:"reduceBucket"`
	NWorker      int               `json:"nWorker"`
	Map          MapDescription    `json:"map"`
	Reduce       ReduceDescription `json:"reduce"`
	Image        string            `json:"image"`
	Command      []string          `json:"command"`
	CPULimit     string            `json:"cpulimit"`
	WorkerDesc   WorkerDescription `json:"workerDesc"`
}

func (bucket *BucketDescription) Marshal() string {
	ret, _ := json.Marshal(*bucket)
	return string(ret)
}
