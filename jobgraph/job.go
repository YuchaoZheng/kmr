package jobgraph

// BucketConfig Parameters of bucket
type BucketConfig map[string]interface{}

// BucketDescription BucketDescription
type BucketDescription struct {
	BucketType string       `json:"bucketType"`
	Config     BucketConfig `json:"config"`
}

// JobDescription description of a job
type JobDescription struct {
	JobNodeName                    string   `json:"jobName"`
	MapredNodeIndex                int32    `json:"mapredNodeIndex"`
	MapperBatchSize                int      `json:"mapperBatchSize"`
	MapperObjectSize               int      `json:"mapperObjectSize"`
	ReducerNumber                  int      `json:"reducerNumber"`
}