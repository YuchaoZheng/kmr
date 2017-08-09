package jobgraph

// JobDescription description of a job
type JobDescription struct {
	JobNodeName        string `json:"jobName"`
	MapReduceNodeIndex int32  `json:"mrNodeIndex"`
	MapperBatchSize    int    `json:"mapperBatchSize"`
	MapperObjectSize   int    `json:"mapperObjectSize"`
	ReducerNumber      int    `json:"reducerNumber"`
}
