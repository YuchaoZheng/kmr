package master

// JobDescription description of a job
type JobDescription struct {
	JobNodeName                    string   `json:"jobName"`
	MapredNodeIndex                int32    `json:"mapredNodeIndex"`
	MapperBatchSize                int      `json:"mapperBatchSize"`
	MapperObjectSize               int      `json:"mapperObjectSize"`
	ReducerNumber                  int      `json:"reducerNumber"`
}