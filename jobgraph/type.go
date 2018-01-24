package jobgraph

// JobDescription description of a job
type JobDescription struct {
	JobNodeName   string `json:"jobName"`
	TaskNodeIndex int32  `json:"taskNodeIndex"`
}
