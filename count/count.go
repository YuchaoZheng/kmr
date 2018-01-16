package count

type CountInterface interface {
	Add(key string, value interface{})
	GetValue(key string) interface{}
}

type CountMessage struct {
	WorkerToMasterMap map[string]int64
}

func (rm *CountMessage) Add(key string, value interface{}) {
	rm.WorkerToMasterMap[key] += value.(int64)
}

func (rm *CountMessage) GetValue(key string) interface{} {
	return rm.WorkerToMasterMap[key]
}
