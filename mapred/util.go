package mapred

// ForEachValue iterate values using iterator
func ForEachValue(values ValueIterator, handler func(interface{})) {
	for v, err := values.Next(); err == nil; v, err = values.Next() {
		handler(v)
	}
}

type mapperFuncType func(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{})
type reducerFuncType func(key interface{}, valuesNext ValueIterator, output func(v interface{}), reporter interface{})

type functionMapper struct {
	MapperCommon
	userDefinedFunc mapperFuncType
	initFunc        func()
}

func (m *functionMapper) Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	m.userDefinedFunc(key, value, output, reporter)
}

func (m *functionMapper) Init() {
	m.initFunc()
}

func GetFunctionMapper(mapperFunc mapperFuncType, inkType, invType, outkType, outvType TypeConverter, initFunc func()) Mapper {
	return &functionMapper{
		MapperCommon{
			TypeConverters{
				inkType,
				invType,
				outkType,
				outvType,
			},
		},
		mapperFunc,
		initFunc,
	}
}

type functionReducer struct {
	ReducerCommon
	userDefinedFunc reducerFuncType
	initFunc        func()
}

func (m *functionReducer) Reduce(key interface{}, valuesNext ValueIterator, output func(v interface{}), reporter interface{}) {
	m.userDefinedFunc(key, valuesNext, output, reporter)
}

func (m *functionReducer) Init() {
	m.initFunc()
}

func GetFunctionReducer(reducerFunc reducerFuncType, inkType, invType, outkType, outvType TypeConverter, initFunc func()) Reducer {
	return &functionReducer{
		ReducerCommon{
			TypeConverters{
				inkType,
				invType,
				outkType,
				outvType,
			},
		},
		reducerFunc,
		initFunc,
	}
}