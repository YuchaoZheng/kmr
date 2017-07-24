package mapred

//InputClassBase input class base
type InputClassBase interface {
	GetInputKeyClass() TypeClass
	GetInputValueClass() TypeClass
}

//OutputClassBase input class base
type OutputClassBase interface {
	GetOutputKeyClass() TypeClass
	GetOutputValueClass() TypeClass
}

//InputOutputClassBase input class base
type InputOutputClassBase interface {
	InputClassBase
	OutputClassBase
}

//MapperReducerBase some common func
type MapperReducerBase interface {
	BeforeRun()
}

//Mapper Mapper interface
type Mapper interface {
	InputOutputClassBase
	MapperReducerBase
	Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{})
}

//Reducer Reducer interface
type Reducer interface {
	InputOutputClassBase
	MapperReducerBase
	Reduce(key interface{}, valuesNext func() (interface{}, error), output func(k interface{}, v interface{}), reporter interface{})
}

//MapReduceBase Mapper and Reducer base class
type MapReduceBase struct {
	InputKeyClass    TypeClass
	InputValueClass  TypeClass
	OutputKeyClass   TypeClass
	OutputValueClass TypeClass
}

func (tb *MapReduceBase) GetInputKeyClass() TypeClass {
	return tb.InputKeyClass
}

func (tb *MapReduceBase) GetOutputKeyClass() TypeClass {
	return tb.OutputKeyClass
}

func (tb *MapReduceBase) GetInputValueClass() TypeClass {
	return tb.InputValueClass
}

func (tb *MapReduceBase) GetOutputValueClass() TypeClass {
	return tb.OutputValueClass
}

//BeforeRun default empty
func (tb *MapReduceBase) BeforeRun() {
}

//ForEachValue iterate values using iterator
func ForEachValue(valuesNext func() (interface{}, error), handler func(interface{})) {
	for v, err := valuesNext(); err == nil; v, err = valuesNext() {
		handler(v)
	}
}
