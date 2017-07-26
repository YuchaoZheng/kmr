package mapred

// InputTypeConverter output type converter interface
type InputTypeConverter interface {
	GetInputKeyTypeConverter() TypeConverter
	GetInputValueTypeConverter() TypeConverter
}

// OutputTypeConverter output type converter interface
type OutputTypeConverter interface {
	GetOutputKeyTypeConverter() TypeConverter
	GetOutputValueTypeConverter() TypeConverter
}

// InputOutputTypeConverter input class base
type InputOutputTypeConverter interface {
	InputTypeConverter
	OutputTypeConverter
}

// MapperReducerBase some common func
type MapperReducerBase interface {
	Init()
}

// Mapper Mapper interface
type Mapper interface {
	InputOutputTypeConverter
	MapperReducerBase
	Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{})
}

// Reducer Reducer interface
type Reducer interface {
	InputOutputTypeConverter
	MapperReducerBase
	Reduce(key interface{}, valuesNext func() (interface{}, error), output func(k interface{}, v interface{}), reporter interface{})
}

// MapReduceBase Mapper and Reducer base class
type MapReduceBase struct {
	InputKeyTypeConverter    TypeConverter
	InputValueTypeConverter  TypeConverter
	OutputKeyTypeConverter   TypeConverter
	OutputValueTypeConverter TypeConverter
}

// GetInputKeyTypeConverter get input key type converter
func (tb *MapReduceBase) GetInputKeyTypeConverter() TypeConverter {
	return tb.InputKeyTypeConverter
}

// GetOutputKeyTypeConverter get output key type converter
func (tb *MapReduceBase) GetOutputKeyTypeConverter() TypeConverter {
	return tb.OutputKeyTypeConverter
}

// GetInputValueTypeConverter get input value type converter
func (tb *MapReduceBase) GetInputValueTypeConverter() TypeConverter {
	return tb.InputValueTypeConverter
}

// GetOutputValueTypeConverter get output value type converter
func (tb *MapReduceBase) GetOutputValueTypeConverter() TypeConverter {
	return tb.OutputValueTypeConverter
}

// Init default empty init function
func (tb *MapReduceBase) Init() {
}

// ForEachValue iterate values using iterator
func ForEachValue(valuesNext func() (interface{}, error), handler func(interface{})) {
	for v, err := valuesNext(); err == nil; v, err = valuesNext() {
		handler(v)
	}
}
