package mapred

// InputTypeConverter Output type converter interface
type InputTypeConverter interface {
	GetInputKeyTypeConverter() TypeConverter
	GetInputValueTypeConverter() TypeConverter
}

// OutputTypeConverter Output type converter interface
type OutputTypeConverter interface {
	GetOutputKeyTypeConverter() TypeConverter
	GetOutputValueTypeConverter() TypeConverter
}

// InputOutputTypeConverter Both input and output type converter interface
type InputOutputTypeConverter interface {
	InputTypeConverter
	OutputTypeConverter
}

// mapperReducer Some common function of mapper and reducer
type mapperReducer interface {
	Init()
}

const (
	// ErrorNoMoreInput End of input
	ErrorNoMoreInput = "Error no more input"
	// ErrorNoMoreKey End of a key
	ErrorNoMoreKey = "Error no more key"
)

// ValueIterator Value iterator for reducer
type ValueIterator interface {
	Next() (interface{}, error)
}

// Mapper Mapper interface
type Mapper interface {
	InputOutputTypeConverter
	mapperReducer
	Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{})
	GetTypeConverters() *TypeConverters
}

// Reducer Reducer interface
type Reducer interface {
	InputOutputTypeConverter
	mapperReducer
	Reduce(key interface{}, valuesNext ValueIterator, output func(v interface{}), reporter interface{})
	GetTypeConverters() *TypeConverters
}

type Combiner interface {
	InputOutputTypeConverter
	mapperReducer
	Combine(key interface{}, v1 interface{}, v2 interface{}, output func(v interface{}))
	GetTypeConverters() *TypeConverters
}

// TypeConverters Define type converters for mapper and reducer, so user defined mapper/reducer will not need to handle []byte.
type TypeConverters struct {
	InputKeyTypeConverter    TypeConverter
	InputValueTypeConverter  TypeConverter
	OutputKeyTypeConverter   TypeConverter
	OutputValueTypeConverter TypeConverter
}

// GetInputKeyTypeConverter get input key type converter
func (tb *TypeConverters) GetInputKeyTypeConverter() TypeConverter {
	return tb.InputKeyTypeConverter
}

// GetOutputKeyTypeConverter get output key type converter
func (tb *TypeConverters) GetOutputKeyTypeConverter() TypeConverter {
	return tb.OutputKeyTypeConverter
}

// GetInputValueTypeConverter get input value type converter
func (tb *TypeConverters) GetInputValueTypeConverter() TypeConverter {
	return tb.InputValueTypeConverter
}

// GetOutputValueTypeConverter get output value type converter
func (tb *TypeConverters) GetOutputValueTypeConverter() TypeConverter {
	return tb.OutputValueTypeConverter
}

// MapperCommon Every implemention of Mapper interface should embeded this struct
type MapperCommon struct {
	TypeConverters
}

// Init default empty init function
func (tb *MapperCommon) Init() {
}

func (tb *MapperCommon) GetTypeConverters() *TypeConverters {
	return &tb.TypeConverters
}

// ReducerCommon Every implemention of Reducer interface should embeded this struct
type ReducerCommon struct {
	// TypeConverters Declare the type converters which will help to handle raw []byte
	TypeConverters
}

// Init default empty init function
func (tb *ReducerCommon) Init() {
}

func (tb *ReducerCommon) GetTypeConverters() *TypeConverters {
	return &tb.TypeConverters
}

type CombineCommon struct {
	TypeConverters
}

// Init default empty init function
func (tb *CombineCommon) Init() {
}

func (tb *CombineCommon) GetTypeConverters() *TypeConverters {
	return &tb.TypeConverters
}
