package mapred
import (
	"io"
)

type InputTypeKeyConverters struct {
	InputKeyTypeConverter    TypeConverter
	InputValueTypeConverter  TypeConverter
}

func (tb *InputTypeKeyConverters) GetInputKeyTypeConverter() TypeConverter {
	return tb.InputKeyTypeConverter
}

// GetInputValueTypeConverter get input value type converter
func (tb *InputTypeKeyConverters) GetInputValueTypeConverter() TypeConverter {
	return tb.InputValueTypeConverter
}

type Filter interface {
	InputTypeConverter
	Filter(k, v interface{}, writer io.Writer) error
	Init()
	After()
}

type FilterCommon struct {
	InputTypeKeyConverters
}

func (*FilterCommon) Init() {}
func (*FilterCommon) After() {}

type BashCommand interface {
	InputTypeConverter
	RunCommand()
	Init()
	After()
}

type BashCommandBase struct {
	InputTypeKeyConverters
}

func (*BashCommandBase) Init() {}
func (*BashCommandBase) After() {}
