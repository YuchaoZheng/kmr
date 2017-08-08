package jobgraph

import "github.com/naturali/kmr/mapred"

type combinedMapper struct {
	mapred.MapperCommon
	FirstMapper  mapred.Mapper
	SecondMapper mapred.Mapper
}

func (cm *combinedMapper) Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	if cm.FirstMapper != nil {
		cm.FirstMapper.Map(key, value, func(k, v interface{}) {
			if cm.SecondMapper != nil {
				cm.SecondMapper.Map(k, v, output, reporter)
			} else {
				output(k, v)
			}
		}, reporter)
	} else {
		cm.SecondMapper.Map(key, value, output, reporter)
	}
}

//CombineMappers combine multiple mappers into one
func combineMappers(mappers ...mapred.Mapper) mapred.Mapper {
	i := len(mappers)
	if i == 0 {
		panic("Number of mappers being combind should not be zero")
	} else if i == 1 {
		return mappers[0]
	} else {
		return &combinedMapper{
			FirstMapper:  mappers[0],
			SecondMapper: combineMappers(mappers[1:]...),
		}
	}
}

// IdentityMapper A mapper which output key/value directly
var IdentityMapper = &identityMapper{
	mapred.MapperCommon{
		mapred.TypeConverters{
			InputKeyTypeConverter:    mapred.Bytes{},
			InputValueTypeConverter:  mapred.Bytes{},
			OutputKeyTypeConverter:   mapred.Bytes{},
			OutputValueTypeConverter: mapred.Bytes{},
		},
	},
}

type identityMapper struct {
	mapred.MapperCommon
}

func (*identityMapper) Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	output(key, value)
}