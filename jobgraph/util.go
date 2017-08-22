package jobgraph

import "github.com/naturali/kmr/mapred"

type combinedMapper struct {
	mapred.MapperCommon
	FirstMapper  mapred.Mapper
	SecondMapper mapred.Mapper
}

func (cm *combinedMapper) Map(key interface{}, value interface{}, output func(k interface{}, v interface{}), reporter interface{}) {
	cm.FirstMapper.Map(key, value, func(k, v interface{}) {
		if cm.SecondMapper != nil {
			cm.SecondMapper.Map(k, v, output, reporter)
		} else {
			output(k, v)
		}
	}, reporter)
}

// CombineMappers combine multiple mappers into one
func combineMappers(mappers ...mapred.Mapper) mapred.Mapper {
	numMappers := len(mappers)
	switch numMappers {
	case 0:
		panic("Number of mappers being combined should not be zero")
	case 1:
		return mappers[0]
	default:
		if mappers[0] == nil {
			return combineMappers(mappers[1:]...)
		} else {
			combined := combineMappers(mappers[1:]...)
			if combined == nil {
				return mappers[0]
			} else {
				return &combinedMapper{
					MapperCommon: mapred.MapperCommon{
						TypeConverters: *mappers[0].GetTypeConverters(),
					},
					FirstMapper:  mappers[0],
					SecondMapper: combined,
				}
			}
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
