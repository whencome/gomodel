package gomodel

import (
    "fmt"
    "reflect"
    "sync"
)

// modeler pool
var pool = NewModelerPool()

type ModelerInfo struct {
    AutoIncrementField string
    Fields             []string
    FieldMaps          map[string]string
    PropMaps           map[string]string
}

type ModelerPool struct {
    pools sync.Map
}

func NewModelerPool() *ModelerPool {
    return new(ModelerPool)
}

func (p *ModelerPool) Parse(m Modeler) *ModelerInfo {
    // check cache
    cacheKey := fmt.Sprintf("%s:%s", m.GetDatabase(), m.GetTableName())
    v, ok := p.pools.Load(cacheKey)
    if ok {
        return v.(*ModelerInfo)
    }
    // parse modeler info
    fieldMaps := map[string]string{}
    propMaps := make(map[string]string)
    fields := make([]string, 0)
    // 获取tag中的内容
    rt := reflect.TypeOf(m)
    // 获取字段数量
    fieldsNum := rt.Elem().NumField()
    for i := 0; i < fieldsNum; i++ {
        field := rt.Elem().Field(i)
        fieldName := field.Name
        tableFieldName := field.Tag.Get(m.GetDBFieldTag())
        if tableFieldName == "" {
            continue
        }
        fields = append(fields, tableFieldName)
        fieldMaps[tableFieldName] = fieldName
        propMaps[fieldName] = tableFieldName
    }
    mi := &ModelerInfo{
        Fields:    fields,
        FieldMaps: fieldMaps,
        PropMaps:  propMaps,
    }
    // cache modeler info
    p.pools.Store(cacheKey, mi)
    return mi
}
