package gomodel

import (
    "fmt"
    "github.com/whencome/xlog"
    "math"
)

type ShardingModelManager struct {
    *ModelManager
    Sharding int64
}

// NewShardingModelManager 创建一个ShardingModelManager
func NewShardingModelManager(m Modeler, opts *Options) *ShardingModelManager {
    mm := NewModelManager(m)
    mm.SetOptions(opts)
    return &ShardingModelManager{
        ModelManager: mm,
        Sharding:     0,
    }
}

// UseSharding 设置使用的sharding值
func (m *ShardingModelManager) UseSharding(v int64) *ShardingModelManager {
    m.Sharding = v
    return m
}

// GetSharding 获取Sharding值
func (m *ShardingModelManager) GetSharding() (int64, int64, error) {
    if !m.Settings.EnableSharding || m.Settings.DbShardingNum <= 0 || m.Settings.TableShardingNum <= 0 {
        return 0, 0, fmt.Errorf("SHARDING_UNAVAILABLE")
    }
    if m.Sharding <= 0 {
        return 0, 0, fmt.Errorf("SHARDING_VALUE_INVALID")
    }
    tblSharding := m.Sharding % m.Settings.TableShardingNum
    dbSharding := int64(math.Floor(float64(tblSharding) / float64(m.Settings.DbShardingNum)))
    return tblSharding, dbSharding, nil
}

// GetTableName 获取Model对应的数据表名
func (m *ShardingModelManager) GetTableName() string {
    if m.Model == nil {
        return ""
    }
    tblName := m.Model.GetTableName()
    ti, _, _ := m.GetSharding()
    return fmt.Sprintf("%s_%d", tblName, ti)
}

// GetDatabase 获取数据库名称（返回配置中的名称，不要使用实际数据库名称，因为实际数据库名称在不同环境可能不一样）
func (m *ShardingModelManager) GetDatabase() string {
    if m.Model == nil {
        return ""
    }
    _, di, _ := m.GetSharding()
    return fmt.Sprintf("%s_%d", m.Model.GetDatabase(), di)
}

// NewQuerier 创建一个查询对象
func (m *ShardingModelManager) NewQuerier() *Querier {
    conn, err := m.GetConnection()
    if err != nil {
        xlog.Errorf("get db [%s] connection failed: %s", m.GetDatabase(), err)
        conn = nil
    }
    return NewModelQuerier(m.Model).Connect(conn).SetOptions(m.Settings).Select(m.QueryFieldsString())
}
