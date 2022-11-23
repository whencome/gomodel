package gomodel

// Options 选项设置，用于扩展设置相关参数
type Options struct {
    // 是否支持sharding
    EnableSharding bool
    // 数据库分库数量
    DbShardingNum int64
    // 每个数据库分表数量
    TableShardingNum int64
}

// NewDefaultOptions 创建一个默认的Options
func NewDefaultOptions() *Options {
    return &Options{
        EnableSharding:   false,
        DbShardingNum:    1,
        TableShardingNum: 1,
    }
}

// NewShardingOptions 创建一个分库分表的Options
func NewShardingOptions(tableNum, dbNum int64) *Options {
    return &Options{
        EnableSharding:   true,
        DbShardingNum:    dbNum,
        TableShardingNum: tableNum,
    }
}
