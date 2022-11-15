package gomodel

import (
    "container/list"
    "database/sql"
    "fmt"
    "sync"
    "time"
)

// DatabaseConfig 定义数据库配置
type DatabaseConfig struct {
    Name        string `yaml:"name" json:"name"`                 // 数据库名称
    Driver      string `yaml:"driver" json:"driver"`             // 驱动类型，sqlite3、mysql、clickhouse
    DSN         string `yaml:"dsn" json:"dsn"`                   // 数据库连接配置
    MaxLifeTime int    `yaml:"max_lifetime" json:"max_lifetime"` // 最大生命周期，单位：秒
    MaxConns    int    `yaml:"max_conns" json:"max_conns"`       // 最大连接数
    MaxIdles    int    `yaml:"max_idles" json:"max_idles"`       // 最大空闲连接数
}

// /////////////////// Connection Statistics ///////////////////////

type ConnStat struct {
    FirstFailTime int64
    LastFailTime  int64
    LastErr       error
    DetectCount   int64
    FailCount     int64
}

func (s *ConnStat) LogFail(err error) {
    now := time.Now()
    // 如果较上次失败持续时间长于指定时间，则忽略上次失败时间
    // 超过5分钟则忽略上次失败
    if now.Unix()-s.LastFailTime > 5*60 {
        s.Reset()
    }
    // record fail info
    if s.DetectCount == 0 {
        s.FirstFailTime = now.Unix()
    }
    s.DetectCount++
    s.FailCount++
    s.LastFailTime = now.Unix()
    s.LastErr = err
}

func (s *ConnStat) Reset() {
    s.DetectCount = 0
    s.FailCount = 0
    s.FirstFailTime = 0
    s.LastFailTime = 0
}

func (s *ConnStat) FailDuration() int64 {
    return s.LastFailTime - s.FirstFailTime
}

func (s *ConnStat) FailTimes() int64 {
    return s.FailCount
}

func (s *ConnStat) FailRate() float64 {
    if s.DetectCount == 0 {
        return 0
    }
    return float64(s.FailCount) / float64(s.DetectCount) * 100
}

// /////////////////// Connection Manager ///////////////////////

// 创建一个链接管理器
var connMgr *ConnectionManager

// ConnectionManager 连接管理器
type ConnectionManager struct {
    // 数据库配置列表 map[string]*DatabaseConfig
    DBConfigs sync.Map
    // 数据库连接列表 map[string]*sql.DB
    DBConns    sync.Map
    dirtyConns *list.List
    // 数据库连接统计 map[string]*ConnStat
    stats sync.Map
}

// NewConnectionManager 创建一个新的连接管理器
func NewConnectionManager() *ConnectionManager {
    return &ConnectionManager{
        dirtyConns: list.New(),
    }
}

// initConnection 初始化数据库连接
func (m *ConnectionManager) initConfig(cfg *DatabaseConfig) {
    if cfg == nil {
        return
    }
    _, ok := m.DBConfigs.Load(cfg.Name)
    if ok {
        _conn, ok := m.DBConns.Load(cfg.Name)
        if ok {
            conn := _conn.(*sql.DB)
            m.dirtyConns.PushBack(conn)
            m.DBConns.Delete(cfg.Name)
        }
    }
    m.DBConfigs.Store(cfg.Name, cfg)
    RegisterDBInitFunc(cfg.Name, GetConnection)
}

// getConnection 获取指定数据库的连接
func (m *ConnectionManager) getConnection(dbName string) (*sql.DB, error) {
    _conn, ok := m.DBConns.Load(dbName)
    if ok {
        conn := _conn.(*sql.DB)
        return conn, nil
    }
    return m.initConnection(dbName)
}

// initConnection 初始化数据库连接
func (m *ConnectionManager) initConnection(dbName string) (*sql.DB, error) {
    // get database config
    _dbCfg, ok := m.DBConfigs.Load(dbName)
    if !ok {
        return nil, fmt.Errorf("no avail config for db [%s]", dbName)
    }
    dbCfg := _dbCfg.(*DatabaseConfig)
    // open connections
    conn, err := sql.Open(dbCfg.Driver, dbCfg.DSN)
    if err != nil {
        return nil, err
    }
    // 初始化连接
    conn.SetConnMaxLifetime(time.Second * time.Duration(dbCfg.MaxLifeTime))
    conn.SetMaxOpenConns(dbCfg.MaxConns)
    conn.SetMaxIdleConns(dbCfg.MaxIdles)
    m.DBConns.Store(dbName, conn)
    return conn, nil
}

// Close close all established connections
func (m *ConnectionManager) Close() {
    m.DBConns.Range(func(key, value any) bool {
        conn := value.(*sql.DB)
        m.dirtyConns.PushBack(conn)
        m.DBConns.Delete(key)
        return true
    })
}

// CloseDB remove & close the given db connection
func (m *ConnectionManager) CloseDB(dbName string) {
    _conn, ok := m.DBConns.LoadAndDelete(dbName)
    if !ok {
        return
    }
    conn := _conn.(*sql.DB)
    m.dirtyConns.PushBack(conn)
}

func (m *ConnectionManager) watchConns() {
    ticker := time.NewTicker(time.Second * 15)
    for {
        <-ticker.C
        // remove dirty connections
        if m.dirtyConns.Len() > 0 {
            go func() {
                for c := m.dirtyConns.Front(); c != nil; c.Next() {
                    conn := c.Value.(*sql.DB)
                    _ = conn.Close()
                }
            }()
        }
        // check active connections
        m.DBConns.Range(func(key, value any) bool {
            conn := value.(*sql.DB)
            err := conn.Ping()
            if err == nil {
                return true
            }
            var stat *ConnStat
            _stat, ok := m.stats.Load(key)
            if ok {
                stat = _stat.(*ConnStat)
                stat.LogFail(err)
            } else {
                stat = new(ConnStat)
                stat.LogFail(err)
            }
            if stat.FailCount >= 8 && stat.FailRate() > 90 {
                m.CloseDB(String(key))
            }
            m.stats.Store(key, stat)
            return true
        })
    }
}

// InitDB 初始化单个数据库配置
func InitDB(cfg *DatabaseConfig) {
    connMgr.initConfig(cfg)
}

// InitDBs 初始化数据库配置
func InitDBs(cfgs []*DatabaseConfig) {
    if cfgs == nil || len(cfgs) == 0 {
        return
    }
    for _, cfg := range cfgs {
        connMgr.initConfig(cfg)
    }
}

// InitMDBs 初始化数据库配置
func InitMDBs(cfgs map[string]*DatabaseConfig) {
    if cfgs == nil || len(cfgs) == 0 {
        return
    }
    for _, cfg := range cfgs {
        connMgr.initConfig(cfg)
    }
}

// GetConnection 获取数据库连接
func GetConnection(dbName string) (*sql.DB, error) {
    return connMgr.getConnection(dbName)
}

// Close 关闭数据库连接
func Close() {
    connMgr.Close()
}
