package gomodel

import (
    "database/sql"
    "fmt"
    "github.com/whencome/xlog"
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

// 创建一个链接管理器
var connMgr = NewConnectionManager()

// ConnectionManager 连接管理器
type ConnectionManager struct {
    // 数据库配置列表
    DBConfigs map[string]*DatabaseConfig
    // 数据库连接列表
    DBConns map[string]*sql.DB
    // 锁
    Locker sync.RWMutex
}

// NewConnectionManager 创建一个新的连接管理器
func NewConnectionManager() *ConnectionManager {
    return &ConnectionManager{
        DBConfigs: nil,
        DBConns:   make(map[string]*sql.DB),
        Locker:    sync.RWMutex{},
    }
}

// initConnection 初始化数据库连接
func (m *ConnectionManager) initConfig(cfg *DatabaseConfig) {
    if cfg == nil {
        return
    }
    m.Locker.Lock()
    defer m.Locker.Unlock()
    if _, ok := m.DBConfigs[cfg.Name]; ok {
        if conn, ok := m.DBConns[cfg.Name]; ok {
            _ = conn.Close()
            delete(m.DBConns, cfg.Name)
        }
    }
    m.DBConfigs[cfg.Name] = cfg
    RegisterDBInitFunc(cfg.Name, GetConnection)
}

// getConnection 获取指定数据库的连接
func (m *ConnectionManager) getConnection(dbName string) (*sql.DB, error) {
    // 检查是否存在既有连接
    m.Locker.RLock()
    conn, ok := m.DBConns[dbName]
    m.Locker.RUnlock()
    if ok {
        return conn, nil
    }
    // 初始化数据库连接
    return m.initConnection(dbName)
}

// initConnection 初始化数据库连接
func (m *ConnectionManager) initConnection(dbName string) (*sql.DB, error) {
    // 初始化数据库连接
    dbCfg, ok := m.DBConfigs[dbName]
    if !ok {
        return nil, fmt.Errorf("no avail config for db [%s]", dbName)
    }
    // 连接数据库
    conn, err := sql.Open(dbCfg.Driver, dbCfg.DSN)
    if err != nil {
        return nil, err
    }
    // 初始化连接
    conn.SetConnMaxLifetime(time.Second * time.Duration(dbCfg.MaxLifeTime))
    conn.SetMaxOpenConns(dbCfg.MaxConns)
    conn.SetMaxIdleConns(dbCfg.MaxIdles)
    // 加锁
    m.Locker.Lock()
    m.DBConns[dbName] = conn
    m.Locker.Unlock()
    // 返回连接信息
    return conn, nil
}

// Close close all established connections
func (m *ConnectionManager) Close() {
    if len(m.DBConns) <= 0 {
        return
    }
    for name, db := range m.DBConns {
        err := db.Close()
        if err != nil {
            xlog.Errorf("close db [%s] failed: %s", name, err)
        }
        // 移除连接
        delete(m.DBConns, name)
    }
    // 将连接列表置为空
    m.DBConns = make(map[string]*sql.DB)
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

// InitDBs 初始化数据库配置
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
