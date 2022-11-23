package gomodel

import (
    "database/sql"
    "errors"
)

//------------ ERRORS DEFINITION ------------//

var (
    // ErrDBConnectionNotSet 数据库连接未设置
    ErrDBConnectionNotSet = errors.New("db connection not set")
    // ErrTxCommitFailed 事务提交失败
    ErrTxCommitFailed = errors.New("transaction commit failed")
)

//------------ CONSTANTS ------------//

const (
    LogicAnd = "AND"
    LogicOr  = "OR"
)

//------------ DEFINITION OF RESOURCE MANAGER ------------//

// GetConnFunc 获取数据库连接的方法
type GetConnFunc func(name string) (*sql.DB, error)

// ResourceManager 定义全局资源管理器
type ResourceManager struct {
    Conns map[string]GetConnFunc
}

func NewResourceManager() *ResourceManager {
    return &ResourceManager{
        Conns: make(map[string]GetConnFunc, 0),
    }
}

// GetConnection 获取数据库连接
func (rm *ResourceManager) GetConnection(dbName string) (*sql.DB, error) {
    f, ok := rm.Conns[dbName]
    if !ok {
        return nil, ErrDBConnectionNotSet
    }
    return f(dbName)
}

//------------ GLOBAL RESOURCE MANAGER ------------//
var globalResManager = NewResourceManager()

// RegisterDB 注册数据库连接对象
func RegisterDB(name string, conn *sql.DB) {
    globalResManager.Conns[name] = func(name string) (db *sql.DB, e error) {
        return conn, nil
    }
}

// RegisterDBInitFunc 注册数据库连接对象
func RegisterDBInitFunc(name string, f GetConnFunc) {
    globalResManager.Conns[name] = f
}
