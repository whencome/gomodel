package gomodel

import (
    "bytes"
    "database/sql"
    "fmt"
)

type SqlCommand struct {
    command bytes.Buffer
    values  []interface{}
}

func NewSqlCommand() *SqlCommand {
    return &SqlCommand{
        command: bytes.Buffer{},
        values:  make([]interface{}, 0),
    }
}

func (sc *SqlCommand) IsEmpty() bool {
    if sc.command.Len() > 0 {
        return false
    }
    return true
}

func (sc *SqlCommand) Add(cmd *SqlCommand) {
    if cmd == nil {
        return
    }
    sc.Write(cmd.command.Bytes())
    sc.values = append(sc.values, cmd.values...)
}

func (sc *SqlCommand) Write(b []byte) {
    sc.command.Write(b)
}

func (sc *SqlCommand) WriteString(s string) {
    sc.command.WriteString(s)
}

func (sc *SqlCommand) Writef(f string, v ...interface{}) {
    sc.command.WriteString(fmt.Sprintf(f, v...))
}

func (sc *SqlCommand) AddValue(v interface{}) {
    sc.values = append(sc.values, v)
}

func (sc *SqlCommand) AddValues(v ...interface{}) {
    sc.values = append(sc.values, v...)
}

func (sc *SqlCommand) Command() string {
    return sc.command.String()
}

func (sc *SqlCommand) Values() []interface{} {
    return sc.values
}

// Commander 执行者，用于执行数据库查询等操作
type Commander struct {
    inTrans  bool     // 是否在执行事务中
    Command  string   // 需要执行的SQL
    Settings *Options // 相关配置
    conn     *sql.DB  // 数据库连接
    tx       *sql.Tx  // 事务
}

// NewCommander 创建一个新的执行者对象
func NewCommander(opts *Options) *Commander {
    if opts == nil {
        opts = NewDefaultOptions()
    }
    return &Commander{
        inTrans:  false,
        Settings: opts,
        conn:     nil,
    }
}

// SetOptions 设置选项参数
func (c *Commander) SetOptions(opts *Options) *Commander {
    c.Settings = opts
    return c
}

// Connect 设置数据库连接
func (c *Commander) Connect(conn *sql.DB) *Commander {
    if conn != nil {
        c.conn = conn
    }
    return c
}

// BeginTransaction 开启事务
func (c *Commander) BeginTransaction() error {
    if c.inTrans {
        return nil
    }
    tx, err := c.conn.Begin()
    if err != nil {
        return err
    }
    c.inTrans = true
    c.tx = tx
    return nil
}

// Commit 提交事务
func (c *Commander) Commit() error {
    if !c.inTrans {
        return nil
    }
    return c.tx.Commit()
}

// Rollback 回滚事务
func (c *Commander) Rollback() error {
    if !c.inTrans {
        return nil
    }
    return c.tx.Rollback()
}

// Execute 执行SQL命令
func (c *Commander) Execute(command string, args ...interface{}) (sql.Result, error) {
    // 增加日志记录
    l := NewLogger()
    l.SetCommand(command)
    defer l.Close()
    // 执行命令
    var rs sql.Result
    var err error
    if c.inTrans {
        rs, err = c.tx.Exec(command, args...)
    } else {
        rs, err = c.conn.Exec(command, args...)
    }
    if err != nil {
        l.Fail(err.Error())
    } else {
        l.Success()
    }
    return rs, err
}

// ExecuteTx 执行事务
func (c *Commander) ExecuteTx(f func(commander *Commander) error) error {
    e := c.BeginTransaction()
    if e != nil {
        return e
    }
    e = f(c)
    if e != nil {
        _ = c.Rollback()
        return e
    }
    if c.Commit() != nil {
        _ = c.Rollback()
        return ErrTxCommitFailed
    }
    return nil
}

// RawQuery 执行原始的查询
func (c *Commander) RawQuery(command string, args ...interface{}) (*sql.Rows, error) {
    var rows *sql.Rows
    var err error
    // 增加日志记录
    l := NewLogger()
    l.SetCommand(command)
    defer l.Close()
    // 执行命令
    if c.inTrans {
        rows, err = c.tx.Query(command, args...)
    } else {
        rows, err = c.conn.Query(command, args...)
    }
    // 记录执行结果
    if err != nil {
        l.Fail(err.Error())
    } else {
        l.Success()
    }
    return rows, err
}

// Query 查询满足条件的全部数据
func (c *Commander) Query(command string, args ...interface{}) (*QueryResult, error) {
    result := NewQueryResult()
    // 增加日志记录
    l := NewLogger()
    l.SetCommand(command)
    // 执行命令
    rows, err := c.RawQuery(command, args...)
    if err != nil {
        l.Fail(err.Error())
        l.Close()
        return nil, err
    }
    l.Success()
    l.Close()
    // 读取数据
    result.Columns, err = rows.Columns()
    if err != nil {
        return nil, err
    }
    // 创建临时切片用于保存数据
    row := make([]interface{}, len(result.Columns))
    // 创建存储数据的字节切片2维数组data
    tmpData := make([][]byte, len(result.Columns))
    for i, _ := range row {
        row[i] = &tmpData[i]
    }
    // 开始读取数据
    count := 0
    for rows.Next() {
        err = rows.Scan(row...)
        if err != nil {
            return nil, err
        }
        data := make(map[string]string)
        for i, v := range row {
            k := result.Columns[i]
            if v == nil {
                data[k] = ""
            } else {
                data[k] = string(*(v.(*[]uint8)))
            }
        }
        result.Rows = append(result.Rows, data)
        count++
    }
    result.TotalCount = count
    result.RowsCount = count
    // 返回查询结果
    return result, nil
}

// QueryRow 查询单行数据
func (c *Commander) QueryRow(command string, args ...interface{}) (map[string]string, error) {
    rows, err := c.RawQuery(command, args...)
    if err != nil {
        return nil, err
    }
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }
    // 创建临时切片用于保存数据
    row := make([]interface{}, len(columns))
    // 创建存储数据的字节切片2维数组data
    tmpData := make([][]byte, len(columns))
    for i, _ := range row {
        row[i] = &tmpData[i]
    }
    // 开始读取数据
    data := make(map[string]string)
    if !rows.Next() {
        return nil, nil
    }
    err = rows.Scan(row...)
    if err != nil {
        return nil, err
    }
    for i, v := range row {
        k := columns[i]
        if v == nil {
            data[k] = ""
        } else {
            data[k] = string(*(v.(*[]uint8)))
        }
    }
    // 返回查询结果
    return data, nil
}

// QueryScalar 查询单个值
func (c *Commander) QueryScalar(command string, args ...interface{}) (string, error) {
    rows, err := c.RawQuery(command, args...)
    if err != nil {
        return "", err
    }
    columns, err := rows.Columns()
    if err != nil {
        return "", err
    }
    // 创建临时切片用于保存数据
    row := make([]interface{}, len(columns))
    // 创建存储数据的字节切片2维数组data
    tmpData := make([][]byte, len(columns))
    for i, _ := range row {
        row[i] = &tmpData[i]
    }
    // 开始读取数据
    data := make(map[string]string)
    if !rows.Next() {
        return "", nil
    }
    err = rows.Scan(row...)
    if err != nil {
        return "", err
    }
    for i, v := range row {
        k := columns[i]
        if v == nil {
            data[k] = ""
        } else {
            data[k] = string(*(v.(*[]uint8)))
        }
    }
    // 查询第一个字段
    firstColumn := columns[0]
    // 返回查询结果
    return data[firstColumn], nil
}

func (c *Commander) Model(m Modeler) *ModelManager {
    mm := NewModelManager(m)
    mm.GetDBFunc = func() (*sql.DB, error) {
        return c.conn, nil
    }
    return mm
}

func (c *Commander) Insert(m Modeler) (int64, error) {
    return c.Model(m).Insert(m)
}

func (c *Commander) Update(m Modeler) (int64, error) {
    return c.Model(m).Update(m)
}

func (c *Commander) Delete(m Modeler, cond interface{}) (int64, error) {
    return c.Model(m).Delete(cond)
}
