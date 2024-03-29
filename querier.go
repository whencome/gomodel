package gomodel

import (
    "bytes"
    "database/sql"
    "errors"
    "fmt"
    "strings"
)

// 定义联表方式
const (
    innerJoin = "INNER"
    leftJoin  = "LEFT"
    rightJoin = "RIGHT"
)

/************************************************************
 ******              SECTION OF JOIN TABLES             *****
 ************************************************************/
// joinTable 定义联表类型
type joinTable struct {
    table     string // 表名
    condition string // 连接条件,只支持字符串
    joinType  string // 连接方式，inner，left，right
}

// newInnerJoinTable 创建一个内联表
func newInnerJoinTable(tblName string, onCond string) *joinTable {
    return &joinTable{
        table:     tblName,
        condition: onCond,
        joinType:  innerJoin,
    }
}

// newRightJoinTable 创建一个右联表
func newRightJoinTable(tblName string, onCond string) *joinTable {
    return &joinTable{
        table:     tblName,
        condition: onCond,
        joinType:  rightJoin,
    }
}

// newLeftJoinTable 创建一个左联表
func newLeftJoinTable(tblName string, onCond string) *joinTable {
    return &joinTable{
        table:     tblName,
        condition: onCond,
        joinType:  leftJoin,
    }
}

/************************************************************
 ******             SECTION OF QUERY RESULT             *****
 ************************************************************/
// QueryResult 保存一个查询结果（不支持分页）
type QueryResult struct {
    TotalCount int                 // 记录总数
    Offset     int                 // 偏移量，用于分页处理
    RowsCount  int                 // 当前查询的记录数量
    Columns    []string            // 用于单独保存字段，以解决显示结果字段顺序不正确的问题
    Rows       []map[string]string // 查询结果，一切皆字符串
}

// NewQueryResult 创建一个新的查询结果
func NewQueryResult() *QueryResult {
    return &QueryResult{
        TotalCount: 0,
        Offset:     0,
        RowsCount:  0,
        Columns:    make([]string, 0),
        Rows:       make([]map[string]string, 0),
    }
}

/************************************************************
 ******                SECTION OF QUERIER               *****
 ************************************************************/
// Querier 查询对象
type Querier struct {
    queryMaps  map[string]interface{}
    joinTables []*joinTable // 联表信息
    QuerySQL   string       // 查询SQL
    Settings   *Options     // 是否开启查询前的SQL语法检测
    conn       *sql.DB      // 数据库连接
}

// NewQuerier 创建一个空的Querier
func NewQuerier() *Querier {
    return &Querier{
        queryMaps: map[string]interface{}{
            "fields":      "",
            "table":       "",
            "join_tables": make([]*joinTable, 0),
            "where":       nil,
            "having":      nil,
            "order_by":    "",
            "group_by":    "",
            "offset":      0,
            "limit":       -1,
        },
        joinTables: make([]*joinTable, 0),
        QuerySQL:   "",
        conn:       nil,
        Settings:   NewDefaultOptions(), // 设置一个默认参数配置
    }
}

// NewRawQuerier 根据查询SQL创建一个Querier
func NewRawQuerier(querySQL string) *Querier {
    q := NewQuerier()
    q.QuerySQL = querySQL
    return q
}

// NewModelQuerier 创建一个指定Model的查询对象
func NewModelQuerier(m Modeler) *Querier {
    q := NewQuerier()
    q.queryMaps["table"] = m.GetTableName()
    q.queryMaps["fields"] = "*"
    return q
}

// SetOptions 设置选项配置
func (q *Querier) SetOptions(opts *Options) *Querier {
    if opts == nil {
        return q
    }
    q.Settings = opts
    return q
}

// doPreQueryCheck 执行查询前的检查
func (q *Querier) doPreQueryCheck() error {
    if q.conn == nil {
        return errors.New("database connection not specified or unavailable")
    }
    return nil
}

// Connect 设置数据库连接
func (q *Querier) Connect(conn *sql.DB) *Querier {
    if conn != nil {
        q.conn = conn
    }
    return q
}

// Select 设置查询字段,fields为以“,”连接的字段列表
func (q *Querier) Select(fields string) *Querier {
    q.queryMaps["fields"] = fields
    return q
}

// From 选择查询的表
func (q *Querier) From(tblName string) *Querier {
    q.queryMaps["table"] = tblName
    return q
}

// Join 设置内联表
func (q *Querier) Join(tblName string, onCond string) *Querier {
    q.joinTables = append(q.joinTables, newInnerJoinTable(tblName, onCond))
    return q
}

// LeftJoin 设置左联表
func (q *Querier) LeftJoin(tblName string, onCond string) *Querier {
    q.joinTables = append(q.joinTables, newLeftJoinTable(tblName, onCond))
    return q
}

// RightJoin 设置右联表
func (q *Querier) RightJoin(tblName string, onCond string) *Querier {
    q.joinTables = append(q.joinTables, newRightJoinTable(tblName, onCond))
    return q
}

// CustomJoin 自定义join方式，用于支持其他数据库
// jnType 为连接方式，如：inner、left、right，支持兼容其他模式（如果非标准SQL格式，请提前关闭sql检查功能）
func (q *Querier) CustomJoin(jnType, tblName string, onCond string) *Querier {
    joinTbl := &joinTable{
        table:     tblName,
        condition: onCond,
        joinType:  jnType,
    }
    q.joinTables = append(q.joinTables, joinTbl)
    return q
}

// Where 设置查询条件
func (q *Querier) Where(cond interface{}) *Querier {
    q.queryMaps["where"] = cond
    return q
}

// OrderBy 设置排序方式
func (q *Querier) OrderBy(orderBy string) *Querier {
    q.queryMaps["order_by"] = orderBy
    return q
}

// GroupBy 设置分组方式
func (q *Querier) GroupBy(groupBy string) *Querier {
    q.queryMaps["group_by"] = groupBy
    return q
}

// Having 设置分组过滤条件
func (q *Querier) Having(cond interface{}) *Querier {
    q.queryMaps["having"] = cond
    return q
}

// Offset 设置查询偏移量
func (q *Querier) Offset(num int) *Querier {
    q.queryMaps["offset"] = num
    return q
}

// Limit 设置查询数量
func (q *Querier) Limit(num int) *Querier {
    q.queryMaps["limit"] = num
    return q
}

// buildCondition 构造查询条件
func (q *Querier) buildCondition() (string, error) {
    where, ok := q.queryMaps["where"]
    if !ok || where == nil {
        return "", nil
    }
    // 根据类型采取不同的构建方式
    condWhere, ok := where.(*Condition)
    if ok {
        return condWhere.Build()
    }
    return NewConditionBuilder().Build(q.queryMaps["where"], "AND")
}

// buildNoLimitQuery 构造没有limit的查询语句
func (q *Querier) buildNoLimitQuery() (string, error) {
    querySQL := bytes.Buffer{}
    querySQL.WriteString("SELECT ")

    // 查询字段
    fields := NewValue(q.queryMaps["fields"]).String()
    if fields == "" {
        fields = "*"
    }
    querySQL.WriteString(fields)

    // 表
    tableName := NewValue(q.queryMaps["table"]).String()
    if tableName == "" {
        return "", errors.New("query table not specified")
    }
    querySQL.WriteString(" FROM ")
    querySQL.WriteString(quote(tableName))

    // 检查联表信息
    if len(q.joinTables) > 0 {
        for _, joinTbl := range q.joinTables {
            if strings.TrimSpace(joinTbl.table) == "" {
                return "", errors.New("empty join table name")
            }
            if strings.TrimSpace(joinTbl.condition) == "" {
                return "", errors.New("join condition empty")
            }
            querySQL.WriteString(" ")
            querySQL.WriteString(joinTbl.joinType)
            querySQL.WriteString(" JOIN ")
            querySQL.WriteString(quote(joinTbl.table))
            querySQL.WriteString(" ON ")
            querySQL.WriteString(joinTbl.condition)
        }
    }

    // 查询条件
    condition, err := q.buildCondition()
    if err != nil {
        return "", err
    }
    if condition != "" {
        querySQL.WriteString(" WHERE ")
        querySQL.WriteString(condition)
    }

    // 检查是否对查询进行分组
    groupBy := NewValue(q.queryMaps["group_by"]).String()
    if groupBy != "" {
        querySQL.WriteString(" GROUP BY ")
        querySQL.WriteString(groupBy)
        // 检查是否有分组过滤
        having, err := NewConditionBuilder().Build(q.queryMaps["having"], "AND")
        if err != nil {
            return "", err
        }
        if having != "" {
            querySQL.WriteString(" HAVING ")
            querySQL.WriteString(having)
        }
    }

    // 设置排序
    orderBy := NewValue(q.queryMaps["order_by"]).String()
    if orderBy != "" {
        querySQL.WriteString(" ORDER BY ")
        querySQL.WriteString(orderBy)
    }
    return querySQL.String(), nil
}

// buildQuery 构造查询语句
func (q *Querier) buildQuery() error {
    if q.QuerySQL != "" {
        return nil
    }
    querySQL := bytes.Buffer{}

    // 构造没有limit的查询
    noLimitQuery, err := q.buildNoLimitQuery()
    if err != nil {
        return err
    }
    querySQL.WriteString(noLimitQuery)

    // 设置limit信息
    offset := NewValue(q.queryMaps["offset"]).Int64()
    limitNum := NewValue(q.queryMaps["limit"]).Int64()
    if limitNum > 0 {
        querySQL.WriteString(fmt.Sprintf(" LIMIT %d, %d", offset, limitNum))
    }

    // 返回查询SQL
    q.QuerySQL = querySQL.String()
    return nil
}

// buildCountQuery 构造count查询语句，用于统计查询数据的数量
func (q *Querier) buildCountQuery() (string, error) {
    // 根据原始查询语句构造Count语句
    if q.QuerySQL != "" && q.queryMaps["where"] == nil {
        return q.buildCountQueryFromRawQuery()
    }
    // 根据条件构造Count语句
    return q.buildCountQueryFromConditions()
}

// buildCountQueryFromConditions 根据条件构造count语句
func (q *Querier) buildCountQueryFromConditions() (string, error) {
    noLimitQuery, err := q.buildNoLimitQuery()
    if err != nil {
        return "", err
    }
    querySQL := bytes.Buffer{}
    querySQL.WriteString("SELECT COUNT(0) FROM ( ")
    querySQL.WriteString(noLimitQuery)
    querySQL.WriteString(" ) a")
    // 返回查询SQL
    return querySQL.String(), nil
}

// buildCountQueryFromRawQuery 根据原始查询构造count语句
func (q *Querier) buildCountQueryFromRawQuery() (string, error) {
    if q.QuerySQL == "" {
        return "", errors.New("query sql can not be empty")
    }
    // 先简单处理(逻辑上有问题，后续再解决)
    lowerQuerySQL := strings.ToLower(q.QuerySQL)
    limitPos := strings.LastIndex(lowerQuerySQL, " limit ")
    noLimitQuery := q.QuerySQL
    if limitPos > 0 {
        noLimitQuery = q.QuerySQL[0:limitPos]
    }

    // 构造count语句
    querySQL := bytes.Buffer{}
    querySQL.WriteString("SELECT COUNT(0) FROM ( ")
    querySQL.WriteString(noLimitQuery)
    querySQL.WriteString(" ) a")

    // 返回查询SQL
    return querySQL.String(), nil
}

// isBinaryValue 判断给定的值是否是二级制数据,这里只做简单判断
func (q *Querier) isBinaryValue(v *[]uint8) bool {
    bits := []byte(*v)
    isBinary := true
    for _, ascii := range bits {
        if ascii >= 32 {
            isBinary = false
            break
        }
    }
    return isBinary
}

// parseValue parse db value to readable value, this need some tricks to parse binary values
func (q *Querier) parseValue(v *[]uint8) string {
    if !q.isBinaryValue(v) {
        return string(*v)
    }
    // 二进制数据，返回二进制字符串
    buf := bytes.Buffer{}
    bits := []byte(*v)
    for _, bit := range bits {
        buf.WriteString(fmt.Sprintf("%b", bit))
    }
    return buf.String()
}

// Query 执行查询,此处返回为切片，以保证返回值结果顺序与查询字段顺序一致
func (q *Querier) Query() (*QueryResult, error) {
    // 构建查询
    err := q.buildQuery()
    if err != nil {
        return nil, err
    }
    // 执行查询前的检查
    err = q.doPreQueryCheck()
    if err != nil {
        return nil, err
    }
    // 执行查询
    result := NewQueryResult()

    // 获取日志对象
    l := NewLogger()
    l.SetCommand(q.QuerySQL)
    defer l.Close()

    // 执行查询
    rows, err := q.conn.Query(q.QuerySQL)
    if err != nil {
        l.Fail(err.Error())
        return nil, err
    }
    l.Success()

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
        // 将字节切片地址赋值给临时切片,这样row才是真正存放数据
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
            data[k] = q.parseValue(v.(*[]uint8))
        }
        result.Rows = append(result.Rows, data)
        count++
    }
    result.TotalCount = count
    result.RowsCount = count
    // 返回查询结果
    return result, nil
}

// 查询记录总数
func (q *Querier) queryTotalCount() (int, error) {
    // 构造统计查询
    countQuery, err := q.buildCountQuery()
    if err != nil {
        return 0, err
    }
    // 执行查询前的检查
    err = q.doPreQueryCheck()
    if err != nil {
        return 0, err
    }

    // 获取日志对象
    l := NewLogger()
    l.SetCommand(countQuery)
    defer l.Close()

    // 查询
    countRow := q.conn.QueryRow(countQuery)
    var totalCount int
    err = countRow.Scan(&totalCount)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    return totalCount, nil
}

// Count 查询记录总数
func (q *Querier) Count() (int, error) {
    return q.queryTotalCount()
}

// QueryPage 查询分页信息
func (q *Querier) QueryPage(page, pageSize int) (*QueryResult, error) {
    // 将page和pageSize转换成limit
    offset := (page - 1) * pageSize
    q.Offset(offset).Limit(pageSize)
    // 开始查询，查询分两步
    // 1. 查询总数量
    totalCount, err := q.queryTotalCount()
    if err != nil {
        return nil, err
    }
    // 2. 查询当前分页的数据
    queryResult, err := q.Query()
    if err != nil {
        return nil, err
    }
    // 重置总数
    queryResult.TotalCount = totalCount
    // 返回查询结果
    return queryResult, nil
}

// QueryRow 查询单条记录
func (q *Querier) QueryRow() (map[string]string, error) {
    q.Limit(1)
    queryResult, err := q.Query()
    if err != nil {
        return nil, err
    }
    if queryResult.RowsCount == 0 {
        return nil, nil
    }
    return queryResult.Rows[0], nil
}

// QueryScalar 查询单个值
func (q *Querier) QueryScalar() (string, error) {
    queryResult, err := q.Query()
    if err != nil {
        return "", err
    }
    if queryResult.RowsCount == 0 ||
        len(queryResult.Columns) == 0 {
        return "", nil
    }
    firstField := queryResult.Columns[0]
    v, _ := queryResult.Rows[0][firstField]
    return v, nil
}

// QueryAll 查询全部记录
func (q *Querier) QueryAll() ([]map[string]string, error) {
    queryResult, err := q.Query()
    if err != nil {
        return nil, err
    }
    if queryResult.RowsCount == 0 {
        return nil, nil
    }
    return queryResult.Rows, nil
}

// QueryAssoc 查询全部记录并以自定field为键返回对应的map
func (q *Querier) QueryAssoc(field string) (map[string]map[string]string, error) {
    queryResult, err := q.Query()
    if err != nil {
        return nil, err
    }
    if queryResult.RowsCount == 0 {
        return nil, nil
    }
    result := make(map[string]map[string]string)
    for _, row := range queryResult.Rows {
        v, ok := row[field]
        if !ok {
            continue
        }
        result[v] = row
    }
    return result, nil
}
