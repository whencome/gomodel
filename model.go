package gomodel

import (
    "bytes"
    "database/sql"
    "errors"
    "fmt"
    "reflect"
    "strconv"
    "strings"

    "github.com/whencome/xlog"
)

/************************************************************
 ******                SECTION OF MODELER               *****
 ************************************************************/

// Modeler 定义一个model接口
type Modeler interface {
    GetDatabase() string
    GetTableName() string
    AutoIncrementField() string
    GetDBFieldTag() string
}

/************************************************************
 ******            SECTION OF MODEL MANAGER             *****
 ************************************************************/
// 定义Modeler调整方法
type PreWriteAdjustFunc func(Modeler) Modeler
type PostReadAdjustFunc func(Modeler, map[string]string) Modeler

// 定义一个sql值调整方法，用于获取数据写入数据库的值
type SqlValueAdjustFunc func(interface{}) string

// 定义字段调整方法
type QueryFieldAdjustFunc func(string) string

// 定义默认的SQL Value调整方法
func DefaultSqlValueCallback(v interface{}) string {
    return NewValue(v).SQLValue()
}

// Manager基类
type Manager interface {
    SetDBInitFunc(func() (*sql.DB, error))
    GetConnection() (*sql.DB, error)
}

// ModelManager 定义ModelManager结构体，用于数据model操作管理
type ModelManager struct {
    Manager
    *ModelerInfo
    Model             Modeler
    Settings          *Options
    GetDBFunc         func() (*sql.DB, error)
    preWriteFunc      PreWriteAdjustFunc
    postReadFunc      PostReadAdjustFunc
    preQueryFieldFunc QueryFieldAdjustFunc
    sqlValueCallbacks map[string]SqlValueAdjustFunc
}

// NewModelManager 创建一个新的ModelManager
func NewModelManager(m Modeler) *ModelManager {
    return &ModelManager{
        Model:             m,
        ModelerInfo:       pool.Parse(m),
        Settings:          NewDefaultOptions(),
        sqlValueCallbacks: make(map[string]SqlValueAdjustFunc, 0),
    }
}

// NewCustomModelManager 创建一个定制化的ModelManager
func NewCustomModelManager(m Modeler, opts *Options) *ModelManager {
    mm := NewModelManager(m)
    mm.SetOptions(opts)
    return mm
}

// SetOptions 设置选项
func (mm *ModelManager) SetOptions(opts *Options) {
    if opts == nil {
        return
    }
    mm.Settings = opts
}

// GetTableName 获取Model对应的数据表名
func (mm *ModelManager) GetTableName() string {
    if mm.Model == nil {
        return ""
    }
    return mm.Model.GetTableName()
}

// GetDatabase 获取数据库名称（返回配置中的名称，不要使用实际数据库名称，因为实际数据库名称在不同环境可能不一样）
func (mm *ModelManager) GetDatabase() string {
    if mm.Model == nil {
        return ""
    }
    return mm.Model.GetDatabase()
}

// SetDBInitFunc 设置数据库初始化函数
func (mm *ModelManager) SetDBInitFunc(f func() (*sql.DB, error)) {
    mm.GetDBFunc = f
}

// GetConnection 获取数据库连接
func (mm *ModelManager) GetConnection() (*sql.DB, error) {
    if mm.GetDBFunc != nil {
        return mm.GetDBFunc()
    }
    return globalResManager.GetConnection(mm.Model.GetDatabase())
}

// NewAndCondition 创建一个AND条件组
func (mm *ModelManager) NewAndCondition() *Condition {
    return NewAndCondition()
}

// NewOrCondition 创建一个OR条件组
func (mm *ModelManager) NewOrCondition() *Condition {
    return NewOrCondition()
}

// NewQuerier 创建一个查询对象
func (mm *ModelManager) NewQuerier() *Querier {
    conn, err := mm.GetConnection()
    if err != nil {
        xlog.Errorf("get db [%s] connection failed: %s", mm.GetDatabase(), err)
        conn = nil
    }
    return NewModelQuerier(mm.Model).Connect(conn).SetOptions(mm.Settings).Select(mm.QueryFieldsString())
}

// NewRawQuerier 创建一个查询对象
func (mm *ModelManager) NewRawQuerier(querySQL string) *Querier {
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        xlog.Errorf("get db [%s] connection failed: %s", mm.GetDatabase(), err)
        conn = nil
    }
    return NewRawQuerier(querySQL).SetOptions(mm.Settings).Connect(conn)
}

// NewCommander 创建一个Commander对象
func (mm *ModelManager) NewCommander() *Commander {
    conn, err := mm.GetConnection()
    if err != nil {
        xlog.Errorf("get db [%s] connection failed: %s", mm.GetDatabase(), err)
        conn = nil
    }
    return NewCommander(mm.Settings).Connect(conn)
}

// getInsertFields 获取插入的字段列表
func (mm *ModelManager) getInsertFields() []string {
    fields := make([]string, 0)
    for _, field := range mm.Fields {
        if field == mm.Model.AutoIncrementField() {
            continue
        }
        fields = append(fields, field)
    }
    return fields
}

// getQueryFields 获取查询的字段列表
func (mm *ModelManager) getQueryFields() []string {
    fields := make([]string, 0)
    for _, field := range mm.Fields {
        if mm.preQueryFieldFunc == nil {
            fields = append(fields, field)
        } else {
            fields = append(fields, mm.preQueryFieldFunc(field))
        }
    }
    return fields
}

// QueryFieldsString 获取查询字段字符串
func (mm *ModelManager) QueryFieldsString() string {
    queryFields := mm.getQueryFields()
    quoted := bytes.Buffer{}
    for i, f := range queryFields {
        if i > 0 {
            quoted.WriteString(",")
        }
        quoted.WriteString(quote(f))
    }
    return quoted.String()
}

// MatchObject 匹配对象，检查对象类型是否匹配
func (mm *ModelManager) MatchObject(obj interface{}) bool {
    if obj == nil {
        return false
    }
    modelObj, ok := obj.(Modeler)
    if !ok {
        return false
    }
    if mm.Model == nil ||
        modelObj.GetDatabase() != mm.Model.GetDatabase() ||
        modelObj.GetTableName() != mm.Model.GetTableName() {
        return false
    }
    return true
}

// SetPreQueryFieldFunc 设置查询前的字段调整方法
func (mm *ModelManager) SetPreQueryFieldFunc(f QueryFieldAdjustFunc) {
    mm.preQueryFieldFunc = f
}

// SetPreWriteFunc 设置写入前的modeler调整方法
func (mm *ModelManager) SetPreWriteFunc(f PreWriteAdjustFunc) {
    mm.preWriteFunc = f
}

// SetPostReadFunc 设置读取后的MODELER的调整方法
func (mm *ModelManager) SetPostReadFunc(f PostReadAdjustFunc) {
    mm.postReadFunc = f
}

// SetValueCallback 设置获取字段值的回调方法
func (mm *ModelManager) SetSqlValueCallback(f string, callback SqlValueAdjustFunc) {
    mm.sqlValueCallbacks[f] = callback
}

// GetValueCallback 获取字段值格式化方法
func (mm *ModelManager) GetSqlValueCallback(f string) SqlValueAdjustFunc {
    if c, ok := mm.sqlValueCallbacks[f]; ok && c != nil {
        return c
    }
    return DefaultSqlValueCallback
}

// 获取字段的值
func (mm *ModelManager) GetSqlValue(f string, v interface{}) string {
    vf := mm.GetSqlValueCallback(f)
    return vf(v)
}

// 将任何满足条件的对象转换为Modeler
func (mm *ModelManager) convert2Model(obj interface{}) (Modeler, bool) {
    if !mm.MatchObject(obj) {
        return nil, false
    }
    modelObj, ok := obj.(Modeler)
    if !ok {
        return nil, false
    }
    if mm.preWriteFunc != nil {
        modelObj = mm.preWriteFunc(modelObj)
    }
    return modelObj, true
}

func (mm *ModelManager) parseModelers(data interface{}) ([]Modeler, error) {
    if data == nil {
        return nil, errors.New("empty data not acceptable")
    }
    modelers := make([]Modeler, 0)
    switch reflect.TypeOf(data).Kind() {
    case reflect.Slice, reflect.Array:
        valData := reflect.ValueOf(data)
        arrSize := valData.Len()
        if arrSize == 0 {
            return nil, errors.New("empty data")
        }
        for i := 0; i < arrSize; i++ {
            v := valData.Index(i).Interface()
            modelObj, ok := mm.convert2Model(v)
            if !ok {
                return nil, fmt.Errorf("invalid modeler, expect %T, got %T", mm.Model, v)
            }
            modelers = append(modelers, modelObj)
        }
    case reflect.Struct:
        modelObj, ok := mm.convert2Model(data)
        if !ok {
            return nil, fmt.Errorf("invalid modeler, expect %T, got %T", mm.Model, data)
        }
        modelers = append(modelers, modelObj)
    }
    if len(modelers) == 0 {
        return nil, errors.New("empty data")
    }
    return modelers, nil
}

// BuildBatchInsertSql 构造批量插入语句
func (mm *ModelManager) BuildBatchInsertSql(data interface{}) (string, error) {
    if data == nil {
        return "", errors.New("can not insert nil data")
    }
    objects, err := mm.parseModelers(data)
    if err != nil {
        return "", err
    }
    // 先获取字段列表
    insertFields := mm.getInsertFields()
    insertSql := fmt.Sprintf("INSERT INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(insertFields, "`,`"))
    for i, object := range objects {
        values := make([]string, 0)
        rv := reflect.ValueOf(object)
        for _, field := range insertFields {
            propName := mm.FieldMaps[field]
            val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
            values = append(values, val)
        }
        if i > 0 {
            insertSql += ","
        }
        insertSql += fmt.Sprintf("(%s)", strings.Join(values, ","))
    }
    return insertSql, nil
}

// BuildInsertSql 构造单条插入语句
func (mm *ModelManager) BuildInsertSql(object interface{}) (string, error) {
    // 类型检查与转换
    modelObj, ok := mm.convert2Model(object)
    if !ok {
        return "", fmt.Errorf("insert action expect a %T object, but %T found", mm.Model, object)
    }
    // 先获取字段列表
    insertFields := mm.getInsertFields()
    insertSql := fmt.Sprintf("INSERT INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(insertFields, "`,`"))
    // 构造插入数据
    values := make([]string, 0)
    rv := reflect.ValueOf(modelObj)
    for _, field := range insertFields {
        propName := mm.FieldMaps[field]
        val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
        values = append(values, val)
    }
    insertSql += fmt.Sprintf("(%s)", strings.Join(values, ","))
    return insertSql, nil
}

// BuildReplaceIntoSql 构造REPLACE INTO语句
func (mm *ModelManager) BuildReplaceIntoSql(data interface{}) (string, error) {
    if data == nil {
        return "", errors.New("can not replace into nil data")
    }
    objects, err := mm.parseModelers(data)
    if err != nil {
        return "", err
    }
    // 先获取字段列表
    allFields := mm.Fields
    replaceSql := fmt.Sprintf("REPLACE INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(allFields, "`,`"))
    for i, object := range objects {
        values := make([]string, 0)
        rv := reflect.ValueOf(object)
        for _, field := range allFields {
            propName := mm.FieldMaps[field]
            val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
            values = append(values, val)
        }
        if i > 0 {
            replaceSql += ","
        }
        replaceSql += fmt.Sprintf("(%s)", strings.Join(values, ","))
    }
    return replaceSql, nil
}

// BuildUpdateSql 构造更新语句
func (mm *ModelManager) BuildUpdateSql(object interface{}) (string, error) {
    // 类型检查与转换
    modelObj, ok := mm.convert2Model(object)
    if !ok {
        return "", fmt.Errorf("insert action expect a %T object, but %T found", mm.Model, object)
    }
    // 先获取字段列表
    updateFields := mm.getInsertFields()
    updateSQL := fmt.Sprintf("UPDATE `%s` SET ", mm.GetTableName())
    // 构造更新数据
    rv := reflect.ValueOf(modelObj)
    for i, field := range updateFields {
        propName := mm.FieldMaps[field]
        val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
        if i > 0 {
            updateSQL += ", "
        }
        updateSQL += fmt.Sprintf(" `%s` = %s", field, val)
    }
    // 自增ID
    autoIncrementField := mm.Model.AutoIncrementField()
    propName := mm.FieldMaps[autoIncrementField]
    idVal := mm.GetSqlValue(autoIncrementField, rv.Elem().FieldByName(propName).Interface())
    updateSQL += fmt.Sprintf(" WHERE `%s` = %s ", autoIncrementField, idVal)
    return updateSQL, nil
}

// BuildUpdateSqlByCond 构造更新语句
func (mm *ModelManager) BuildUpdateSqlByCond(params map[string]interface{}, cond interface{}) (string, error) {
    if len(params) <= 0 {
        return "", errors.New("nothing to update")
    }
    where, err := NewConditionBuilder().Build(cond, "AND")
    if err != nil {
        return "", err
    }
    if strings.TrimSpace(where) == "" {
        return "", errors.New("update condition can not be empty")
    }
    // 构造更新语句
    updateSQL := fmt.Sprintf("UPDATE `%s` SET ", mm.GetTableName())
    counter := 0
    for field, iv := range params {
        // val := NewValue(iv).SQLValue()
        val := mm.GetSqlValue(field, iv)
        if counter > 0 {
            updateSQL += ", "
        }
        updateSQL += fmt.Sprintf(" `%s` = %s", field, val)
        counter++
    }
    updateSQL += fmt.Sprintf(" WHERE %s ", where)
    return updateSQL, nil
}

// BuildDeleteSql 构造删除语句
func (mm *ModelManager) BuildDeleteSql(conds interface{}) (string, error) {
    delSQL := fmt.Sprintf("DELETE FROM `%s` WHERE ", mm.GetTableName())
    where, err := BuildCondition(conds)
    if err != nil {
        return "", err
    }
    // 不支持无条件删除
    if where == "" {
        return "", fmt.Errorf("delete condition can not be empty")
    }
    delSQL += where
    return delSQL, nil
}

// buildInsertCommand return insert command and values, this may be safer than buildInsertSql
func (mm *ModelManager) buildInsertCommand(object interface{}) (*SqlCommand, error) {
    modelObj, ok := mm.convert2Model(object)
    if !ok {
        return nil, fmt.Errorf("insert action expect a %T object, but %T found", mm.Model, object)
    }
    insertFields := mm.getInsertFields()
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("INSERT INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(insertFields, "`,`"))
    valueMarks := make([]string, 0)
    rv := reflect.ValueOf(modelObj)
    for _, field := range insertFields {
        propName := mm.FieldMaps[field]
        val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
        sqlCmd.AddValue(val)
        valueMarks = append(valueMarks, "?")
    }
    sqlCmd.Writef("(%s)", strings.Join(valueMarks, ","))
    return sqlCmd, nil
}

// buildBatchInsertCommand 构造批量插入语句
func (mm *ModelManager) buildBatchInsertCommand(data interface{}) (*SqlCommand, error) {
    objects, err := mm.parseModelers(data)
    if err != nil {
        return nil, err
    }
    // 先获取字段列表
    insertFields := mm.getInsertFields()
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("INSERT INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(insertFields, "`,`"))
    for i, object := range objects {
        valueMarks := make([]string, 0)
        rv := reflect.ValueOf(object)
        for _, field := range insertFields {
            propName := mm.FieldMaps[field]
            val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
            sqlCmd.AddValue(val)
            valueMarks = append(valueMarks, "?")
        }
        if i > 0 {
            sqlCmd.WriteString(",")
        }
        sqlCmd.Writef("(%s)", strings.Join(valueMarks, ","))
    }
    return sqlCmd, nil
}

// buildReplaceIntoCommand 构造REPLACE INTO语句
func (mm *ModelManager) buildReplaceIntoCommand(data interface{}) (*SqlCommand, error) {
    objects, err := mm.parseModelers(data)
    if err != nil {
        return nil, err
    }
    allFields := mm.Fields
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("REPLACE INTO %s(`%s`) VALUES", quote(mm.GetTableName()), strings.Join(allFields, "`,`"))
    for i, object := range objects {
        valueMarks := make([]string, 0)
        rv := reflect.ValueOf(object)
        for _, field := range allFields {
            propName := mm.FieldMaps[field]
            val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
            sqlCmd.AddValue(val)
            valueMarks = append(valueMarks, "?")
        }
        if i > 0 {
            sqlCmd.WriteString(",")
        }
        sqlCmd.Writef("(%s)", strings.Join(valueMarks, ","))
    }
    return sqlCmd, nil
}

// buildUpdateCommand 构造更新语句
func (mm *ModelManager) buildUpdateCommand(object interface{}) (*SqlCommand, error) {
    modelObj, ok := mm.convert2Model(object)
    if !ok {
        return nil, fmt.Errorf("insert action expect a %T object, but %T found", mm.Model, object)
    }
    updateFields := mm.getInsertFields()
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("UPDATE `%s` SET ", mm.GetTableName())
    rv := reflect.ValueOf(modelObj)
    for i, field := range updateFields {
        if i > 0 {
            sqlCmd.WriteString(", ")
        }
        propName := mm.FieldMaps[field]
        val := mm.GetSqlValue(field, rv.Elem().FieldByName(propName).Interface())
        sqlCmd.Writef(" `%s` = ?", field)
        sqlCmd.AddValue(val)
    }
    autoIncrementField := mm.Model.AutoIncrementField()
    propName := mm.FieldMaps[autoIncrementField]
    idVal := mm.GetSqlValue(autoIncrementField, rv.Elem().FieldByName(propName).Interface())
    sqlCmd.Writef(" WHERE `%s` = ? ", autoIncrementField)
    sqlCmd.AddValue(idVal)
    return sqlCmd, nil
}

// buildUpdateCommandByCond 构造更新语句
func (mm *ModelManager) buildUpdateCommandByCond(params map[string]interface{}, cond interface{}) (*SqlCommand, error) {
    if len(params) <= 0 {
        return nil, errors.New("nothing to update")
    }
    where, err := NewConditionBuilder().Build(cond, "AND")
    if err != nil {
        return nil, err
    }
    if strings.TrimSpace(where) == "" {
        return nil, errors.New("update condition can not be empty")
    }
    // 构造更新语句
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("UPDATE `%s` SET ", mm.GetTableName())
    counter := 0
    for field, iv := range params {
        if counter > 0 {
            sqlCmd.WriteString(", ")
        }
        val := mm.GetSqlValue(field, iv)
        sqlCmd.AddValue(val)
        sqlCmd.Writef(" `%s` = ?", field)
        counter++
    }
    sqlCmd.Writef(" WHERE %s ", where)
    return sqlCmd, nil
}

// buildDeleteCommand 构造删除语句
func (mm *ModelManager) buildDeleteCommand(conds interface{}) (*SqlCommand, error) {
    sqlCmd := NewSqlCommand()
    sqlCmd.Writef("DELETE FROM `%s` WHERE ", mm.GetTableName())
    where, err := BuildCondition(conds)
    if err != nil {
        return nil, err
    }
    // 不支持无条件删除
    if where == "" {
        return nil, fmt.Errorf("delete condition can not be empty")
    }
    sqlCmd.WriteString(where)
    return sqlCmd, nil
}

// Insert 插入一条新数据
func (mm *ModelManager) Insert(obj interface{}) (int64, error) {
    // 构造插入语句
    insertCmd, err := mm.buildInsertCommand(obj)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 执行插入操作
    l := NewLogger()
    l.SetCommand(insertCmd.Command())
    defer l.Close()
    // 执行插入操作
    result, err := conn.Exec(insertCmd.Command(), insertCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    return result.LastInsertId()
}

// InsertBatch 批量插入数据
func (mm *ModelManager) InsertBatch(objs interface{}) (int64, error) {
    // 构造插入语句
    insertCmd, err := mm.buildBatchInsertCommand(objs)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 获取日志对象
    l := NewLogger()
    l.SetCommand(insertCmd.Command())
    defer l.Close()
    // 执行插入操作
    _, err = conn.Exec(insertCmd.Command(), insertCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    // 只返回是否成功
    return 1, nil
}

// ReplaceInto 批量插入/更新数据
func (mm *ModelManager) ReplaceInto(objs interface{}) (int64, error) {
    replaceCmd, err := mm.buildReplaceIntoCommand(objs)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 获取日志对象
    l := NewLogger()
    l.SetCommand(replaceCmd.Command())
    defer l.Close()
    // 执行插入操作
    _, err = conn.Exec(replaceCmd.Command(), replaceCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    // 只返回是否成功
    return 1, nil
}

// Update 更新数据
func (mm *ModelManager) Update(obj interface{}) (int64, error) {
    // 构造更新语句
    updateCmd, err := mm.buildUpdateCommand(obj)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 获取日志对象
    l := NewLogger()
    l.SetCommand(updateCmd.Command())
    defer l.Close()
    // 执行插入操作
    result, err := conn.Exec(updateCmd.Command(), updateCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    return result.RowsAffected()
}

// UpdateByCond 根据条件更新数据
func (mm *ModelManager) UpdateByCond(params map[string]interface{}, cond interface{}) (int64, error) {
    // 构造更新语句
    updateCmd, err := mm.buildUpdateCommandByCond(params, cond)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 获取日志对象
    l := NewLogger()
    l.SetCommand(updateCmd.Command())
    defer l.Close()
    // 执行更新操作
    result, err := conn.Exec(updateCmd.Command(), updateCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    return result.RowsAffected()
}

// Delete 删除数据
func (mm *ModelManager) Delete(cond interface{}) (int64, error) {
    // 构造删除语句
    delCmd, err := mm.buildDeleteCommand(cond)
    if err != nil {
        return 0, err
    }
    // 获取数据库连接
    conn, err := mm.GetConnection()
    if err != nil {
        return 0, err
    }
    // 获取日志对象
    l := NewLogger()
    l.SetCommand(delCmd.Command())
    defer l.Close()
    // 执行删除操作
    result, err := conn.Exec(delCmd.Command(), delCmd.Values()...)
    if err != nil {
        l.Fail(err.Error())
        return 0, err
    }
    l.Success()
    return result.RowsAffected()
}

// MapToModeler 将map转换为Modeler对象(待测试)
func (mm *ModelManager) MapToModeler(data map[string]string) Modeler {
    if len(data) == 0 || mm.Model == nil {
        return nil
    }
    // 创建对象并进行转换
    t := reflect.TypeOf(mm.Model)
    // 指针类型获取真正type需要调用Elem
    if t.Kind() == reflect.Ptr {
        t = t.Elem()
    }
    // 调用反射创建对象
    newModel := reflect.New(t)
    // 遍历字段列表并设置值
    for field, val := range data {
        // 1. 检查model是否包含该字段
        propName, ok := mm.FieldMaps[field]
        if !ok {
            continue
        }
        // 设置值
        reflectField := newModel.Elem().FieldByName(propName)
        propTypeKind := reflectField.Type().Kind()
        switch propTypeKind {
        case reflect.String:
            reflectField.SetString(NewValue(val).String())
        case reflect.Bool:
            reflectField.SetBool(NewValue(val).Boolean())
        case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
            reflectField.SetInt(NewValue(val).Int64())
        case reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16, reflect.Uint8:
            reflectField.SetUint(NewValue(val).Uint64())
        case reflect.Float64:
            reflectField.SetFloat(NewValue(val).Float64())
        default: // 其他类型暂不支持
            break
        }
    }
    // 读取后的数据处理
    m := newModel.Interface().(Modeler)
    if mm.postReadFunc != nil {
        m = mm.postReadFunc(m, data)
    }
    // 返回结果
    return m
}

// Map 将model转换为map
func (mm *ModelManager) Map(obj Modeler) map[string]interface{} {
    if !mm.MatchObject(obj) {
        return nil
    }
    retData := make(map[string]interface{})
    fields := mm.Fields
    rv := reflect.ValueOf(obj)
    for _, field := range fields {
        propName := mm.FieldMaps[field]
        val := rv.Elem().FieldByName(propName).Interface()
        retData[field] = val
    }
    // 返回结果
    return retData
}

// FindPage 分页查询
func (mm *ModelManager) FindPage(conds interface{}, orderBy string, page, pageSize int) (*QueryResult, error) {
    return mm.NewQuerier().From(mm.GetTableName()).Where(conds).OrderBy(orderBy).QueryPage(page, pageSize)
}

// FindOne 查询单条数据
func (mm *ModelManager) FindOne(conds interface{}, orderBy string) (Modeler, error) {
    data, err := mm.NewQuerier().From(mm.GetTableName()).Where(conds).OrderBy(orderBy).QueryRow()
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, nil
    }
    mData := mm.MapToModeler(data)
    return mData, nil
}

// FindAll 查询满足条件的全部数据
func (mm *ModelManager) FindAll(conds interface{}, orderBy string) ([]interface{}, error) {
    queryRs, err := mm.NewQuerier().From(mm.GetTableName()).Where(conds).OrderBy(orderBy).Query()
    if err != nil {
        return nil, err
    }
    if queryRs.RowsCount == 0 {
        return nil, nil
    }
    list := make([]interface{}, 0)
    for _, d := range queryRs.Rows {
        v := mm.MapToModeler(d)
        list = append(list, v)
    }
    return list, nil
}

// Count 数据统计
func (mm *ModelManager) Count(conds interface{}) (int, error) {
    data, err := mm.NewQuerier().Select("COUNT(0)").From(mm.GetTableName()).Where(conds).QueryScalar()
    if err != nil {
        return 0, err
    }
    return strconv.Atoi(data)
}

// QueryAll 根据SQL查询满足条件的全部数据
func (mm *ModelManager) QueryAll(querySql string) (*QueryResult, error) {
    queryRs, err := mm.NewRawQuerier(querySql).Query()
    if err != nil {
        return nil, err
    }
    return queryRs, nil
}

// QueryRow 根据SQL查询满足条件的全部数据
func (mm *ModelManager) QueryRow(querySql string) (map[string]string, error) {
    row, err := mm.NewRawQuerier(querySql).Limit(1).QueryRow()
    if err != nil {
        return nil, err
    }
    return row, nil
}

// QueryAssoc 根据SQL查询满足条件的全部数据
func (mm *ModelManager) QueryAssoc(querySql string, field string) (map[string]map[string]string, error) {
    mapData, err := mm.NewRawQuerier(querySql).QueryAssoc(field)
    if err != nil {
        return nil, err
    }
    return mapData, nil
}
