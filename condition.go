package gomodel

import (
    "bytes"
    "fmt"
    "strings"
)

/*********************************************************
 ************** Definition of condition  *****************
 *********************************************************/

// Condition 定义一个sql条件组
type Condition struct {
    Logic    string        // 条件逻辑，AND / OR
    condData []interface{} // 条件组数据，优先级高于Conds
}

// NewAndCondition 创建一个And条件组
func NewAndCondition() *Condition {
    return &Condition{
        Logic:    "AND",
        condData: make([]interface{}, 0),
    }
}

// NewOrCondition 创建一个Or条件组
func NewOrCondition() *Condition {
    return &Condition{
        Logic:    "OR",
        condData: make([]interface{}, 0),
    }
}

// Add 添加一个条件
func (c *Condition) Add(field string, val interface{}) {
    c.condData = append(c.condData, map[string]interface{}{field: val})
}

// AddBatch 批量添加条件
func (c *Condition) AddBatch(batchConds []map[string]interface{}) {
    for _, bc := range batchConds {
        c.condData = append(c.condData, bc)
    }
}

// AddCondition 田间一个条件组
func (c *Condition) AddCondition(cc *Condition) {
    c.condData = append(c.condData, cc)
}

// AddRaw 添加写好的SQL条件
func (c *Condition) AddRaw(s string) {
    s = strings.TrimSpace(s)
    if s == "" {
        return
    }
    c.condData = append(c.condData, s)
}

// AddRawf 添加写好的SQL条件
func (c *Condition) AddRawf(format string, values ...interface{}) {
    format = strings.TrimSpace(format)
    if format == "" {
        return
    }
    c.condData = append(c.condData, fmt.Sprintf(format, values...))
}

// Build 构造条件
func (c *Condition) Build() (string, error) {
    patch, err := NewConditionBuilder().Build(c.condData, c.Logic)
    if err != nil {
        return "", err
    }
    return patch, nil
}

// BuildCondition 根据任意条件参数构造条件
func BuildCondition(conds interface{}) (string, error) {
    if conds == nil {
        return "", nil
    }
    return NewConditionBuilder().Build(conds, "AND")
}

/*********************************************************
 ********** Definition of condition builder  *************
 *********************************************************/

// ConditionBuilder 条件构造器，构造SQL查询条件
type ConditionBuilder struct{}

// NewConditionBuilder 创建一个新的条件构造器
func NewConditionBuilder() *ConditionBuilder {
    return &ConditionBuilder{}
}

// Build 构造SQL条件
func (cb *ConditionBuilder) Build(conds interface{}, logic string) (string, error) {
    return cb.buildCondition(conds, logic)
}

// addSQLCondition 写入SQL查询条件
func (cb *ConditionBuilder) addSQLCondition(buffer *bytes.Buffer, logic string, sqlPatch string) {
    if buffer.Len() > 0 {
        buffer.WriteString(" ")
        buffer.WriteString(logic)
        buffer.WriteString(" ")
    }
    buffer.WriteString(" ( ")
    buffer.WriteString(sqlPatch)
    buffer.WriteString(" ) ")
}

// buildCondition 构造逻辑查询条件
func (cb *ConditionBuilder) buildCondition(conds interface{}, logic string) (string, error) {
    // 如果条件为空，则认为查询全部
    if conds == nil {
        return "", nil
    }
    // 构造查询条件
    // 查询逻辑，logic = AND/OR
    logic = strings.ToUpper(strings.TrimSpace(logic))
    if logic == "" {
        logic = "AND"
    }
    buffer := &bytes.Buffer{}
    // 检查条件是否为已经写好的SQL段
    switch conds.(type) {
    // 查询内容为纯粹的sql段，无需处理
    case string:
        sqlPatch := string(conds.(string))
        cb.addSQLCondition(buffer, logic, sqlPatch)
    case []uint8:
        sqlPatch := string(conds.([]uint8))
        cb.addSQLCondition(buffer, logic, sqlPatch)
    case []rune:
        sqlPatch := string(conds.([]rune))
        cb.addSQLCondition(buffer, logic, sqlPatch)
    case []interface{}:
        condList := conds.([]interface{})
        if len(condList) == 0 {
            break
        }
        for _, v := range condList {
            sqlPatch, err := cb.buildCondition(v, logic)
            if err != nil {
                return "", err
            }
            cb.addSQLCondition(buffer, logic, sqlPatch)
        }
    case map[string]interface{}:
        mapCond := conds.(map[string]interface{})
        sqlPatch, err := cb.buildMapCondition(mapCond, logic)
        if err != nil {
            return "", err
        }
        cb.addSQLCondition(buffer, logic, sqlPatch)
    case []map[string]interface{}:
        listMapConds := conds.([]map[string]interface{})
        for _, mapConds := range listMapConds {
            sqlPatch, err := cb.buildMapCondition(mapConds, logic)
            if err != nil {
                return "", err
            }
            cb.addSQLCondition(buffer, logic, sqlPatch)
        }
    case *Condition:
        c := conds.(*Condition)
        sqlPatch, err := c.Build()
        if err != nil {
            return "", err
        }
        cb.addSQLCondition(buffer, logic, sqlPatch)
    default:
        return "", fmt.Errorf("unsupported condition data type %T of %#v", conds, conds)
    }
    return buffer.String(), nil
}

// buildMapCondition 根据map参数构造
func (cb *ConditionBuilder) buildMapCondition(conds map[string]interface{}, logic string) (string, error) {
    buffer := &bytes.Buffer{}
    for k, v := range conds {
        k = strings.TrimSpace(k)
        mapLogic := strings.ToUpper(k)
        // K如果是指定查询逻辑
        if mapLogic == "AND" || mapLogic == "OR" {
            sqlPatch, err := cb.buildCondition(v, mapLogic)
            if err != nil {
            }
            cb.addSQLCondition(buffer, mapLogic, sqlPatch)
            continue
        }
        // K如果是指定查询字段
        field := k
        matchLogic := "="
        logicSep := strings.Index(k, " ")
        if logicSep > 0 {
            field = k[:logicSep]
            matchLogic = k[logicSep+1:]
        }
        sqlPatch, err := cb.buildMatchLogicQuery(field, matchLogic, v)
        if err != nil {
            return "", err
        }
        cb.addSQLCondition(buffer, logic, sqlPatch)
        continue
    }
    return buffer.String(), nil
}

// buildMatchLogicQuery 构造匹配条件
func (cb *ConditionBuilder) buildMatchLogicQuery(field, matchLogic string, value interface{}) (string, error) {
    matchLogic = strings.ToUpper(strings.TrimSpace(matchLogic))
    if matchLogic == "" {
        matchLogic = "="
    }
    field = strings.ReplaceAll(field, "`", "")
    switch matchLogic {
    case "=", "!=", ">", ">=", "<", "<=", "<>", "LIKE", "NOT LIKE", "IS":
        fieldValue := NewValue(value).SQLValue()
        return fmt.Sprintf("%s %s %s", quote(field), matchLogic, fieldValue), nil
    case "IN", "NOT IN":
        inVales := transValue2Array(value)
        if len(inVales) == 0 {
            return "", fmt.Errorf("[%s] value not qualified", matchLogic)
        }
        fieldValues := make([]string, 0)
        for _, v := range inVales {
            vv := NewValue(v).SQLValue()
            fieldValues = append(fieldValues, vv)
        }
        return fmt.Sprintf("%s %s (%s)", quote(field), matchLogic, strings.Join(fieldValues, ", ")), nil
    case "BETWEEN", "NOT BETWEEN":
        betweenVales := transValue2Array(value)
        if len(betweenVales) != 2 {
            return "", fmt.Errorf("[%s] value count not qualified", matchLogic)
        }
        firstV := NewValue(betweenVales[0]).SQLValue()
        secondV := NewValue(betweenVales[1]).SQLValue()
        return fmt.Sprintf("%s %s %s AND %s", quote(field), matchLogic, firstV, secondV), nil
    default:
        return "", fmt.Errorf("unsupported match logic %s", matchLogic)
    }
}

/*****************************************************************
 ********** Definition of condition command builder  *************
 *****************************************************************/

// ConditionCommandBuilder 条件构造器，构造SQL查询条件
type ConditionCommandBuilder struct {
    *SqlCommand
    buffer bytes.Buffer
}

// NewConditionCommandBuilder 创建一个新的条件构造器
func NewConditionCommandBuilder() *ConditionCommandBuilder {
    return &ConditionCommandBuilder{
        SqlCommand: &SqlCommand{
            Command: "",
            Values:  make([]interface{}, 0),
        },
        buffer: bytes.Buffer{},
    }
}

// Build 构造SQL条件
func (cb *ConditionCommandBuilder) Build(conds interface{}, logic string) (*SqlCommand, error) {
    cb.buildCondition(conds, logic)
    cb.Command = cb.buffer.String()
    return cb.SqlCommand, nil
}

// addSQLCondition 写入SQL查询条件
func (cb *ConditionCommandBuilder) addSQLCondition(logic string, sqlPatch string, values ...interface{}) {
    if cb.buffer.Len() > 0 {
        cb.buffer.WriteString(" ")
        cb.buffer.WriteString(logic)
        cb.buffer.WriteString(" ")
    }
    cb.buffer.WriteString(" ( ")
    cb.buffer.WriteString(sqlPatch)
    cb.buffer.WriteString(" ) ")
    if len(values) > 0 {
        cb.Values = append(cb.Values, values...)
    }
}

func (cb *ConditionCommandBuilder) addSQLCommand(logic string, sqlCommand *SqlCommand) {
    if sqlCommand == nil {
        return
    }
    if cb.buffer.Len() > 0 {
        cb.buffer.WriteString(" ")
        cb.buffer.WriteString(logic)
        cb.buffer.WriteString(" ")
    }
    cb.buffer.WriteString(" ( ")
    cb.buffer.WriteString(sqlCommand.Command)
    cb.buffer.WriteString(" ) ")
    if len(sqlCommand.Values) > 0 {
        cb.Values = append(cb.Values, sqlCommand.Values...)
    }
}

// buildCondition 构造逻辑查询条件
func (cb *ConditionCommandBuilder) buildCondition(conds interface{}, logic string) error {
    // 如果条件为空，则认为查询全部
    if conds == nil {
        return nil
    }
    // 构造查询条件
    // 查询逻辑，logic = AND/OR
    logic = strings.ToUpper(strings.TrimSpace(logic))
    if logic == "" {
        logic = "AND"
    }
    // 检查条件是否为已经写好的SQL段
    switch conds.(type) {
    // 查询内容为纯粹的sql段，无需处理
    case string:
        sqlPatch := string(conds.(string))
        cb.addSQLCondition(logic, sqlPatch)
    case []uint8:
        sqlPatch := string(conds.([]uint8))
        cb.addSQLCondition(logic, sqlPatch)
    case []rune:
        sqlPatch := string(conds.([]rune))
        cb.addSQLCondition(logic, sqlPatch)
    case []interface{}:
        condList := conds.([]interface{})
        if len(condList) == 0 {
            break
        }
        for _, v := range condList {
            err := cb.buildCondition(v, logic)
            if err != nil {
                return err
            }
        }
    case map[string]interface{}:
        mapCond := conds.(map[string]interface{})
        sqlPatch, err := cb.buildMapCondition(mapCond, logic)
        if err != nil {
            return err
        }
        cb.addSQLCondition(logic, sqlPatch)
    case []map[string]interface{}:
        listMapConds := conds.([]map[string]interface{})
        for _, mapConds := range listMapConds {
            sqlPatch, err := cb.buildMapCondition(mapConds, logic)
            if err != nil {
                return err
            }
            cb.addSQLCondition(logic, sqlPatch)
        }
    case *Condition:
        c := conds.(*Condition)
        sqlPatch, err := c.Build()
        if err != nil {
            return err
        }
        cb.addSQLCondition(logic, sqlPatch)
    default:
        return fmt.Errorf("unsupported condition data type %T of %#v", conds, conds)
    }
    return nil
}

// buildMapCondition 根据map参数构造
func (cb *ConditionCommandBuilder) buildMapCondition(conds map[string]interface{}, logic string) (string, error) {
    buffer := &bytes.Buffer{}
    for k, v := range conds {
        k = strings.TrimSpace(k)
        mapLogic := strings.ToUpper(k)
        // K如果是指定查询逻辑
        if mapLogic == "AND" || mapLogic == "OR" {
            err := cb.buildCondition(v, mapLogic)
            if err != nil {
            }
            continue
        }
        // K如果是指定查询字段
        field := k
        matchLogic := "="
        logicSep := strings.Index(k, " ")
        if logicSep > 0 {
            field = k[:logicSep]
            matchLogic = k[logicSep+1:]
        }
        sqlPatch, err := cb.buildMatchLogicQuery(field, matchLogic, v)
        if err != nil {
            return "", err
        }
        cb.addSQLCommand(logic, sqlPatch)
        continue
    }
    return buffer.String(), nil
}

// buildMatchLogicQuery 构造匹配条件
func (cb *ConditionCommandBuilder) buildMatchLogicQuery(field, matchLogic string, value interface{}) (*SqlCommand, error) {
    condCmd := NewSqlCommand()
    matchLogic = strings.ToUpper(strings.TrimSpace(matchLogic))
    if matchLogic == "" {
        matchLogic = "="
    }
    field = strings.ReplaceAll(field, "`", "")
    switch matchLogic {
    case "=", "!=", ">", ">=", "<", "<=", "<>", "LIKE", "NOT LIKE", "IS":
        fieldValue := NewValue(value).SQLValue()
        condCmd.Command = fmt.Sprintf("%s %s ?", quote(field), matchLogic)
        condCmd.AddValue(fieldValue)
        return condCmd, nil
    case "IN", "NOT IN":
        inVales := transValue2Array(value)
        if len(inVales) == 0 {
            return nil, fmt.Errorf("[%s] value not qualified", matchLogic)
        }
        fieldValues := make([]string, 0)
        for _, v := range inVales {
            vv := NewValue(v).SQLValue()
            fieldValues = append(fieldValues, vv)
        }
        condCmd.Command = fmt.Sprintf("%s %s (?)", quote(field), matchLogic)
        condCmd.AddValue(fieldValues)
        return condCmd, nil
    case "BETWEEN", "NOT BETWEEN":
        betweenVales := transValue2Array(value)
        if len(betweenVales) != 2 {
            return nil, fmt.Errorf("[%s] value count not qualified", matchLogic)
        }
        firstV := NewValue(betweenVales[0]).SQLValue()
        secondV := NewValue(betweenVales[1]).SQLValue()
        condCmd.Command = fmt.Sprintf("%s %s ? AND ?", quote(field), matchLogic)
        condCmd.AddValues(firstV, secondV)
        return condCmd, nil
    default:
        return nil, fmt.Errorf("unsupported match logic %s", matchLogic)
    }
}
