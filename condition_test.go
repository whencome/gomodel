package gomodel

import (
    "log"
    "testing"
)

func TestCondition_Build(t *testing.T) {
    cond := NewAndCondition()
    cond.Add("name", "whencome")
    cond.Add("age", 18)
    cond.AddRaw("score > 80")
    cond.AddRawf("book_name like '%s'", "golang")
    orCond := NewOrCondition()
    orCond.Add("hobbies", "basketball")
    orCond.Add("hobbies", "ping pang")
    orCond.Add("hobbies", "movie")
    cond.AddCondition(orCond)
    cond.Add("what_else", "nothing")
    andCond := NewAndCondition()
    andCond.Add("class", "class_1")
    cond.AddCondition(andCond)
    sqlPatch, err := cond.Build()
    if err != nil {
        log.Printf("build sql patch fail: %s", err)
        t.Fail()
    }
    log.Println(sqlPatch)
}
