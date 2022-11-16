package gomodel

import (
    "database/sql"
    "github.com/whencome/xlog"
    "testing"
)

/**
CREATE TABLE `user` (
  `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '用户ID',
  `name` varchar(50) NOT NULL DEFAULT '' COMMENT '用户姓名',
  `email` varchar(100) NOT NULL DEFAULT '' COMMENT '用户邮箱',
  `mobile` varchar(20) NOT NULL DEFAULT '' COMMENT '用户手机号',
  `track` linestring DEFAULT NULL COMMENT '用户轨迹',
  `create_time` int unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
  `update_time` int unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',
  `gender` bit(1) NOT NULL DEFAULT b'0',
  `stat` bit(30) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
*/

func getConn() (*sql.DB, error) {
    // 连接数据库
    conn, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/ddl_test?charset=utf8")
    if err != nil {
        return nil, err
    }
    // 返回连接信息
    return conn, nil
}

// 测试构造插入语句
func TestQuerier_QueryAll(t *testing.T) {
    conn, err := getConn()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }

    query := "SELECT * FROM `user`"
    q := NewRawQuerier(query)
    q.conn = conn
    rs, err := q.QueryAll()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }
    t.Logf("result: %+v", rs)
}

func TestQuerier_QueryByCond(t *testing.T) {
    xlog.Register("db", xlog.DefaultConfig())
    conn, err := getConn()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }

    // condition
    cond := NewAndCondition()
    cond.Add("id IN", []int{3, 4, 5, 6})

    // build query
    q := NewQuerier()
    q.Connect(conn)
    q.From("user")
    q.Where(cond)
    rs, err := q.QueryAll()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }
    t.Logf("result: %+v", rs)
}

func TestQuerier_QuerWithParam(t *testing.T) {
    xlog.Register("db", xlog.DefaultConfig())
    conn, err := getConn()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }

    // build query
    q := NewRawQuerier("SELECT * FROM `user` WHERE id = ?", 4)
    q.Connect(conn)
    rs, err := q.QueryRow()
    if err != nil {
        t.Logf("get connection failed: %s", err)
        t.Fail()
    }
    t.Logf("result: %+v", rs)
}
