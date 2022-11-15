package gomodel

func init() {
    connMgr = NewConnectionManager()
    go connMgr.watchConns()
}
