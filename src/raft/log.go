package raft

import "fmt"

// Entry 具体的日志内容实例
type Entry struct {
	Command interface{} //日志条目
	Term    int         //候选人最后日志条目的任期号
	Index   int
}

// Log 日志
type Log struct {
	log    []Entry //日志
	index0 int     //快照索引
}

// 打印日志
func (e Entry) String() string {
	return fmt.Sprintf("term %v", e.Term)
}

// 初始化log
func mkLogEntry() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

// 添加日志
func (l *Log) append(e Entry) {
	l.log = append(l.log, e)
}

func (l *Log) appends(entries ...Entry) {
	l.log = append(l.log, entries...)
}

func (l *Log) start() int {
	return l.index0
}

// 获取多出的log
func (l *Log) cutend(index int) {
	l.log = l.log[0 : index-l.index0]
}

func (l *Log) cutstart(index int) {
	l.index0 += index
	l.log = l.log[index:]
}

// 获取最新日志的索引
func (l *Log) lastLogIndex() int {
	return len(l.log) - 1
}

// 获取当前索引位的
func (l *Log) at(idx int) *Entry {
	return &l.log[idx]
}

func (l *Log) slice(idx int) []Entry {
	return l.log[idx:]
}

func (l *Log) truncate(idx int) {
	l.log = l.log[:idx]
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
