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
	Logs   []Entry //日志
	Index0 int     //快照索引
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
	l.Logs = append(l.Logs, e)
}

func (l *Log) appends(entries ...Entry) {
	l.Logs = append(l.Logs, entries...)
}

func (l *Log) start() int {
	return l.Index0
}

// 获取多出的log
func (l *Log) cutEnd(index int) {
	l.Logs = l.Logs[0 : index-l.Index0]
}

func (l *Log) cutStart(index int) {
	//创建一个新数组来存储保留的日志条目
	newLogs := make([]Entry, len(l.Logs)-index)
	// 将需要保留的日志条目复制到新数组中
	copy(newLogs, l.Logs[index:])
	// 更新日志起始索引
	l.Index0 += index
	// 更新日志数组
	l.Logs = newLogs
}

// 获取最新日志
func (l *Log) lastLog() *Entry {
	return l.at(len(l.Logs) - 1)
}

// 获取最新日志的索引
func (l *Log) lastLogIndex() int {
	return len(l.Logs) - 1
}

// 获取当前索引位的
func (l *Log) at(idx int) *Entry {
	return &l.Logs[idx]
}

func (l *Log) slice(idx int) []Entry {
	return l.Logs[idx:]
}

func (l *Log) truncate(idx int) {
	l.Logs = l.Logs[:idx]
}

func Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
