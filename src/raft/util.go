package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// setupLogging 设置测试的日志输出到文件
func setupLogging() *log.Logger {
	// 打开或创建日志文件
	logFile, err := os.OpenFile("test.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
	}

	// 创建一个新的log.Logger实例
	testLogger := log.New(logFile, "TEST: ", log.Ldate|log.Ltime|log.Lshortfile)

	return testLogger
}
