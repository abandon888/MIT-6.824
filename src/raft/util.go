package raft

import (
	"log"
	"os"
)

// Debugging
//const Debug = false
//
//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug {
//		log.Printf(format, a...)
//	}
//	return
//}

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

// use direct ansi code to print yellow
func PrintYellow(msg string) {
	log.Printf("\033[33m%s\033[0m", msg)
}

// use direct ansi code to print red
func PrintRed(msg string) {
	log.Printf("\033[31m%s\033[0m", msg)
}

// use direct ansi code to print green
func PrintGreen(msg string) {
	log.Printf("\033[32m%s\033[0m", msg)
}

// use direct ansi code to print blue
func PrintBlue(msg string) {
	log.Printf("\033[34m%s\033[0m", msg)
}

// use # line to log important info
func PrintImportant(msg string) {
	log.Println("##################################################################")
	log.Println(msg)
	log.Println("##################################################################")
}
