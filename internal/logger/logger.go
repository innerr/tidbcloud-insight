package logger

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ConcurrencyInfoProvider interface {
	GetCurrentConcurrency() int
	GetDesiredConcurrency() int
}

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

type Logger struct {
	mu              sync.RWMutex
	concurrencyProv ConcurrencyInfoProvider
	verbose         bool
}

var defaultLogger = &Logger{}

func init() {
	log.SetFlags(0)
}

func SetVerbose(v bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.verbose = v
}

func IsVerbose() bool {
	defaultLogger.mu.RLock()
	defer defaultLogger.mu.RUnlock()
	return defaultLogger.verbose
}

func SetConcurrencyProvider(p ConcurrencyInfoProvider) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.concurrencyProv = p
}

func getGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.ParseUint(idField, 10, 64)
	return id
}

func formatTimestamp() string {
	return time.Now().Format("20060102-15:04:05")
}

func (l *Logger) getConcurrency() (current, desired int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.concurrencyProv != nil {
		return l.concurrencyProv.GetCurrentConcurrency(), l.concurrencyProv.GetDesiredConcurrency()
	}
	return 3, 3
}

func (l *Logger) isVerbose() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.verbose
}

func (l *Logger) log(level string, levelVal LogLevel, msg string) {
	if levelVal <= INFO && !l.isVerbose() {
		return
	}
	current, desired := l.getConcurrency()
	gid := getGoroutineID()
	ts := formatTimestamp()
	log.Printf("[%s] %s concurrency: %d/%d goroutine: %d %s", level, ts, current, desired, gid, msg)
}

func (l *Logger) Debug(msg string) {
	l.log("DEBUG", DEBUG, msg)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", DEBUG, fmt.Sprintf(format, args...))
}

func (l *Logger) Info(msg string) {
	l.log("INFO", INFO, msg)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.log("INFO", INFO, fmt.Sprintf(format, args...))
}

func (l *Logger) Warn(msg string) {
	l.log("WARN", WARN, msg)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log("WARN", WARN, fmt.Sprintf(format, args...))
}

func (l *Logger) Error(msg string) {
	l.log("ERROR", ERROR, msg)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", ERROR, fmt.Sprintf(format, args...))
}

func Debug(msg string) {
	defaultLogger.Debug(msg)
}

func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

func Info(msg string) {
	defaultLogger.Info(msg)
}

func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

func Warn(msg string) {
	defaultLogger.Warn(msg)
}

func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

func Error(msg string) {
	defaultLogger.Error(msg)
}

func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

func LogHTTP(metric string, statusCode int, logAll bool) {
	if !logAll && statusCode == 200 {
		return
	}
	msg := metric + " -> " + strconv.Itoa(statusCode)
	defaultLogger.log("INFO", INFO, msg)
}
