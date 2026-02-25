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
	getColor        func() bool
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

func SetColorGetter(fn func() bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.getColor = fn
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
	return time.Now().Format("15:04:05")
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
	ts := formatTimestamp()
	if l.useColor() {
		const darkGray = "\x1b[90m"
		const reset = "\x1b[0m"
		if l.hasConcurrencyProvider() {
			current, desired := l.getConcurrency()
			gid := getGoroutineID()
			log.Printf("%s[%s] %s conc: %d/%d grid: #%d %s%s", darkGray, level, ts, current, desired, gid, msg, reset)
		} else {
			log.Printf("%s[%s] %s %s%s", darkGray, level, ts, msg, reset)
		}
	} else {
		if l.hasConcurrencyProvider() {
			current, desired := l.getConcurrency()
			gid := getGoroutineID()
			log.Printf("[%s] %s conc: %d/%d grid: #%d %s", level, ts, current, desired, gid, msg)
		} else {
			log.Printf("[%s] %s %s", level, ts, msg)
		}
	}
}

func (l *Logger) useColor() bool {
	l.mu.RLock()
	fn := l.getColor
	l.mu.RUnlock()
	if fn == nil {
		return false
	}
	return fn()
}

func (l *Logger) hasConcurrencyProvider() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.concurrencyProv != nil
}

func (l *Logger) Debug(msg string) {
	l.log("DBUG", DEBUG, msg)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log("DBUG", DEBUG, fmt.Sprintf(format, args...))
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
	l.log("ERRO", ERROR, msg)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log("ERRO", ERROR, fmt.Sprintf(format, args...))
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
	msg := metric + " -> HTTP " + strconv.Itoa(statusCode)
	defaultLogger.log("INFO", INFO, msg)
}

func LogMetricsArrival(clusterID, metric string, start, end, statusCode int, bytesWritten int64, logAll bool) {
	if !logAll && statusCode == 200 && bytesWritten > 0 {
		return
	}
	durationStr := ""
	if start > 0 && end > 0 && end > start {
		durationStr = formatDurationShort(end - start)
	}

	bytesStr := formatBytes(bytesWritten)
	if bytesWritten == 0 {
		bytesStr = "empty"
	}

	var msg string
	if clusterID != "" && durationStr != "" {
		msg = fmt.Sprintf("%s %s [%s] %s -> HTTP %d", clusterID, metric, durationStr, bytesStr, statusCode)
	} else if clusterID != "" {
		msg = fmt.Sprintf("%s %s %s -> HTTP %d", clusterID, metric, bytesStr, statusCode)
	} else if durationStr != "" {
		msg = fmt.Sprintf("%s [%s] %s -> HTTP %d", metric, durationStr, bytesStr, statusCode)
	} else {
		msg = fmt.Sprintf("%s %s -> HTTP %d", metric, bytesStr, statusCode)
	}
	defaultLogger.logForce("INFO", INFO, msg)
}

func (l *Logger) logForce(level string, levelVal LogLevel, msg string) {
	ts := formatTimestamp()
	if l.useColor() {
		const darkGray = "\x1b[90m"
		const reset = "\x1b[0m"
		if l.hasConcurrencyProvider() {
			current, desired := l.getConcurrency()
			gid := getGoroutineID()
			log.Printf("%s[%s] %s conc: %d/%d grid: #%d %s%s", darkGray, level, ts, current, desired, gid, msg, reset)
		} else {
			log.Printf("%s[%s] %s %s%s", darkGray, level, ts, msg, reset)
		}
	} else {
		if l.hasConcurrencyProvider() {
			current, desired := l.getConcurrency()
			gid := getGoroutineID()
			log.Printf("[%s] %s conc: %d/%d grid: #%d %s", level, ts, current, desired, gid, msg)
		} else {
			log.Printf("[%s] %s %s", level, ts, msg)
		}
	}
}

func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(bytes)/1024)
	} else {
		return fmt.Sprintf("%.1fMB", float64(bytes)/(1024*1024))
	}
}

func formatDurationShort(seconds int) string {
	if seconds < 60 {
		return "<1m"
	}
	minutes := seconds / 60
	if minutes < 60 {
		return strconv.Itoa(minutes) + "m"
	}
	hours := minutes / 60
	if hours < 24 {
		remainingMinutes := minutes % 60
		if remainingMinutes == 0 {
			return strconv.Itoa(hours) + "h"
		}
		return strconv.Itoa(hours) + "h" + strconv.Itoa(remainingMinutes) + "m"
	}
	days := hours / 24
	remainingHours := hours % 24
	if remainingHours == 0 {
		return strconv.Itoa(days) + "d"
	}
	return strconv.Itoa(days) + "d" + strconv.Itoa(remainingHours) + "h"
}
