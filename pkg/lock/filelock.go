package lock

import (
	"fmt"
	"os"
	"syscall"
)

type FileLock struct {
	file *os.File
	path string
}

func TryLock(path string) (*FileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		pid := readPidFromFile(file)
		file.Close()
		if pid > 0 {
			return nil, fmt.Errorf("lock held by process %d", pid)
		}
		return nil, fmt.Errorf("lock held by another process")
	}

	pidStr := fmt.Sprintf("%d\n", os.Getpid())
	if err := file.Truncate(0); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to truncate lock file: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to seek lock file: %w", err)
	}
	if _, err := file.WriteString(pidStr); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to write pid to lock file: %w", err)
	}

	return &FileLock{file: file, path: path}, nil
}

func (fl *FileLock) Unlock() {
	if fl.file != nil {
		syscall.Flock(int(fl.file.Fd()), syscall.LOCK_UN)
		fl.file.Close()
		fl.file = nil
	}
}

func (fl *FileLock) Path() string {
	return fl.path
}

func readPidFromFile(file *os.File) int {
	file.Seek(0, 0)
	var pid int
	_, err := fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return 0
	}
	return pid
}
