package main

import (
	"os"
	"path/filepath"
	"syscall"
)

// 使用 flock 系统调用
type FileLock struct {
	file *os.File
	path string
}

func NewFileLock(path string) *FileLock {
	return &FileLock{path: path}
}

func (l *FileLock) Lock() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	l.file = f

	// 使用 flock 锁定文件
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return err
	}
	return nil
}

func (l *FileLock) Unlock() error {
	if l.file != nil {
		err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
		l.file.Close()
		return err
	}
	return nil
}

// GetProgramName 获取程序名（不含路径）
func getProgramName() string {
	// 方法1：尝试使用 os.Executable
	if execPath, err := os.Executable(); err == nil {
		return filepath.Base(execPath)
	}

	// 方法2：回退到 os.Args[0]
	return filepath.Base(os.Args[0])
}
