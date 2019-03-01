// +build linux

package nvm

import (
	"os"
	"syscall"
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func openFileWithDirectIO(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
}

func lockFileWithExclusiveLock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
