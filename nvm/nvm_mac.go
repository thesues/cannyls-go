// +build darwin

package nvm

import (
	"fmt"
	"os"
	"syscall"
)

func openFileWithDirectIO(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}

	// Set F_NOCACHE to avoid caching
	// F_NOCACHE    Turns data caching off/on. A non-zero value in arg turns data caching off.  A value
	//              of zero in arg turns data caching on.
	_, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(file.Fd()), syscall.F_NOCACHE, 1)
	if e1 != 0 {
		err = fmt.Errorf("Failed to set F_NOCACHE: %s", e1)
		file.Close()
		file = nil
	}

	return
}

func lockFileWithExclusiveLock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
