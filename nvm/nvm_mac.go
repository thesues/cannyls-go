// +build darwin

package nvm

import (
	"fmt"
	"os"
	"syscall"
)

var _ = fmt.Printf

/*
* On mac system, F_NOCACHE is just a hint, it will not reject non-alignned data
* So it is required for the NVM layer to check every mem is aligned
* https://forums.developer.apple.com/thread/25464
 */
func openFileWithDirectIO(name string, flag int, perm os.FileMode) (file *os.File, err error) {

	file, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}

	// Set F_NOCACHE to avoid caching
	// F_NOCACHE    Turns data caching off/on. A non-zero value in arg turns data caching off.  A value
	//              of zero in arg turns data caching on.
	//TODO
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

// copy-paste from src/pkg/syscall/zsyscall_linux_amd64.go
func fcntl(fd int, cmd int, arg int) (val int, err error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
	val = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}

/*
The following constant comes from
https://github.com/apple/darwin-xnu/blob/master/bsd/sys/fcntl.h#L162
*/
func isDirectIO(val int) bool {
	return (val & 0x40000) != 0
}

/*
The following constant comes from
https://github.com/apple/darwin-xnu/blob/master/bsd/sys/fcntl.h#L133
*/
func isExclusiveLock(path string, val int) bool {
	return (val & 0x4000) != 0
}

func fallocate(file *os.File, preallocate int64){
	//not implemented!!
}
