// +build linux

package nvm

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT
func openFileWithDirectIO(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
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

func isDirectIO(val int) bool {
	return (val & syscall.O_DIRECT) != 0
}

func isExclusiveLock(path string) bool {
	cmdBinary := fmt.Sprintf("/usr/bin/bash -c '/usr/bin/flock -e -n %s -c echo'", path)
	parts := strings.Fields(cmdBinary)
	_, err := exec.Command(parts[0], parts[1:]...).Output()
	if err != nil {
		return true
	} else {
		return false
	}

}
