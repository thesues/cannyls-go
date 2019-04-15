package internalerror

import (
	"errors"
)

var (
	DeviceBusy         = errors.New("Device is busy")
	DeviceTerminated   = errors.New("Device is terminated")
	StorageFull        = errors.New("Storage is full")
	JournalStorageFull = errors.New("Journal is full")
	StorageCorrupted   = errors.New("Storage is corrupted")
	InvalidInput       = errors.New("Invalid input")
	InconsistentState  = errors.New("Inconsistent state")
	Other              = errors.New("Unknow error")
	NoEntries          = errors.New("NoEntries")
)
