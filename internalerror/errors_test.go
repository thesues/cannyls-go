package internalerror


import (
	"testing"
	"fmt"
)


func TestErrorCode(t *testing.T) {
	var err error = DeviceBusy;
	fmt.Printf("%s\n", err.Error())
}
