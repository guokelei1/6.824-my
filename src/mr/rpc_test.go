package mr

import (
	"os"
	"testing"
)

func Test(t *testing.T) {
	for i := 0; i < 100; i++ {
		println(os.Getuid())
	}
}
