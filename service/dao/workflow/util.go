package workflow

import (
	"fmt"
	"sync/atomic"
)

var counter int32

func generateAnonymousName() string {
	return fmt.Sprintf("anonymous-%d", atomic.AddInt32(&counter, 1))
}
