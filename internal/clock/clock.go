package clock

import "time"

// NowFunc returns current time. Override in tests for determinism.
var NowFunc = time.Now

// Now is a thin wrapper around NowFunc.
func Now() time.Time { return NowFunc() }
