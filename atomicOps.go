package dataAggregator

import (
	"sync/atomic"
)

// AtomicAdder provides type-specific atomic addition operations
type AtomicAdder[K Number] interface {
	Add(addr *K, delta K)
}

// Uint64Adder implements atomic operations for uint64
type Uint64Adder struct{}

func (a Uint64Adder) Add(addr *uint64, delta uint64) {
	atomic.AddUint64(addr, delta)
}

// Int64Adder implements atomic operations for int64
type Int64Adder struct{}

func (a Int64Adder) Add(addr *int64, delta int64) {
	atomic.AddInt64(addr, delta)
}

// Uint32Adder implements atomic operations for uint32
type Uint32Adder struct{}

func (a Uint32Adder) Add(addr *uint32, delta uint32) {
	atomic.AddUint32(addr, delta)
}

// Int32Adder implements atomic operations for int32
type Int32Adder struct{}

func (a Int32Adder) Add(addr *int32, delta int32) {
	atomic.AddInt32(addr, delta)
}
