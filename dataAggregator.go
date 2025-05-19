package dataAggregator

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer
}

// Number of shards for the map to reduce lock contention
const defaultShards = 32

type DataAggrigrator[T any, P comparable, K Number] struct {
	shards          []*mapShard[T, P]
	dataPool        chan *T
	ticker          *time.Ticker
	wgCleanup       *sync.WaitGroup
	valFunc         func(data *T) *K
	keyFunc         func(data *T) P
	logger          *zerolog.Logger
	cleanupInterval time.Duration
	shardMask       uint32
	numShards       uint32
	atomicAdder     AtomicAdder[K]
}

// Shard for the map to reduce lock contention
type mapShard[T any, P comparable] struct {
	sync.RWMutex
	items *xsync.Map[P, *T]
}

// New creates an optimized data aggregator with sharded maps for better concurrency
func New[T any, P comparable, K Number](
	ctx context.Context,
	cleanupInterval time.Duration,
	maxPoolSize int,
	logger *zerolog.Logger,
	valFunc func(data *T) *K,
	keyFunc func(data *T) P) *DataAggrigrator[T, P, K] {
	// Determine optimal number of shards based on CPU cores
	numShards := uint32(runtime.NumCPU() * 2)
	if numShards < defaultShards {
		numShards = defaultShards
	}

	// Create sharded maps
	shards := make([]*mapShard[T, P], numShards)
	for i := range shards {
		shards[i] = &mapShard[T, P]{
			items: xsync.NewMap[P, *T](),
		}
	}

	var adder AtomicAdder[K]
	var zero K
	switch any(zero).(type) {
	case uint64:
		adder = any(Uint64Adder{}).(AtomicAdder[K])
	case int64:
		adder = any(Int64Adder{}).(AtomicAdder[K])
	case uint32:
		adder = any(Uint32Adder{}).(AtomicAdder[K])
	case int32:
		adder = any(Int32Adder{}).(AtomicAdder[K])
	default:
		panic(fmt.Sprintf("Unsupported type for atomic operations: %T", zero))
	}

	d := &DataAggrigrator[T, P, K]{
		shards:          shards,
		dataPool:        make(chan *T, maxPoolSize),
		ticker:          time.NewTicker(cleanupInterval),
		wgCleanup:       &sync.WaitGroup{},
		valFunc:         valFunc,
		keyFunc:         keyFunc,
		logger:          logger,
		cleanupInterval: cleanupInterval,
		shardMask:       numShards - 1, // For fast modulo using bitwise AND
		numShards:       numShards,
		atomicAdder:     adder,
	}

	go d.tick(ctx)
	return d
}

// getShard returns the appropriate shard for a key using fast hash computation
func (d *DataAggrigrator[T, P, K]) getShard(key P) *mapShard[T, P] {
	// Simple but effective hash function for shard selection
	keyStr := fmt.Sprintf("%v", key)
	h := uint32(0)
	for i := range len(keyStr) {
		h = h*31 + uint32(keyStr[i])
	}
	return d.shards[h&d.shardMask]
}

func (d *DataAggrigrator[T, P, K]) tick(ctx context.Context) {
	d.logger.Info().Msg("start tick")
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ticker.C:
			d.Cleanup()
		}
	}
}

func (d *DataAggrigrator[T, P, K]) Cleanup() {
	d.logger.Info().Msg("start cleanup")
	d.wgCleanup.Add(int(d.numShards))

	// Process each shard concurrently for faster cleanup
	for i := range d.shards {
		shard := d.shards[i]
		// Lock this shard
		shard.Lock()

		// Skip empty shards
		if shard.items.Size() == 0 {
			shard.Unlock()
			d.wgCleanup.Done()
			continue
		}

		// Create a new map and swap with the old one
		oldItems := shard.items
		shard.items = xsync.NewMap[P, *T]()
		shard.Unlock()

		// Drain the old map into the channel
		count := 0
		oldItems.Range(func(key P, value *T) bool {
			// This will block if channel is full
			d.dataPool <- value
			count++
			return true
		})

		if count > 0 {
			d.logger.Debug().Msgf("cleaned up %d items from shard", count)
		}
		d.wgCleanup.Done()
	}
}

// Add is an optimized implementation that reduces lock contention
func (d *DataAggrigrator[T, P, K]) Add(key P, data *T) {
	// Skip zero values early
	vDelta := d.valFunc(data)
	if *vDelta == 0 {
		return
	}
	if d.keyFunc != nil {
		key = d.keyFunc(data)
	}
	shard := d.getShard(key)

	// Try read lock first for better concurrency
	shard.RLock()
	val, found := shard.items.Load(key)
	shard.RUnlock()

	if found {
		// Existing key - use atomic add without holding locks
		d.atomicAdder.Add(d.valFunc(val), *vDelta)
		return
	}

	// // Key not found with read lock, try again with write lock
	// shard.Lock()
	// val, found = shard.items.Load(key)
	// if found {
	// 	// Someone else added it while we were waiting
	// 	shard.Unlock()
	// 	d.atomicAdder.Add(d.valFunc(val), *vDelta)
	// 	return
	// }

	// Add new entry
	shard.Lock()
	shard.items.Store(key, data)
	shard.Unlock()
}

func (d *DataAggrigrator[T, P, K]) ChanPool() chan *T {
	return d.dataPool
}

func (d *DataAggrigrator[T, P, K]) Shutdown() {
	// Stop ticker first
	d.ticker.Stop()
	d.logger.Info().Msg("tick stopped")

	// Run final cleanup on each shard
	d.Cleanup()

	// Wait for all cleanups to complete
	d.wgCleanup.Wait()
	d.logger.Info().Msg("finished cleanup")

	// Close channel safely
	close(d.dataPool)
	d.logger.Info().Msg("data pool closed")
}

// GetShardCount returns the number of shards for testing/debugging
func (d *DataAggrigrator[T, P, K]) GetShardCount() uint32 {
	return d.numShards
}

// GetSize returns the total count of items across all shards
func (d *DataAggrigrator[T, P, K]) GetSize() int {
	total := 0
	for _, shard := range d.shards {
		shard.RLock()
		total += shard.items.Size()
		shard.RUnlock()
	}
	return total
}

func (d *DataAggrigrator[T, P, K]) GetItems() map[P]*T {
	items := make(map[P]*T, 0)
	for _, shard := range d.shards {
		shard.RLock()
		shard.items.Range(func(key P, value *T) bool {
			items[key] = value
			return true
		})
		shard.RUnlock()
	}
	return items
}

func (d *DataAggrigrator[T, P, K]) GetItem(key P) *T {
	shard := d.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	val, found := shard.items.Load(key)
	if found {
		return val
	}
	return nil
}

func (d *DataAggrigrator[T, P, K]) GetTicker() *time.Ticker {
	return d.ticker
}
