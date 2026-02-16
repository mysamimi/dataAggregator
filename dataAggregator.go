package dataAggregator

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type DataAggregator[P comparable, T any] struct {
	shards          []*mapShard[P, T]
	dataPool        chan *T
	ticker          *time.Ticker
	wgCleanup       *sync.WaitGroup
	addFn           func(storData, newData *T)
	logger          *zerolog.Logger
	cleanupInterval time.Duration
	shardMask       uint32
	numShards       uint32
	cancel          context.CancelFunc
	tickWg          sync.WaitGroup
}

// Shard for the map to reduce lock contention
type mapShard[P comparable, T any] struct {
	sync.RWMutex
	items *xsync.Map[P, *T]
}

// New creates an optimized data aggregator with sharded maps for better concurrency
func New[P comparable, T any](
	ctx context.Context,
	cleanupInterval time.Duration,
	maxPoolSize int,
	logger *zerolog.Logger,
	addFunc func(storData, newData *T),
) *DataAggregator[P, T] {
	// Determine optimal number of shards based on CPU cores
	numShards := uint32(runtime.NumCPU())

	// Create sharded maps
	shards := make([]*mapShard[P, T], numShards)
	for i := range shards {
		shards[i] = &mapShard[P, T]{
			items: xsync.NewMap[P, *T](),
		}
	}

	ctx, cancel := context.WithCancel(ctx)

	d := &DataAggregator[P, T]{
		shards:          shards,
		dataPool:        make(chan *T, maxPoolSize),
		ticker:          time.NewTicker(cleanupInterval),
		wgCleanup:       &sync.WaitGroup{},
		addFn:           addFunc,
		logger:          logger,
		cleanupInterval: cleanupInterval,
		shardMask:       numShards - 1, // For fast modulo using bitwise AND
		numShards:       numShards,
		cancel:          cancel,
	}

	d.tickWg.Add(1)
	go func() {
		defer d.tickWg.Done()
		d.tick(ctx)
	}()
	return d
}

// getShard returns the appropriate shard for a key using fast hash computation
func (d *DataAggregator[P, T]) getShard(key P) *mapShard[P, T] {
	var h uint32
	switch k := any(key).(type) {
	case string:
		// FNV-1a hash for strings
		const offsetBasis uint32 = 2166136261
		const prime uint32 = 16777619
		h = offsetBasis
		for i := 0; i < len(k); i++ {
			h ^= uint32(k[i])
			h *= prime
		}
	case int:
		h = uint32(k) // Simple conversion, assumes distribution is okay
	case int32:
		h = uint32(k)
	case int64:
		// XOR folding for int64
		h = uint32(k ^ (k >> 32))
	case uint:
		h = uint32(k)
	case uint32:
		h = k
	case uint64:
		// XOR folding for uint64
		h = uint32(k ^ (k >> 32))
	default:
		// Fallback for other types - this can still be a performance bottleneck
		// Using djb2 hash for the fallback string representation
		keyStr := fmt.Sprintf("%v", key)
		h = 5381 // Initial hash value
		for _, char := range keyStr {
			h = ((h << 5) + h) + uint32(char) // h = h * 33 + c
		}
	}
	return d.shards[h&d.shardMask]
}

func (d *DataAggregator[P, T]) tick(ctx context.Context) {
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

func (d *DataAggregator[P, T]) Cleanup() {
	d.logger.Info().Msg("start cleanup")

	// Process each shard concurrently for faster cleanup
	var wg sync.WaitGroup
	wg.Add(int(d.numShards))

	for i := range d.shards {
		go func(shard *mapShard[P, T]) {
			defer wg.Done()

			// Lock this shard
			shard.Lock()

			// Skip empty shards
			if shard.items.Size() == 0 {
				shard.Unlock()
				return
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
		}(d.shards[i])
	}
	wg.Wait()
}

// Add is an optimized implementation that reduces lock contention
// Add is an optimized implementation that reduces lock contention
func (d *DataAggregator[P, T]) Add(key P, data *T) {
	shard := d.getShard(key)

	// We need a read lock to ensure the map pointer (shard.items) doesn't change
	// while we're using it (e.g. during cleanup)
	shard.RLock()
	defer shard.RUnlock()

	existingVal, loaded := shard.items.LoadOrStore(key, data)

	if loaded {
		d.addFn(existingVal, data)
	}
}

func (d *DataAggregator[P, T]) ChanPool() chan *T {
	return d.dataPool
}

func (d *DataAggregator[P, T]) Shutdown() {
	// Stop ticker first
	d.ticker.Stop()
	d.logger.Info().Msg("tick stopped")

	// Cancel the context to stop the tick loop
	if d.cancel != nil {
		d.cancel()
	}

	// Wait for tick loop to exit
	d.tickWg.Wait()
	d.logger.Info().Msg("tick loop exited")

	// Run final cleanup on each shard
	d.Cleanup()
	d.logger.Info().Msg("finished cleanup")

	// Close channel safely
	close(d.dataPool)
	d.logger.Info().Msg("data pool closed")
}

// GetShardCount returns the number of shards for testing/debugging
func (d *DataAggregator[P, T]) GetShardCount() uint32 {
	return d.numShards
}

// GetSize returns the total count of items across all shards
func (d *DataAggregator[P, T]) GetSize() int {
	total := 0
	for _, shard := range d.shards {
		shard.RLock()
		total += shard.items.Size()
		shard.RUnlock()
	}
	return total
}

func (d *DataAggregator[P, T]) GetItems() map[P]*T {
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

func (d *DataAggregator[P, T]) GetItem(key P) *T {
	shard := d.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	val, found := shard.items.Load(key)
	if found {
		return val
	}
	return nil
}

func (d *DataAggregator[P, T]) GetTicker() *time.Ticker {
	return d.ticker
}
