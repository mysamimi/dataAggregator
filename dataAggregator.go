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
	// Determine optimal number of shards (next power of 2 for fast masking)
	numCPUs := runtime.NumCPU()
	numShards := uint32(1)
	for numShards < uint32(numCPUs) {
		numShards <<= 1
	}
	if numShards < 4 {
		numShards = 4 // Minimum shards
	}

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
	case float32:
		h = uint32(k)
	case float64:
		h = uint32(k) ^ uint32(uint64(k)>>32)
	default:
		// Fallback for other types - using a simpler way to get a string if possible
		// or just using a constant to avoid the expensive fmt.Sprintf in tight loops
		// if performance is critical for custom types, they should be added above.
		h = 5381
		keyStr := fmt.Sprint(key)
		for i := 0; i < len(keyStr); i++ {
			h = ((h << 5) + h) + uint32(keyStr[i])
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
			queued := 0
			oldItems.Range(func(key P, value *T) bool {
				select {
				case d.dataPool <- value:
					count++
				default:
					// dataPool is full, re-insert into the active map (this won't lose data)
					d.Add(key, value)
					queued++
					// Optional: Log fewer times or at debug level if spam is an issue
				}
				return true
			})

			if count > 0 || queued > 0 {
				d.logger.Debug().Msgf("cleaned up %d items (re-inserted %d) from shard", count, queued)
			}
		}(d.shards[i])
	}
	wg.Wait()
}

// Add returns true if the item was aggregated (merged) into an existing entry,
// or false if it was newly stored.
func (d *DataAggregator[P, T]) Add(key P, data *T) bool {
	shard := d.getShard(key)

	// We need a read lock to ensure the map pointer (shard.items) doesn't change
	// while we're using it (e.g. during cleanup)
	shard.RLock()
	defer shard.RUnlock()

	var wasLoaded bool
	shard.items.Compute(key, func(oldValue *T, loaded bool) (*T, xsync.ComputeOp) {
		wasLoaded = loaded
		if !loaded {
			return data, xsync.UpdateOp
		}
		d.addFn(oldValue, data)
		return oldValue, xsync.UpdateOp
	})
	return wasLoaded
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
