package dataAggregator

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestData represents sample data for testing
type TestData struct {
	ID    string
	Value *uint64
	V2    uint32
}

// TestKey represents the key for test data
type TestKey string

func TestNew(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test with valid parameters
	agg := New[TestKey, TestData](
		ctx,
		time.Second,
		10,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	assert.NotNil(t, agg)
	assert.NotNil(t, agg.GetItems())
	assert.NotNil(t, agg.ChanPool())
	assert.NotNil(t, agg.GetTicker())
	// assert.Equal(t, time.Second, agg.cleanupInterval)
}

func TestAdd(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Second,
		10,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)
	t.Log("add data1")
	// Test adding a single item
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(TestKey(data1.ID), data1)

	// Verify it was added to the slice
	found := false
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == "test1" {
			found = true
			assert.Equal(t, uint64(5), *value.Value)
		}
	}
	assert.True(t, found, "Data should be found in the slice")

	t.Log("add data2")
	// Use the value from the map for atomic increment
	v2 := uint64(10)
	// Create data with the same memory pointer
	data2 := &TestData{ID: "test1", Value: &v2}
	agg.Add(TestKey(data2.ID), data2) // This should atomically add 10 to the existing value

	// Verify values were aggregated
	found = false
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == "test1" {
			found = true
			assert.Equal(t, uint64(15), *value.Value)
		}
	}
	assert.True(t, found, "Aggregated data should be found in the slice")
}

func TestAddFn(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Second,
		10,
		&logger,
		func(storeData, newData *TestData) {
			atomic.AddUint64(storeData.Value, *newData.Value)
			atomic.AddUint32(&storeData.V2, newData.V2)
		},
	)
	t.Log("add data1")
	// Test adding a single item
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1, V2: uint32(1)}
	agg.Add(TestKey(data1.ID), data1)

	// Verify it was added to the slice
	found := false
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == "test1" {
			found = true
			assert.Equal(t, uint64(5), *value.Value)
			assert.Equal(t, uint32(1), value.V2)
		}
	}
	assert.True(t, found, "Data should be found in the slice")

	t.Log("add data2")
	// Use the value from the map for atomic increment
	v2 := uint64(10)
	// Create data with the same memory pointer
	data2 := &TestData{ID: "test1", Value: &v2, V2: uint32(10)}
	agg.Add(TestKey(data2.ID), data2) // This should atomically add 10 to the existing value

	// Verify values were aggregated
	found = false
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == "test1" {
			found = true
			assert.Equal(t, uint64(15), *value.Value)
			assert.Equal(t, uint32(11), value.V2)
		}
	}
	assert.True(t, found, "Aggregated data should be found in the slice")
}

func TestCleanup(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Millisecond*100,
		10,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	// Add test data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(TestKey(data1.ID), data1)
	v2 := uint64(10)
	data2 := &TestData{ID: "test2", Value: &v2}
	agg.Add(TestKey(data2.ID), data2)

	// Manually trigger cleanup
	agg.Cleanup()

	// Verify the slice is empty after cleanup
	count := 0
	for range agg.GetItems() {
		count++
	}
	assert.Equal(t, 0, count, "Slice should be empty after cleanup")

	// Verify data was moved to the channel
	collectedData := make([]*TestData, 0)
	timeout := time.After(time.Second)
collectLoop:
	for {
		select {
		case data := <-agg.ChanPool():
			if data != nil {
				collectedData = append(collectedData, data)
			}
			if len(collectedData) >= 2 {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	assert.Equal(t, 2, len(collectedData), "Should collect both data items")

	// Verify collected data contains both original items
	foundTest1 := false
	foundTest2 := false
	for _, data := range collectedData {
		if data.ID == "test1" {
			foundTest1 = true
			assert.Equal(t, uint64(5), *data.Value)
		}
		if data.ID == "test2" {
			foundTest2 = true
			assert.Equal(t, uint64(10), *data.Value)
		}
	}
	assert.True(t, foundTest1, "Should find test1 data")
	assert.True(t, foundTest2, "Should find test2 data")
}

type ThreadSafeBuffer struct {
	b strings.Builder
	m sync.Mutex
}

func (b *ThreadSafeBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func (b *ThreadSafeBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}

func TestCleanup_OverlapSkipped(t *testing.T) {
	// Setup logger with a thread-safe string builder to catch the debug skip message
	var buf ThreadSafeBuffer
	logger := zerolog.New(&buf).With().Timestamp().Logger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Millisecond*5, // Fast tick
		100,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	// Manually set the lock to true so the next tick skips
	agg.isCleaning.Store(true)

	// Wait a bit so multiple ticks trigger and hit the "skip" branch
	time.Sleep(time.Millisecond * 30)

	// Revert the lock so Shutdown can cleanly finish
	agg.isCleaning.Store(false)

	// Proper shutdown
	agg.Shutdown()

	logOutput := buf.String()
	assert.Contains(t, logOutput, "cleanup skipped: previous cleanup still in progress", "Should have skipped overlapping cleanups and logged it")
}

func TestCleanup_FullPool(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Millisecond*100,
		1, // Pool size of 1 to easily fill it
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	// Add 3 items (more than pool size)
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(TestKey(data1.ID), data1)

	v2 := uint64(10)
	data2 := &TestData{ID: "test2", Value: &v2}
	agg.Add(TestKey(data2.ID), data2)

	v3 := uint64(15)
	data3 := &TestData{ID: "test3", Value: &v3}
	agg.Add(TestKey(data3.ID), data3)

	// Manually trigger cleanup
	// 1 item will go to the pool, 2 will be re-inserted into the map
	agg.Cleanup()

	// Verify the channel has 1 item
	assert.Equal(t, 1, len(agg.ChanPool()), "Channel should have 1 item")

	// Verify the map has the other 2 items (because they were re-inserted)
	count := 0
	for range agg.GetItems() {
		count++
	}
	assert.Equal(t, 2, count, "Map should have 2 items remaining after full pool cleanup")

	// Consume the 1 item from the channel
	<-agg.ChanPool()

	// Trigger cleanup again, now the remaining 2 should try to go to the pool
	// 1 will go to the pool, 1 will be re-inserted
	agg.Cleanup()

	assert.Equal(t, 1, len(agg.ChanPool()), "Channel should have 1 item again")

	count2 := 0
	for range agg.GetItems() {
		count2++
	}
	assert.Equal(t, 1, count2, "Map should have 1 item remaining after second cleanup")
}

func TestAutomaticCleanup(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator with very short cleanup interval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use very short ticker interval for testing
	agg := New[TestKey, TestData](
		ctx,
		time.Millisecond*200, // Cleanup every 200ms
		10,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	// Override ticker for faster testing
	agg.GetTicker().Reset(time.Millisecond * 50) // Tick every 50ms

	// Add test data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(TestKey(data1.ID), data1)

	// Wait for cleanup to happen automatically
	time.Sleep(time.Millisecond * 100)

	// Verify data was moved to the channel
	select {
	case data := <-agg.ChanPool():
		assert.Equal(t, "test1", data.ID)
		assert.Equal(t, uint64(5), *data.Value)
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for data in channel")
	}
}

func TestParallelAdd(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Second,
		1000, // Larger pool for parallel test
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
			atomic.AddUint32(&old.V2, new.V2)
		},
	)

	// Test parallel additions
	const numWorkers = 100
	const incrementsPerWorker = 1000
	const testKey = "concurrent-test"

	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	// First add the initial data
	v0 := uint64(0)
	initialData := &TestData{ID: testKey, Value: &v0, V2: uint32(0)}
	agg.Add(TestKey(initialData.ID), initialData)

	// Now have multiple goroutines update it
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				v1 := uint64(1)
				increment := &TestData{ID: testKey, Value: &v1, V2: uint32(1)}
				agg.Add(TestKey(increment.ID), increment)
			}
		}()
	}

	wg.Wait()
	// Verify total value
	var totalValue uint64
	var totalV2 uint32
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == testKey {
			totalValue += *value.Value
			totalV2 += value.V2
		}
	}
	expectedTotal := uint64(numWorkers * incrementsPerWorker)
	assert.Equal(t, expectedTotal, totalValue, "Total should match expected parallel increments")
	assert.Equal(t, uint32(expectedTotal), totalV2, "Total should match expected parallel increments")
}

func TestShutdown(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestKey, TestData](
		ctx,
		time.Second,
		10,
		&logger,
		func(old, new *TestData) {
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	// Wait for ticker to start
	time.Sleep(time.Millisecond * 100)

	// Add data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(TestKey(data1.ID), data1)

	// Give data time to be processed
	time.Sleep(time.Millisecond * 100)

	// Shutdown (this should now properly clean up and transfer data)
	agg.Shutdown()

	// Verify data was moved to the channel before it closed
	select {
	case data, ok := <-agg.ChanPool():
		if !ok {
			t.Fatal("Channel closed too early")
		}
		assert.Equal(t, "test1", data.ID)
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for data in channel")
	}

	// Verify channel is closed after all data is processed
	_, ok := <-agg.ChanPool()
	assert.False(t, ok, "Channel should be closed after shutdown")
}

func BenchmarkParallelAdd(b *testing.B) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)

	// Reset the benchmark timer before the actual work
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer() // Stop timer during setup

		// Create aggregator for each iteration
		ctx, cancel := context.WithCancel(context.Background())
		agg := New[TestKey, TestData](
			ctx,
			time.Hour, // Long interval to prevent automatic cleanup during benchmark
			100000,    // Large pool to prevent blocking
			&logger,
			func(old, new *TestData) {
				atomic.AddUint64(old.Value, *new.Value)
			},
		)

		// First add the initial data
		v0 := uint64(0)
		initialData := &TestData{ID: "benchmark-test", Value: &v0}
		agg.Add(TestKey(initialData.ID), initialData)

		// Configure workers based on available CPUs
		numWorkers := runtime.NumCPU()
		incrementsPerWorker := 10000 / numWorkers // Distribute load
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		b.StartTimer() // Resume timer for the actual benchmark

		// Launch workers
		for w := 0; w < numWorkers; w++ {
			go func() {
				defer wg.Done()
				for j := 0; j < incrementsPerWorker; j++ {
					v1 := uint64(1)
					increment := &TestData{ID: "benchmark-test", Value: &v1}
					agg.Add(TestKey(increment.ID), increment)
				}
			}()
		}

		// Wait for all goroutines to finish
		wg.Wait()

		b.StopTimer() // Stop timer for cleanup
		cancel()      // Clean up context
	}
}

// Benchmark variations with different concurrency patterns
func BenchmarkParallelAddMultipleKeys(b *testing.B) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.ErrorLevel)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		ctx, cancel := context.WithCancel(context.Background())
		agg := New[TestKey, TestData](
			ctx,
			time.Hour,
			100000,
			&logger,
			func(old, new *TestData) {
				atomic.AddUint64(old.Value, *new.Value)
			},
		)

		numWorkers := runtime.NumCPU()
		keysPerWorker := 100
		incrementsPerKey := 100
		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		b.StartTimer()

		// Launch workers - each adding to multiple keys
		for w := 0; w < numWorkers; w++ {
			workerID := w
			go func() {
				defer wg.Done()
				for k := 0; k < keysPerWorker; k++ {
					keyID := fmt.Sprintf("benchmark-key-%d-%d", workerID, k)

					// Initialize the key
					v0 := uint64(0)
					initialData := &TestData{ID: keyID, Value: &v0}
					agg.Add(TestKey(initialData.ID), initialData)

					// Increment the key many times
					for j := 0; j < incrementsPerKey; j++ {
						*initialData.Value = 1
						agg.Add(TestKey(initialData.ID), initialData)
					}
				}
			}()
		}

		wg.Wait()
		b.StopTimer()
		cancel()
	}
}

func TestConcurrency_Add_vs_Cleanup(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator with short cleanup interval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[string, int64](
		ctx,
		time.Millisecond*10, // Fast cleanup
		1000,
		&logger,
		func(old, new *int64) {
			atomic.AddInt64(old, *new)
		},
	)

	// Run for a fixed duration
	duration := time.Second * 2
	done := make(chan struct{})
	time.AfterFunc(duration, func() {
		close(done)
	})

	var wg sync.WaitGroup

	// Writer goroutines
	numWriters := 10
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					val := int64(1)
					key := fmt.Sprintf("key-%d", id%5) // Shared keys
					agg.Add(key, &val)
					// Small sleep to allow context switches
					if id%2 == 0 {
						runtime.Gosched()
					}
				}
			}
		}(i)
	}

	// Cleaner goroutine (in addition to the automatic one)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				// Manually trigger cleanup frequently to stress test locking
				agg.Cleanup()
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()

	// Consumer goroutine to empty the channel so it doesn't block
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				// Drain remaining
				for {
					select {
					case <-agg.ChanPool():
					default:
						return
					}
				}
			case <-agg.ChanPool():
				// consume
			}
		}
	}()

	wg.Wait()
	agg.Shutdown()
}

func TestHighConcurrency(t *testing.T) {
	// Setup logger to discard to not slow down the test too much
	logger := zerolog.Nop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// High concurrency parameters
	const numWriters = 1000     // Total number of writer goroutines
	const itemsPerWriter = 1000 // Items added by each writer
	const numKeys = 500         // Number of unique keys
	const cleanupInterval = 50 * time.Millisecond
	const poolSize = 10000

	// Track expected totals
	var expectedGrandTotal uint64 = uint64(numWriters * itemsPerWriter)
	keyTotals := make([]*uint64, numKeys)
	for i := range keyTotals {
		var v uint64
		keyTotals[i] = &v
	}

	agg := New[string, uint64](
		ctx,
		cleanupInterval,
		poolSize,
		&logger,
		func(old, new *uint64) {
			atomic.AddUint64(old, *new)
		},
	)

	var wg sync.WaitGroup
	wg.Add(numWriters)

	start := time.Now()

	// Launch writers
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWriter; j++ {
				keyIndex := (writerID + j) % numKeys
				key := fmt.Sprintf("key-%d", keyIndex)
				val := uint64(1)

				agg.Add(key, &val)
				atomic.AddUint64(keyTotals[keyIndex], 1)

				// Occasionally yield
				if j%100 == 0 {
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Launch consumer
	var actualGrandTotal uint64
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for val := range agg.ChanPool() {
			atomic.AddUint64(&actualGrandTotal, *val)
		}
	}()

	// Wait for writers to finish
	wg.Wait()
	duration := time.Since(start)
	t.Logf("Writers finished in %v. Total items: %d", duration, expectedGrandTotal)

	// Shutdown aggregator to trigger final cleanup and close channel
	agg.Shutdown()

	// Wait for consumer to finish processing the channel
	<-consumerDone

	// Final verification
	assert.Equal(t, expectedGrandTotal, actualGrandTotal, "Grand total should match")
}

func TestWithSyncPool_And_Requeue(t *testing.T) {
	// Setup logger
	logger := zerolog.Nop()

	// Create a context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a sync.Pool for TestData
	pool := &sync.Pool{
		New: func() any {
			v := uint64(0)
			return &TestData{Value: &v}
		},
	}

	// 2. Create the aggregator with a very small channel pool (e.g., 5)
	// This ensures that `dataPool` fills up quickly, forcing the `default`
	// case in Cleanup() to re-insert the item back into the map.
	agg := New[string, TestData](
		ctx,
		time.Millisecond*50, // Fast cleanup interval
		5,                   // Very small channel size to guarantee full channel & requeuing
		&logger,
		func(old, new *TestData) {
			// Aggregate the data
			atomic.AddUint64(old.Value, *new.Value)
		},
	)

	var wg sync.WaitGroup
	numWriters := 100
	itemsPerWriter := 1000

	start := time.Now()

	// Launch writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWriter; j++ {
				// Rent from sync.Pool
				item := pool.Get().(*TestData)

				// Prepare the item's state
				key := fmt.Sprintf("key-%d", j%20) // 20 distinct keys -> high aggregation chance
				item.ID = key
				*item.Value = 1 // Add 1 count for this event

				// Add to aggregator
				merged := agg.Add(key, item)

				// If it was merged into an existing map record, we don't need this item anymore
				if merged {
					pool.Put(item)
				}

				// Occasionally yield to increase contention
				if j%50 == 0 {
					runtime.Gosched()
				}
			}
		}(i)
	}

	var actualTotal uint64
	consumerDone := make(chan struct{})

	// Launch consumer
	go func() {
		defer close(consumerDone)
		// Read aggregated items from the channel
		for item := range agg.ChanPool() {
			// Tally up the total count to ensure Zero Data Loss
			atomic.AddUint64(&actualTotal, *item.Value)

			// Return the consumed item back to the sync.Pool
			pool.Put(item)
		}
	}()

	// Wait for all writers to finish their loops
	wg.Wait()
	t.Logf("Writers finished in %v", time.Since(start))

	// Shutdown will perform one final cleanup and close ChanPool
	agg.Shutdown()

	// Wait for consumer to finish reading the remaining items
	<-consumerDone

	// Assertions
	expectedTotal := uint64(numWriters * itemsPerWriter)
	assert.Equal(t, expectedTotal, actualTotal, "Total aggregated values must perfectly match all inserted counts despite re-queuing and sync.Pool usage")
}
