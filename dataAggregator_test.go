package dataAggregator

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestData represents sample data for testing
type TestData struct {
	ID    string
	Value *uint64
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
	agg := New[TestData, TestKey](
		ctx,
		time.Second,
		10,
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
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

	agg := New[TestData, TestKey](
		ctx,
		time.Second,
		10,
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
		},
	)
	t.Log("add data1")
	// Test adding a single item
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(data1, "")

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
	agg.Add(data2, "") // This should atomically add 10 to the existing value

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

	// Test adding an item with value 0 should be ignored
	v3 := uint64(0)
	data3 := &TestData{ID: "test2", Value: &v3}
	agg.Add(data3, "")

	// Verify it was not added
	found = false
	for key, _ := range agg.GetItems() {
		if string(key) == "test2" {
			found = true
		}
	}
	assert.False(t, found, "Zero-value data should not be added to the slice")
}

func TestCleanup(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestData, TestKey](
		ctx,
		time.Millisecond*100,
		10,
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
		},
	)

	// Add test data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(data1, "")
	v2 := uint64(10)
	data2 := &TestData{ID: "test2", Value: &v2}
	agg.Add(data2, "")

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

func TestAutomaticCleanup(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator with very short cleanup interval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use very short ticker interval for testing
	agg := New[TestData, TestKey](
		ctx,
		time.Millisecond*200, // Cleanup every 200ms
		10,
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
		},
	)

	// Override ticker for faster testing
	agg.GetTicker().Reset(time.Millisecond * 50) // Tick every 50ms

	// Add test data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(data1, "")

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

	agg := New[TestData, TestKey](
		ctx,
		time.Second,
		1000, // Larger pool for parallel test
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
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
	initialData := &TestData{ID: testKey, Value: &v0}
	agg.Add(initialData, "")

	// Now have multiple goroutines update it
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				v1 := uint64(1)
				increment := &TestData{ID: testKey, Value: &v1}
				agg.Add(increment, "")
			}
		}()
	}

	wg.Wait()
	// Verify total value
	var totalValue uint64
	for key, value := range agg.GetItems() {
		t.Log("key:", key, "value:", *value.Value)
		if string(key) == testKey {
			totalValue += *value.Value
		}
	}
	expectedTotal := uint64(numWorkers * incrementsPerWorker)
	assert.Equal(t, expectedTotal, totalValue, "Total should match expected parallel increments")
}

func TestShutdown(t *testing.T) {
	// Setup logger
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Create aggregator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agg := New[TestData, TestKey](
		ctx,
		time.Second,
		10,
		&logger,
		func(data *TestData) *uint64 { return data.Value },
		func(data *TestData) TestKey {
			key := TestKey(data.ID)
			return key
		},
	)

	// Wait for ticker to start
	time.Sleep(time.Millisecond * 100)

	// Add data
	v1 := uint64(5)
	data1 := &TestData{ID: "test1", Value: &v1}
	agg.Add(data1, "")

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
		agg := New[TestData, TestKey](
			ctx,
			time.Hour, // Long interval to prevent automatic cleanup during benchmark
			100000,    // Large pool to prevent blocking
			&logger,
			func(data *TestData) *uint64 { return data.Value },
			func(data *TestData) TestKey {
				key := TestKey(data.ID)
				return key
			},
		)

		// First add the initial data
		v0 := uint64(0)
		initialData := &TestData{ID: "benchmark-test", Value: &v0}
		agg.Add(initialData, "")

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
					agg.Add(increment, "")
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
		agg := New[TestData, TestKey](
			ctx,
			time.Hour,
			100000,
			&logger,
			func(data *TestData) *uint64 { return data.Value },
			func(data *TestData) TestKey {
				key := TestKey(data.ID)
				return key
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
					agg.Add(initialData, "")

					// Increment the key many times
					for j := 0; j < incrementsPerKey; j++ {
						*initialData.Value = 1
						agg.Add(initialData, "")
					}
				}
			}()
		}

		wg.Wait()
		b.StopTimer()
		cancel()
	}
}
