# dataAggregator


A high-performance, concurrent data aggregation library for Go applications. Efficiently aggregate and process data in memory with minimal lock contention.

## Features

- **High Concurrency**: Sharded map design optimized for parallel operations
- **Generic Types**: Works with any comparable key and value types
- **Custom Aggregation Functions**: Define how your data should be combined, must use atomic function
- **Periodic Cleanup**: Configurable intervals for processing aggregated data
- **Memory Efficient**: Only allocates what you need
- **Type Safe**: Fully leverages Go generics

## Installation

```bash
go get github.com/mysamimi/dataAggregator
```

## Usage
Basic Example

```go
package main

import (
    "context"
    "fmt"
    "sync/atomic"
    "time"

    "github.com/mysamimi/dataAggregator"
    "github.com/rs/zerolog"
)

// Define your data structure
type MetricData struct {
    Name  string
    Count *uint64
}

func main() {
    // Setup logger
    logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
    
    // Create context with cancellation
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Create the aggregator
    // - cleanup every 5 seconds
    // - channel buffer size of 1000 items
    aggregator := dataAggregator.New[string, MetricData](
        ctx,
        5*time.Second,
        1000,
        &logger,
        func(stored, new *MetricData) {
            // Combine values using atomic operations
            atomic.AddUint64(stored.Count, *new.Count)
        },
    )
    
    // Add data
    count1 := uint64(5)
    aggregator.Add("api.requests", &MetricData{Name: "api.requests", Count: &count1})
    
    count2 := uint64(10)
    aggregator.Add("api.requests", &MetricData{Name: "api.requests", Count: &count2})
    
    // Start a goroutine to process aggregated data
    go func() {
        for data := range aggregator.ChanPool() {
            fmt.Printf("Processed metric: %s, count: %d\n", data.Name, *data.Count)
        }
    }()
    
    // Run for a while to demonstrate periodic cleanup
    time.Sleep(10 * time.Second)
    
    // Shutdown properly when done
    aggregator.Shutdown()
}
```
Concurrent Usage Example
```go
package main

import (
    "context"
    "fmt"
    "sync"
    "sync/atomic"
    "time"

    "github.com/mysamimi/dataAggregator"
    "github.com/rs/zerolog"
)

type MetricData struct {
    Name  string
    Count *uint64
}

func main() {
    // Setup
    logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Create aggregator with custom aggregation function
    metrics := dataAggregator.New[string, MetricData](
        ctx,
        time.Second*2,
        10000,
        &logger,
        func(stored, new *MetricData) {
            atomic.AddUint64(stored.Count, *new.Count)
        },
    )
    
    // Process aggregated metrics
    go processMetrics(metrics)
    
    // Simulate high-volume concurrent updates
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 1000; j++ {
                value := uint64(1)
                metrics.Add(
                    fmt.Sprintf("metric.%d", j%5), // Key
                    &MetricData{
                        Name:  fmt.Sprintf("metric.%d", j%5),
                        Count: &value,
                    },
                )
                
                // Simulate work
                time.Sleep(time.Millisecond)
            }
        }(i)
    }
    
    wg.Wait()
    time.Sleep(3 * time.Second) // Allow final cleanup to occur
    metrics.Shutdown()
    
    fmt.Println("All done!")
}

func processMetrics(metrics *dataAggregator.DataAggrigrator[string, MetricData]) {
    for metric := range metrics.ChanPool() {
        fmt.Printf("Processed: %s = %d\n", metric.Name, *metric.Count)
    }
}
```

## API

Creating a New Aggregator

```go
aggregator := dataAggregator.New[P, T](
    ctx,                // Context for cancellation
    cleanupInterval,    // How often to move data to channel
    maxPoolSize,        // Buffer size for the channel
    logger,             // Zerolog logger
    addFunc,            // Function to combine stored and new data
)
```
Where:

* ```P```: Key type (must be comparable)
* ```T```: Your data type

Key Methods

* ```Add(key P, data *T)``` - Add or update data in the aggregator
* ```ChanPool() chan *T``` - Get the channel for receiving aggregated data
* ```Cleanup()``` - Manually trigger cleanup
* ```Shutdown()``` - Properly stop the aggregator and clean up resources
* ```GetItem(key P) *T``` - Retrieve a specific item by key
* ```GetItems() map[P]*T``` - Get all items currently in the aggregator

## Implementation Details

* Uses sharded maps to minimize lock contention
* Optimizes for CPU cache locality
* Implements atomic operations for various numeric types (uint64, int64, uint32, int32)
* Scales with available CPU cores
* Customizable aggregation logic through user-defined functions


## License
MIT License