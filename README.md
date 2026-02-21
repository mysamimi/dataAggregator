# 🚀 dataAggregator

[![Go Reference](https://pkg.go.dev/badge/github.com/mysamimi/dataAggregator.svg)](https://pkg.go.dev/github.com/mysamimi/dataAggregator)
[![Go Version](https://img.shields.io/github/go-mod/go-version/mysamimi/dataAggregator?color=00add8&logo=go&logoColor=white)](https://go.dev/)
[![Go Tests](https://github.com/mysamimi/dataAggregator/actions/workflows/test.yml/badge.svg)](https://github.com/mysamimi/dataAggregator/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A **high-performance, concurrent data aggregation library** for Go applications. Efficiently aggregate and process high-throughput data streams in memory with minimal lock contention resulting in extremely fast execution.

---

## ✨ Features

- **High Concurrency**: Sharded map structure optimized for parallel, multi-threaded operations.
- **Generic Types**: Leverages standard Go Generics natively. Works with any comparable key (`P`) and value type (`T`).
- **Custom Aggregation Functions**: Highly adaptable. Define exactly how your data should be combined.
- **Periodic Cleanup Anti-Overlap**: Intelligently skips overlapping periodic cleanups under heavy load to prevent CPU/memory exhaustion.
- **Zero Data Loss Guarantee**: Automatically re-inserts data if the output pool is at capacity during cleanup, making it extraordinarily safe for big data flows.
- **Memory Efficient**: Backed by `sync.Pool` safety compatibility. Allocates only what you strictly need.
- **Type Safe**: End-to-end static typing. 

---

## 📦 Installation

Ensure your Go version is `1.26` or later.

```bash
go get github.com/mysamimi/dataAggregator
```

---

## 🛡️ Thread Safety Rules

> [!IMPORTANT]
> The `addFunc` provided to `New` MUST be thread-safe for concurrent access to the same key.
> If multiple goroutines call `Add` with the same key, `addFunc` will be called concurrently.
> Use `sync/atomic` operations (e.g., `atomic.AddUint64`) or proper locking inside your data structure if it contains shared state.

---

## 🚀 Usage Guide

### Basic Walkthrough

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
    // - execution interval: every 5 seconds
    // - connection pool: channel buffer size of 1000 items
    aggregator := dataAggregator.New[string, MetricData](
        ctx,
        5*time.Second,
        1000,
        &logger,
        func(stored, new *MetricData) {
            // Safely combine values using atomic operations
            atomic.AddUint64(stored.Count, *new.Count)
        },
    )
    
    // Push real-time data
    count1, count2 := uint64(5), uint64(10)
    aggregator.Add("api.requests", &MetricData{Name: "api.requests", Count: &count1})
    aggregator.Add("api.requests", &MetricData{Name: "api.requests", Count: &count2})
    
    // Process aggregated data asynchronously
    go func() {
        for data := range aggregator.ChanPool() {
            fmt.Printf("Processed metric: %s, Total: %d\n", data.Name, *data.Count)
        }
    }()
    
    time.Sleep(10 * time.Second) // Simulate application runtime
    
    // Graceful Teardown
    aggregator.Shutdown()
}
```

### Concurrent/Heavy Load Usage

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
    logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    metrics := dataAggregator.New[string, MetricData](
        ctx,
        time.Second*2,
        10000,
        &logger,
        func(stored, new *MetricData) {
            atomic.AddUint64(stored.Count, *new.Count)
        },
    )
    
    go func() {
        for metric := range metrics.ChanPool() {
            fmt.Printf("Processed: %s = %d\n", metric.Name, *metric.Count)
        }
    }()
    
    var wg sync.WaitGroup
    // Launch 10 parallel high-throughput providers
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 1000; j++ {
                value := uint64(1)
                metrics.Add(
                    fmt.Sprintf("metric.%d", j%5),
                    &MetricData{
                        Name:  fmt.Sprintf("metric.%d", j%5),
                        Count: &value,
                    },
                )
                time.Sleep(time.Millisecond) // Simulate calculation
            }
        }(i)
    }
    
    wg.Wait()
    time.Sleep(3 * time.Second)
    metrics.Shutdown()
    fmt.Println("Graceful shutdown completed!")
}
```

---

## 🛠 API Reference

### Initializing a New Aggregator

```go
aggregator := dataAggregator.New[P, T](
    ctx,                // Context for cancellation
    cleanupInterval,    // Interval for extracting active data
    maxPoolSize,        // Max buffer depth for the export channel
    logger,             // Zerolog pointer instance
    addFunc,            // Custom atomic aggregation logic
)
```

Where:
* `P`: Identifier Key type (must be comparable, e.g., string, int, struct)
* `T`: Custom structurally-defined object type.

### Essential Methods

* `Add(key P, data *T) bool` - Insert or update items. Returns `true` if item collided and merged.
* `ChanPool() chan *T` - Retrieves the active export channel for consumer-end handling.
* `Cleanup()` - Trigger a manual early push payload flush.
* `Shutdown()` - Shuts off internals and guarantees all final datasets process thoroughly before shutting down the instance completely.
* `GetItem(key P) *T` - Manual extraction mapping bypass.
* `GetItems() map[P]*T` - Dumps the actively buffered tree context.

---

## 🧠 Behind the Scenes (Implementation)

- **Map Sharding**: Employs mathematically optimal powers-of-two sharding to sidestep RWMutex contention deadlocks entirely.
- **Hashing**: Utilizes highly customized FNV-1a variations specifically scoped for String key acceleration, while retaining bitwise fast-casting for numeric keys.
- **Cache Locality**: Preserves data bounds in memory structurally optimizing internal processor cache.
- **Zero Loss Queue Management**: When outputs peak, safely recycles traffic to inner stores maintaining absolute data integrity.
- **Thread scaling**: Senses core CPU topologies automatically to distribute maximum viable threading allocations.

---

## ⚡ Performance

### Stress Validation
The CI Pipeline continually validates multi-threaded integrity bounds testing **1,000,000 parallel additions** across **1,000 goroutines**. Under such constraints, average compute time falls impressively low (approx `~100ms`).

### Benchmark Matrix
*(Hardware dependent average baseline)*

| Target Process                   | Op Iteration Base | Iteration Span (ns) | Bytes/op    | Memory Allocations/op |
|----------------------------------|-------------------|---------------------|-------------|-----------------------|
| `Parallel-SingleKey_Overload`    | 733         | 1,635,363           | 724,423     | ~40,037               |
| `Parallel-MultiKey_Distribution` | 153         | 7,755,561           | 6,631,846   | ~329,768              |

Run benchmarks locally:
```bash
go test -bench=. -benchmem -run=^$ ./...
```

---

## 📄 License
This library is distributed under the **MIT License**.