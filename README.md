# RxOn

RxOn is a semantic DSL wrapper for RxJava 3 that enforces architectural patterns through lazy pipeline orchestration, explicit threading, and strict null safety.

## Core Features
* **Lazy Orchestration**: Pipelines are declarative blueprints; execution is deferred until a terminal operator is invoked.
* **Explicit Scheduler Injection**: All operations explicitly define execution context via `WorkScheduler` for fine-grained, predictable concurrency.
* **Explicit State Propagation**: Flexible `BiFunction` merger overloads on chaining operators allow clean state-awareness and value retention without complex implicit context tracing.
* **Zero-Nesting Architecture**: Flat API structure for branching, error handling, and parallel composition via `thenBranch()`, `recover()`, and `zipWith()`.
* **Streaming Parity**: The `Observe` class provides a lazy orchestration engine for streaming data with full functional symmetry to `Work`.
* **Segment Resilience**: Native support for `resilience()` blocks that apply retry, timeout, fallback, and compensation policies collectively to entire pipeline segments.
* **Stacktrace Cleaning**: Built-in exception stacktrace filtering to strip out reflection, lambda wrappers, and library overhead from diagnostic reports.
* **Debugging Infrastructure**: Declarative `tag(String)` and `log(LogLevel)` operators with automated lifecycle stage logging.

## Installation

### Gradle
```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha5")
}
```

## Documentation

### Changelogs
| Version | Documentation | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha5** | [Release Notes](changelogs/v0.3.0-alpha5.md) | Single-Generic Simplification, Smart Branching, Segment Resilience, and Composition. |
| **v0.3.0-alpha4** | [Release Notes](changelogs/v0.3.0-alpha4.md) | Explicit naming symmetry, lazy Observe parity, and logging infrastructure. |
| **v0.3.0-alpha3** | [Release Notes](changelogs/v0.3.0-alpha3.md) | Semantic flow control, SAGA rollback support, and pipeline recovery. |

## Quick Start

### Explicit Architecture & Concurrency
```java
Work.callable(WorkScheduler.IO, () -> repository.getUserId())
    .thenSingle(WorkScheduler.IO, id -> repository.getUserAsync(id))
    .thenFunction(WorkScheduler.COMPUTE, user -> transform(user))
    .thenAction(WorkScheduler.DATA_WRITE, () -> repository.markSync())
    .tag("UserSync")
    .execute();
```

### State Merging & Branching
```java
Work.callable(WorkScheduler.IO, () -> api.fetchProduct())
    // Retain state (Product) through a side-effect branch
    .thenBranch(
        product -> product.isDigital(),
        Work.action(WorkScheduler.IO, () -> sendEmail()),
        Work.action(WorkScheduler.IO, () -> scheduleShipping())
    )
    .execute();
```

### Segment Resilience
```java
Work.callable(WorkScheduler.IO, () -> api.fetchPart1())
    .thenSingle(WorkScheduler.IO, part1 -> api.fetchPart2(part1))
    // Apply policies collectively to all preceding stages in this block
    .resilience(policy -> policy
        .retry(3, 100, ResiliencePolicy.BackoffStrategy.LINEAR)
        .timeout(10, TimeUnit.SECONDS)
        .fallback(Work.callable(WorkScheduler.COMPUTE, () -> FallbackData.INSTANCE))
    )
    .execute();
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
