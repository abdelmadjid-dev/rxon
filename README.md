# RxOn

RxOn is a semantic DSL wrapper for RxJava 3 that enforces architectural patterns through lazy pipeline orchestration, explicit threading, and strict null safety.

## Core Features
* **Lazy Orchestration**: Pipelines are declarative blueprints; execution is deferred until a terminal operator is invoked.
* **Explicit Scheduler Injection**: All operations explicitly define execution context via `WorkScheduler` (e.g., `Work.callable(WorkScheduler.IO, ...)`).
* **Zero-Nesting Architecture**: Flat API structure for branching, error handling, and asynchronous composition via `thenChain()`, `breakIf()`, and `zipWith()`.
* **Streaming Parity**: The `Observe` class (formerly `Stream`) provides a lazy orchestration engine for streaming data with full functional symmetry to `Work`.
* **First-Class Resilience**: Native support for `retry`, `timeout`, `fallback`, and SAGA-style `compensate` operators.
* **Deep Recovery**: Advanced `recover` operators that traverse cause chains to catch specific errors even when wrapped.
* **Debugging Infrastructure**: Built-in `tag(String)` and `log(LogLevel)` operators with automated lifecycle logging for easier pipeline tracing.

## Installation

### Gradle
```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha4")
}
```

## Documentation

### Changelogs
| Version | Documentation | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha4** | [Release Notes](changelogs/v0.3.0-alpha4.md) | Explicit naming symmetry, lazy Observe parity, and logging infrastructure. |
| **v0.3.0-alpha3** | [Release Notes](changelogs/v0.3.0-alpha3.md) | Semantic flow control, SAGA rollback support, and pipeline recovery. |
| **v0.3.0-alpha2** | [Release Notes](changelogs/v0.3.0-alpha2.md) | Async composition and non-blocking integration operators. |
| **v0.3.0-alpha** | [Release Notes](changelogs/v0.3.0-alpha.md) | Lazy orchestration engine and functional context. |
| **v0.2.2** | [Release Notes](changelogs/v0.2.2.md) | Sync/async chaining disambiguation. |
| **v0.2.1** | [Release Notes](changelogs/v0.2.1.md) | Unified chaining and Throwable support. |
| **v0.2.0** | [Release Notes](changelogs/v0.2.0.md) | Semantic DSL and pipeline API. |

### Migration Guides
| From -> To | Guide | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha3 -> v0.3.0-alpha4** | [Migration Guide](migrations/MIGRATION_0.3.0-alpha3_TO_0.3.0-alpha4.md) | Transitioning to param-based naming and lazy Observe. |
| **v0.3.0-alpha2 -> v0.3.0-alpha3** | [Migration Guide](migrations/MIGRATION_0.3.0-alpha2_TO_0.3.0-alpha3.md) | Adopting semantic breaks and pipeline recovery. |

## Quick Start

### Lazy Pipeline (Explicit Naming)
```java
Work.callable(WorkScheduler.IO, () -> id)
    .thenSingle(WorkScheduler.IO, (ctx, id) -> repository.getUserAsync(id))
    .thenFunction(WorkScheduler.COMPUTE, (ctx, user) -> transform(user))
    .thenAction(WorkScheduler.DATA_WRITE, () -> repository.markSync())
    .tag("UserSync")
    .log(LogLevel.DEBUG)
    .executeOn(WorkScheduler.MAIN, 
        result -> ui.show(result),
        error -> ui.error(error)
    );
```

### Event Orchestration (Observe)
```java
Observe.flow(WorkScheduler.MAIN, uiEvents)
    .debounce(300, TimeUnit.MILLISECONDS)
    .distinct()
    .trigger(
        Work.withContext(() -> computeState())
            .thenFunction(WorkScheduler.IO, (ctx, event) -> sendAnalytics(ctx, event))
    );
```

### Control Flow & Resilience
```java
Work.callable(WorkScheduler.IO, () -> fetchRemoteData())
    .breakIf(data -> data.isEmpty(), "No Data Available")
    .retry(3, 1000, ResiliencePolicy.BackoffStrategy.EXPONENTIAL)
    .recover(IOException.class, err -> fetchLocalCache())
    .thenFunction(WorkScheduler.COMPUTE, (ctx, data) -> process(data))
    .execute();
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
