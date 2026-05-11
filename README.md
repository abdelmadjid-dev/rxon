# RxOn

RxOn is a semantic DSL wrapper for RxJava 3 that enforces architectural patterns through lazy pipeline orchestration, explicit threading, and strict null safety.

## Core Features
* **Lazy Orchestration**: Pipelines are declarative blueprints; execution is deferred until a terminal operator is invoked.
* **Thread Isolation**: Operations explicitly define execution context via `WorkScheduler` (e.g., `.thenIo()`, `.thenCompute()`).
* **Zero-Nesting Architecture**: Flat API structure for branching, error handling, and asynchronous composition via `thenChain()` and `then()`.
* **Async Integration**: First-class support for `Single`-returning tasks across all schedulers using `then<Stage>Single()`.
* **Semantic Side-Effects**: Overloaded `peek<Stage>(Consumer<T>)` operators for all schedulers to define side-effects without context ceremony.
* **Zero Nullability**: immediate failure upon null emission in any stage of the pipeline.
* **Functional Context**: Typed context propagation between isolated pipeline stages.
* **Flow Control & SAGA**: Semantic pipeline termination (`breakWork`), recovery (`recoverBreak`), and reliable LIFO compensations (`compensate`) for transactional integrity.

## Installation

### Gradle
```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha3")
}
```

## Documentation

### Changelogs
| Version | Documentation | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha3** | [Release Notes](changelogs/v0.3.0-alpha3.md) | Semantic flow control, SAGA rollback support, and pipeline recovery. |
| **v0.3.0-alpha2** | [Release Notes](changelogs/v0.3.0-alpha2.md) | Async composition and non-blocking integration operators. |
| **v0.3.0-alpha** | [Release Notes](changelogs/v0.3.0-alpha.md) | Lazy orchestration engine and functional context. |
| **v0.2.2** | [Release Notes](changelogs/v0.2.2.md) | Sync/async chaining disambiguation. |
| **v0.2.1** | [Release Notes](changelogs/v0.2.1.md) | Unified chaining and Throwable support. |
| **v0.2.0** | [Release Notes](changelogs/v0.2.0.md) | Semantic DSL and pipeline API. |

### Migration Guides
| From -> To | Guide | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha2 -> v0.3.0-alpha3** | [Migration Guide](migrations/MIGRATION_0.3.0-alpha2_TO_0.3.0-alpha3.md) | Adopting semantic breaks and pipeline recovery. |
| **v0.3.0-alpha -> v0.3.0-alpha2** | [Migration Guide](migrations/MIGRATION_0.3.0-alpha_TO_0.3.0-alpha2.md) | Adopting non-blocking async and pipeline composition. |
| **v0.2.2 -> v0.3.0-alpha** | [Migration Guide](migrations/MIGRATION_0.2.2_TO_0.3.0-alpha.md) | Lazy pipelines and API refactoring. |

## Initialization

### Android (Automatic)
Register `RxOnInitializer` in `AndroidManifest.xml`.

### Manual
```java
RxOnConfig.builder()
    .debug(DEBUG)
    .logger(new MyLogger())
    .errorMapper(new MyMapper())
    .init();
```

## Quick Start

### Lazy Pipeline
```java
Work.io(() -> id)
    .thenIoSingle(id -> repository.getUserAsync(id)) // Returns Single<User>
    .thenCompute(user -> transform(user))
    .thenWrite(user -> repository.save(user))
    .executeOn(WorkScheduler.MAIN, 
        result -> ui.show(result),
        error -> ui.error(error)
    );
```

### Event Orchestration
```java
Observe.flow(uiEvents)
    .debounce(300, TimeUnit.MILLISECONDS)
    .trigger(
        Work.withContext(() -> computeState())
            .thenIo((ctx, event) -> sendAnalytics(ctx, event))
    );
```

### Resilience & Compensation
```java
Work.io(() -> fetchRemoteData())
    .retry(3, 1000, ResiliencePolicy.BackoffStrategy.EXPONENTIAL)
    .fallback(Work.read(() -> fetchLocalCache()))
    .thenCompute(data -> process(data))
    .execute();
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
