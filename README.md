# RxOn

RxOn is a semantic DSL wrapper for RxJava 3 that enforces architectural patterns through lazy pipeline orchestration, explicit threading, and strict null safety.

## Core Features
* **Lazy Orchestration**: Pipelines are declarative blueprints; execution is deferred until a terminal operator is invoked.
* **Thread Isolation**: Operations explicitly define execution context via `WorkScheduler` (e.g., `.thenIo()`, `.thenCompute()`).
* **Zero-Nesting Architecture**: Flat API structure for branching, error handling, and asynchronous composition.
* **Null Safety**: immediate failure upon null emission in any stage of the pipeline.
* **Functional Context**: Typed context propagation between isolated pipeline stages.

## Installation

### Gradle
```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha")
}
```

## Documentation

### Changelogs
| Version | Documentation | Description |
| :--- | :--- | :--- |
| **v0.3.0-alpha** | [Release Notes](changelogs/v0.3.0-alpha.md) | Lazy orchestration engine, functional context, and component renames. |
| **v0.2.2** | [Release Notes](changelogs/v0.2.2.md) | Sync/async chaining disambiguation. |
| **v0.2.1** | [Release Notes](changelogs/v0.2.1.md) | Unified chaining and Throwable support. |
| **v0.2.0** | [Release Notes](changelogs/v0.2.0.md) | Semantic DSL and pipeline API. |

### Migration Guides
| From -> To | Guide | Description |
| :--- | :--- | :--- |
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
Work.io(() -> repository.getUser(id))
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
