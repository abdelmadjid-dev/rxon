# RxOn

**RxOn** is a disciplined, semantic DSL wrapper for RxJava 3. It enforces architectural patterns by providing explicit threading, controlled branching, and a strict no-null policy.

## Why RxOn?

*   **Explicit Threading**: No more `subscribeOn` or `observeOn` guessing. Every operation explicitly defines its execution context using `WorkScheduler`.
*   **Zero Nullability**: Designed to fail fast. Emitting `null` at any point in the chain triggers an immediate, traceable error.
*   **Railway-Oriented Programming**: First-class support for branching (`branch`) and early termination (`finish`, `fail`), avoiding deeply nested `flatMap` pyramids.
*   **Encapsulation**: Keeps RxJava primitives (Single, Flowable, etc.) inside your infrastructure layer, exposing a clean, readable API to your business logic.

## Installation

### Gradle
Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.benaether:rxon:0.2.0")
}
```

## Documentation & Migration

For detailed information on version updates and migration instructions, please refer to the following resources:

### Changelogs
| Version | Documentation | Description |
| :--- | :--- | :--- |
| **v0.2.0** | [Release Notes](changelogs/v0.2.0.md) | Unified semantic DSL, new entry points (`start`), and semantic finish mechanism. |

### Migration Guides
| From -> To | Guide | Description |
| :--- | :--- | :--- |
| **v0.1.0 -> v0.2.0** | [Migration Guide](migrations/MIGRATION_0.1.0_TO_0.2.0.md) | Step-by-step guide to migrating to the unified semantic pipeline API. |

## Setup & Initialization

RxOn can be initialized manually or automatically using a `ContentProvider`. The latter is recommended for Android projects to ensure the library is ready before any work starts.

### Automatic Initialization (Android)
Add the `RxOnInitializer` to your `AndroidManifest.xml`. This ensures `RxOnConfig` is initialized as soon as the app starts.

```xml
<application>
    <provider
        android:name="com.benaether.rxon.sample.RxOnInitializer"
        android:authorities="${applicationId}.rxon-init"
        android:exported="false"
        android:initOrder="100" />
</application>
```

*(Note: Use your own implementation of `RxOnInitializer` that calls `RxOnConfig.init()` with your preferred settings.)*

### Manual Initialization
In your `Application.onCreate()` or main entry point:

```java
RxOnConfig.builder()
    .debug(BuildConfig.DEBUG)
    .logger(new MyRxLogger())
    .errorMapper(new MyApiErrorMapper())
    .init();
```

## Quick Start

### Basic Work
`Work<T>` represents a single-shot computation.

```java
Work.start(WorkScheduler.IO, () -> repository.getUser(id))
    .then(WorkScheduler.COMPUTE, user -> transform(user))
    .executeOn(WorkScheduler.MAIN, 
        result -> ui.show(result),
        error -> ui.error(error)
    );
```

### Continuous Stream
`Stream<T>` represents continuous data over time.

```java
Stream.start(WorkScheduler.IO, database.observeItems())
    .thenOnlyIf(item -> item.isValid(), item -> Stream.start(WorkScheduler.COMPUTE, Flowable.just(item)))
    .executeOn(WorkScheduler.MAIN, 
        item -> ui.update(item),
        error -> ui.handle(error)
    );
```

### Controlled Branching
```java
work.then(WorkScheduler.COMPUTE, data -> {
    if (data.isExpired()) return Work.fail(new ExpiredException());
    if (data.isCached()) return Work.finish(data);
    return fetchRemote(data);
});
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
