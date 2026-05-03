# RxOn

**RxOn** is a disciplined, semantic DSL wrapper for RxJava 3. It enforces architectural patterns by providing explicit threading, controlled branching, and a strict no-null policy.

## Why RxOn?

*   **Explicit Threading**: No more `subscribeOn` or `observeOn` guessing. Every operation explicitly defines its execution context using `WorkScheduler`.
*   **Zero Nullability**: Designed to fail fast. Emitting `null` at any point in the chain triggers an immediate, traceable error.
*   **Railway-Oriented Programming**: First-class support for branching (`branch`, `switchOn`) and early termination (`halt`, `fail`), avoiding deeply nested `flatMap` pyramids.
*   **Encapsulation**: Keeps RxJava primitives (Single, Flowable, etc.) inside your infrastructure layer, exposing a clean, readable API to your business logic.

## Installation

### Gradle
Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.benaether:rxon:0.1.0")
}
```

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
Work.on(WorkScheduler.IO, () -> repository.getUser(id))
    .on(WorkScheduler.COMPUTE, user -> transform(user))
    .executeOn(WorkScheduler.MAIN, 
        result -> ui.show(result),
        error -> ui.error(error)
    );
```

### Continuous Stream
`Stream<T>` represents continuous data over time.

```java
Stream.onAsync(WorkScheduler.IO, database.observeItems())
    .require(WorkScheduler.COMPUTE, item -> item.isValid(), item -> new InvalidItemException())
    .executeOn(WorkScheduler.MAIN, 
        item -> ui.update(item),
        error -> ui.handle(error)
    );
```

### Controlled Branching
```java
work.switchOn(WorkScheduler.COMPUTE, data -> {
    if (data.isExpired()) return Work.fail(new ExpiredException());
    if (data.isCached()) return Work.halt(data);
    return fetchRemote(data);
});
```

## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
