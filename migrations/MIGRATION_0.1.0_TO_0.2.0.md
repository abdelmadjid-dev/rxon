# Migration Guide: v0.1.0 to v0.2.0

RxOn v0.2.0 introduces a modernized, unified semantic DSL. While backward compatibility has been maintained through `@Deprecated` annotations, we strongly recommend migrating to the new syntax as the old methods will be removed in future releases.

This guide outlines the steps to migrate your existing code to the new API.

## 1. Entry Points: Use `start()`
All entry points for both `Work` and `Stream` have been unified under the `start()` method.

**Work:**
```java
// v0.1.0
Work.on(WorkScheduler.IO, () -> fetchData());
Work.onUnit(WorkScheduler.IO, () -> performSideEffect());

// v0.2.0
Work.start(WorkScheduler.IO, () -> fetchData());
Work.start(WorkScheduler.IO, () -> performSideEffect()); // Automatically maps Runnable to Work<Done>
```

**Stream:**
```java
// v0.1.0
Stream.onAsync(WorkScheduler.IO, flowableSource);

// v0.2.0
Stream.start(WorkScheduler.IO, flowableSource);
```

## 2. Chaining: Use `then()`
Chaining operators like `on`, `onAsync`, and `switchOn` have been unified under `then()`.

```java
// v0.1.0
work.on(WorkScheduler.COMPUTE, data -> mapData(data))
    .switchOn(WorkScheduler.IO, mapped -> branchLogic(mapped));

// v0.2.0
work.then(WorkScheduler.COMPUTE, data -> mapData(data))
    .then(WorkScheduler.IO, mapped -> branchLogic(mapped));
```

## 3. Early Returns: `halt()` is now `finish()`
The semantic early-return mechanism has been renamed from `halt` to `finish` to better represent a successful early completion.

```java
// v0.1.0
if (cacheHit) return Work.halt(cachedData);

// v0.2.0
if (cacheHit) return Work.finish(cachedData);
```

## 4. Conditional Guards: `require()` is now `thenOnlyIf()`
If you previously used custom conditional logic to stop a chain, use the built-in `thenOnlyIf()` guard.

```java
// v0.2.0 (New Feature)
work.thenOnlyIf(
    data -> data.isValid(), 
    data -> Work.start(WorkScheduler.COMPUTE, () -> process(data))
);
```

## 5. RxJava Interoperability: Use `asTerminal*()`
When bridging from RxOn back to standard RxJava (`Single` or `Flowable`), use the new `asTerminalSingle()` or `asTerminalFlowable()` methods to ensure any internal signals (like `finish`) are correctly resolved.

**Work:**
```java
// v0.1.0
Single<Data> single = work.asSafeSingle();

// v0.2.0
Single<Data> single = work.asTerminalSingle();
```

**Stream:**
```java
// v0.1.0
Flowable<Data> flowable = stream.asSafeFlowable();

// v0.2.0
Flowable<Data> flowable = stream.asTerminalFlowable();
```

## 6. Void Types: `Unit` is now `Done`
The internal `Unit` type has been replaced with `Done` to better express completion semantics.

```java
// v0.1.0
Work<Unit> task = Work.onUnit(...);

// v0.2.0
Work<Done> task = Work.start(...);
```
