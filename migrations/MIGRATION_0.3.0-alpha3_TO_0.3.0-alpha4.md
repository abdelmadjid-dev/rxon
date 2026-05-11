# Migration Guide: v0.3.0-alpha3 to v0.3.0-alpha4

RxOn v0.3.0-alpha4 introduces a major refactor of the API naming convention to achieve full symmetry across all gates and pipeline types.

## 1. Adopt Param-Based Naming
Legacy gate-prefixed methods have been removed in favor of explicit parameter-based naming.

**Before:**
```java
Work.io(() -> "data")
    .thenCompute(val -> val.toUpperCase())
    .thenIoSingle(val -> saveAsync(val));
```

**After:**
```java
Work.callable(WorkScheduler.IO, () -> "data")
    .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val.toUpperCase())
    .thenSingle(WorkScheduler.IO, (ctx, val) -> saveAsync(val));
```

## 2. Stream to Observe Rename
The `Stream` class has been renamed to `Observe` and now uses a lazy model.

**Before:**
```java
Stream.flow(source)
    .debounce(100, TimeUnit.MILLISECONDS)
    .trigger(work);
```

**After:**
```java
Observe.flow(WorkScheduler.MAIN, source)
    .debounce(100, TimeUnit.MILLISECONDS)
    .trigger(work);
```

## 3. Explicit Scheduler Injection
All entry points now require a `WorkScheduler`.

| Old Entry Point | New Entry Point |
| :--- | :--- |
| `Work.read(Callable)` | `Work.callable(WorkScheduler.DATA_READ, Callable)` |
| `Work.write(Runnable)` | `Work.action(WorkScheduler.DATA_WRITE, Runnable)` |
| `Work.io(Callable)` | `Work.callable(WorkScheduler.IO, Callable)` |
| `Work.compute(Callable)` | `Work.callable(WorkScheduler.COMPUTE, Callable)` |

## 4. Context Access in Chaining
Most chaining operators now provide the current context `C` as the first argument to the lambda.

**Before:**
```java
.thenFunction(WorkScheduler.COMPUTE, val -> val + 1)
```

**After:**
```java
.thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val + 1)
```

## 5. Enable Debugging
If you want to leverage the new lifecycle logging, ensure you initialize the library in debug mode:

```java
RxOnConfig.builder()
    .debug(true)
    .init();
```
