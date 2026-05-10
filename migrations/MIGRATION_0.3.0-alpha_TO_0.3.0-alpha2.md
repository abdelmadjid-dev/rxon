# Migration Guide: v0.3.0-alpha to v0.3.0-alpha2

v0.3.0-alpha2 introduces first-class support for async task integration and pipeline composition across all schedulers.

## 1. Non-Blocking Async Tasks
Instead of calling `.blockingGet()` inside any stage, use the new async operators available for your specific scheduler.

**Old Pattern (Blocking):**
```java
Work.io(() -> service.fetch(id).blockingGet());
```

**New Pattern (Non-Blocking):**
```java
Work.io(() -> id).thenIoSingle(id -> service.fetch(id));
```
*Note: Available as `thenReadSingle`, `thenWriteSingle`, `thenIoSingle`, `thenComputeSingle`, and `thenMainSingle`.*

## 2. Universal Semantic Side-Effects (peekX)
The `peekX(Consumer<T>)` operators are now available for all semantic stages, allowing for cleaner side-effect definitions without context ceremony. These replace the previous experimental `thenX(Consumer)` overloads to avoid method ambiguity.

**New Pattern:**
```java
Work.io(() -> data)
    .peekWrite(data -> database.save(data))
    .peekMain(data -> ui.show(data));
```

## 3. Pipeline Composition
Join blueprints or chain sub-pipelines dynamically.

**Blueprint Merging:**
```java
Work<User, C> pipeline = loginWork.then(fetchProfileWork);
```

**Dynamic Chaining:**
```java
Work.io(() -> id).thenChain(id -> database.getUserWork(id));
```
