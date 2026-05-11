# Migration Guide: v0.3.0-alpha2 to v0.3.0-alpha3

## Summary
v0.3.0-alpha3 renames `finish()` to `breakWork()` to better reflect its role in recoverable flow control. It also adds several new operators for more expressive pipeline orchestration.

## 1. Rename `finish()` to `breakWork()` / `thenBreak()`
The `finish()` method is now deprecated. It has been split into `breakWork` (static) and `thenBreak` (instance).

### Before
```java
Work.io(() -> "Action")
    .then(Work.finish("Done"));
```

### After
```java
Work.io(() -> "Action")
    .thenBreak("Done");
```

## 2. Using `recoverBreak()`
If you want to resume a pipeline after an early exit, you can now use `recoverBreak()`.

```java
Work.io(() -> findUser(id))
    .ifTrue(user -> user == null, Work.breakWork("Not Found")) // Early exit if null
    .recoverBreak("Guest User") // Resume with a default if it was broken
    .thenCompute(user -> "Welcome, " + user);
```

## 3. Using `peekChain()`
For side-effect sub-pipelines that shouldn't change the main value or context:

### Before (Manual preservation)
```java
Work.io(() -> value)
    .thenChain(val -> someSideEffect(val).then(Work.finish(val)));
```

### After
```java
Work.io(() -> value)
    .peekChain(val -> someSideEffect(val));
```

## 4. Relaxed `then()` Requirement
You can now merge blueprints with different context types into your pipeline if the merged blueprint is context-agnostic (e.g., from `breakWork` or `fail`).

```java
Work.withContext(() -> "Context")
    .then(Work.breakWork("Early Stop")); // This now compiles without explicit casting
```
