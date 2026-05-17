# Migration Guide: v0.3.0-alpha4 to v0.3.0-alpha5

RxOn v0.3.0-alpha5 introduces a significant design refinement. The library has transitioned from the two-parameter pipeline model (`Work<T, C>` / `Observe<T, C>`) to a clean, single-parameter pipeline model (`Work<T>` / `Observe<T>`). Implicit context tracking has been replaced by explicit, developer-defined state merging.

This guide provides step-by-step instructions to migrate your applications.

---

## 1. Simplify Generic Type Declarations

All references to `Work` and `Observe` must be updated to remove the second generic type parameter (`C`).

### Before (v0.3.0-alpha4):
```java
Work<User, MyContext> work = Work.callable(WorkScheduler.IO, () -> fetchUser());
Observe<Event, MyContext> observe = Observe.flow(WorkScheduler.MAIN, flowable);
```

### After (v0.3.0-alpha5):
```java
Work<User> work = Work.callable(WorkScheduler.IO, () -> fetchUser());
Observe<Event> observe = Observe.flow(WorkScheduler.MAIN, flowable);
```

---

## 2. Transition from Context Lambdas to Explicit Merger Overloads

In v0.3.0-alpha4, chaining operators took lambdas with a double argument signature `(ctx, val)` representing the current context and the incoming value. In v0.3.0-alpha5, the primary overload takes a simple single-parameter lambda `val -> ...`. 

For pipelines that need to propagate or combine previous results (acting as "context"), you now use the explicit `BiFunction<T, R, U> merger` overload.

### Before (v0.3.0-alpha4):
```java
Work.callable(WorkScheduler.IO, () -> "User1")
    // Double argument lambda: (ctx, val)
    .thenFunction(WorkScheduler.COMPUTE, (ctx, username) -> username.toUpperCase());
```

### After (v0.3.0-alpha5):
```java
Work.callable(WorkScheduler.IO, () -> "User1")
    // Single argument lambda: val
    .thenFunction(WorkScheduler.COMPUTE, username -> username.toUpperCase());
```

### Retaining State via Merger Overloads:
To keep a previous value down the pipeline:

#### Before (v0.3.0-alpha4):
```java
Work.callable(WorkScheduler.IO, () -> "User1")
    // Implicit context propagation via updateContext
    .updateContext((ctx, username) -> username)
    .thenFunction(WorkScheduler.IO, (ctx, username) -> fetchTheme(username))
    .thenFunction(WorkScheduler.COMPUTE, (ctx, theme) -> ctx + " prefers " + theme);
```

#### After (v0.3.0-alpha5):
```java
Work.callable(WorkScheduler.IO, () -> "User1")
    // Explicit value propagation using merger function: (previousValue, newValue) -> result
    .thenFunction(
        WorkScheduler.IO, 
        username -> fetchTheme(username),
        (username, theme) -> username + " prefers " + theme
    );
```

---

## 3. Adopt Smart Branching (`thenBranch`)

Replace complex `thenChain` conditional checks with the new native branching operator.

### Before (v0.3.0-alpha4):
```java
.thenChain(val -> {
    if (val.isPremium()) {
        return Work.callable(WorkScheduler.IO, () -> fetchPremiumDetails());
    } else {
        return Work.callable(WorkScheduler.COMPUTE, () -> DefaultDetails.INSTANCE);
    }
})
```

### After (v0.3.0-alpha5):
```java
.thenBranch(
    val -> val.isPremium(),
    Work.callable(WorkScheduler.IO, () -> fetchPremiumDetails()),
    Work.callable(WorkScheduler.COMPUTE, () -> DefaultDetails.INSTANCE)
)
```

---

## 4. Leverage Segment Resilience

Instead of adding individual retries or fallbacks to multiple stages, group them collectively into a segment resilience block.

### Before (v0.3.0-alpha4):
```java
Work.callable(WorkScheduler.IO, () -> api.step1())
    .retry(3, 100, BackoffStrategy.LINEAR)
    .thenFunction(WorkScheduler.IO, val -> api.step2(val))
    .retry(3, 100, BackoffStrategy.LINEAR);
```

### After (v0.3.0-alpha5):
```java
Work.callable(WorkScheduler.IO, () -> api.step1())
    .thenFunction(WorkScheduler.IO, val -> api.step2(val))
    // Applies retry to the entire preceding block collectively
    .resilience(policy -> policy
        .retry(3, 100, ResiliencePolicy.BackoffStrategy.LINEAR)
        .timeout(5, TimeUnit.SECONDS)
    );
```

---

## 5. Enable Stacktrace Cleaning

Ensure you benefit from reduced stacktrace noise by enabling the new stacktrace cleaner in your configuration builder:

```java
RxOnConfig.builder()
    .debug(true)
    .cleanStackTrace(true) // <-- New configuration parameter
    .init();
```
