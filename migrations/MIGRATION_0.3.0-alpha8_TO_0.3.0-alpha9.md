# Migration Guide: v0.3.0-alpha8 to v0.3.0-alpha9

RxOn v0.3.0-alpha9 transitions the core library to a single-generic architecture, removing internal context tracking in favor of explicit value merging and centralized terminal execution.

---

## 1. Dependency Update

Update your `build.gradle` or `build.gradle.kts` dependency declaration:

```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha9")
}
```

---

## 2. Generic Signature Migration

Remove the secondary `Context` generic parameter from `Work` and `Observe` declarations.

### Before:
```java
Work<User, MyContext> userWork = Work.callable(WorkScheduler.IO, () -> fetchUser());
Observe<Event, MyContext> eventStream = Observe.flow(WorkScheduler.IO, flowable);
```

### After:
```java
Work<User> userWork = Work.callable(WorkScheduler.IO, () -> fetchUser());
Observe<Event> eventStream = Observe.flow(WorkScheduler.IO, flowable);
```

---

## 3. Explicit State Merging

If you relied on implicit context propagation across stages, use the explicit `BiFunction<T, R, U> merger` overloads on chaining operators (`thenFunction`, `thenSingle`, `thenCallable`, etc.).

### Before (Context-based):
```java
Work.callable(WorkScheduler.IO, () -> fetchUser())
    .pushContext((user, ctx) -> ctx.withUser(user))
    .thenSingle(WorkScheduler.IO, user -> fetchOrders(user.getId()))
    .thenFunction(WorkScheduler.COMPUTE, (orders, ctx) -> combine(ctx.getUser(), orders));
```

### After (Explicit State Merging):
```java
Work.callable(WorkScheduler.IO, () -> fetchUser())
    .thenSingle(
        WorkScheduler.IO,
        user -> fetchOrders(user.getId()),
        (user, orders) -> new UserOrders(user, orders)
    );
```

---

## 4. PipelineResult and Exceptions

If your code inspected `PipelineResult.getContext()` or `PipelineException.getContext()`, remove those invocations. Pipeline results and errors are strictly value and throwable based.

---

## 5. Callback & Emitter Integrations

If you were manually creating `Single` or `Completable` instances to bridge callback-based SDKs into RxOn, you can now use the built-in emitter helpers:

```java
Work.completableEmitter(WorkScheduler.IO, emitter -> {
    legacyClient.doAsyncWork(new Callback() {
        @Override public void onComplete() { emitter.onComplete(); }
        @Override public void onError(Exception e) { emitter.onError(e); }
    });
});
```
