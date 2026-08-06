# Migration Guide: v0.3.0-alpha6 to v0.3.0-alpha7

RxOn v0.3.0-alpha7 introduces refinements to the configuration DSL and the error processing pipeline. While these changes are largely additive, there is a minor shift in the configuration step order.

---

## 1. Update `RxOnConfig` Initialization

In v0.3.0-alpha6, `.cleanStackTrace(boolean)` and `.errorMapper(RxOnErrorMapper)` were mutually exclusive terminal steps in the builder. In v0.3.0-alpha7, they can be chained together, and the recommended order has changed.

### Before (v0.3.0-alpha6):
```java
// You had to choose one or the other
RxOnConfig.builder()
    .debug(true)
    .logger(myLogger)
    .errorMapper(myMapper) // Terminal step
    .init();

// OR
RxOnConfig.builder()
    .debug(true)
    .logger(myLogger)
    .cleanStackTrace(true) // Terminal step
    .init();
```

### After (v0.3.0-alpha7):
```java
// You can now use both, and cleanStackTrace comes first
RxOnConfig.builder()
    .debug(true)
    .logger(myLogger)
    .cleanStackTrace(true) // Optional
    .errorMapper(myMapper)  // Optional
    .init();
```

---

## 2. Behavioral Change: Stacktrace Cleaning Order

In v0.3.0-alpha6, stacktrace cleaning (if enabled) was applied *after* the error mapper. In v0.3.0-alpha7, it is applied *before*.

If your custom `RxOnErrorMapper` relies on inspecting RxJava internal frames or the specific structure of `CompositeException` before they are pruned by `StackTraceCleaner`, you may need to adjust your mapping logic. In most cases, this change is beneficial as it provides your mapper with a cleaner, more relevant exception chain.

---

## 3. Dependency Update

Update your `build.gradle` to reference the new version:

```kotlin
implementation("com.benaether:rxon:0.3.0-alpha7")
```
