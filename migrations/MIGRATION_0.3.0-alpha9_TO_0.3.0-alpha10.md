# Migration Guide: v0.3.0-alpha9 to v0.3.0-alpha10

RxOn v0.3.0-alpha10 improves error sanitization for callback emitter functions and introduces automated GitHub Release changelog generation.

---

## 1. Dependency Update

Update your `build.gradle` or `build.gradle.kts` dependency declaration:

```kotlin
dependencies {
    implementation("com.benaether:rxon:0.3.0-alpha10")
}
```

---

## 2. Emitter Error Handling

No breaking API changes were introduced. All callback emitters constructed using `Work.singleEmitter` or `Work.completableEmitter` now automatically clean `RxJavaAssemblyException` stack trace noise when `emitter.onError(t)` or `emitter.tryOnError(t)` is called.
