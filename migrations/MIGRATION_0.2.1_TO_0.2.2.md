# Migration Guide: v0.2.1 to v0.2.2

RxOn v0.2.2 splits the chaining API into distinct methods to resolve Java lambda ambiguity.

## 1. The `then` vs `chain` Split

### Synchronous Mapping (Raw Values)
Keep using `then()`.
```java
// No change
work.then(data -> data.toString());
```

### Asynchronous Composition (Work/Stream)
Use `chain()`.
```java
// v0.2.1
work.then(data -> Work.start(...));

// v0.2.2
work.chain(data -> Work.start(...));
```

### RxJava Chaining (Single/Completable/Publisher)
Use the specialized `chain` methods. This eliminates the need for explicit casts.
```java
// v0.2.1 (Required cast)
work.then((AsyncScope<T, R>) data -> repository.getSingle(data));

// v0.2.2 (Auto-resolved)
work.chainSingle(data -> repository.getSingle(data));
work.chainCompletable(data -> repository.doSomething());
stream.chainPublisher(data -> Flowable.just(data));
```

## 2. Renamed Conditional Operators
To align with the new `chain` terminology:
- `thenIf` -> **`chainIf`**
- `thenOnlyIf` -> **`chainOnlyIf`**

```java
// v0.2.1
work.thenIf(cond, sideEffect);

// v0.2.2
work.chainIf(cond, sideEffect);
```
