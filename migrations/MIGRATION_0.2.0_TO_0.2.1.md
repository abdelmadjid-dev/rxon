# Migration Guide: v0.2.0 to v0.2.1

RxOn v0.2.1 introduces an overloaded `then()` operator. While this provides a more unified API, it may introduce some lambda ambiguity in specific edge cases.

## 1. Unified `then()` Overloads
In v0.2.0, `then()` was primarily used for `Work` or `Stream` composition. In v0.2.1, it now handles raw mappings and RxJava primitives as well.

### Synchronous Mapping
```java
// v0.2.0 (using deprecated on)
work.on(WorkScheduler.COMPUTE, data -> data.toString());

// v0.2.1 (using unified then)
work.then(data -> data.toString()); 
```

### Async Boundaries
```java
// v0.2.0 (using deprecated onAsync)
work.onAsync(WorkScheduler.IO, data -> api.update(data));

// v0.2.1 (using unified then)
work.then(data -> api.update(data)); // Works if api.update returns Single, Work, or Completable
```

## 2. Resolving Lambda Ambiguity
If the compiler cannot determine which `then()` overload to use, you may see an "ambiguous method call" error. This typically happens when the return type of your lambda could match multiple functional interfaces (e.g., returning a `Work` object which could be seen as a raw value or a next stage).

**Solution**: Use an explicit cast or a method reference:
```java
// Ambiguous
work.then(s -> Work.finish("done"));

// Fixed
work.then((WorkScope<String, String>) s -> Work.finish("done"));
```

## 3. Throwable Support
Functional interfaces now throw `Throwable`. You no longer need to wrap checked exceptions in `RuntimeException`.

```java
// v0.2.0
work.then(data -> {
    try {
        return performTask(data);
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
});

// v0.2.1
work.then(data -> performTask(data)); // performTask can throw IOException
```
