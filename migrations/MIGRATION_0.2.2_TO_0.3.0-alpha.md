# Migration Guide: v0.2.2 to v0.3.0-alpha

v0.3.0-alpha transitions RxOn to a lazy orchestration engine. This guide details the API changes required for migration.

## 1. Entry Point Renaming
Static factory methods on `Work` have removed the `from` prefix.

**Old API:**
```java
Work.fromIo(() -> fetchData());
Work.fromRead(() -> readDatabase());
Work.fromWrite(() -> writeDatabase());
```

**New API:**
```java
Work.io(() -> fetchData());
Work.read(() -> readDatabase());
Work.write(() -> writeDatabase());
Work.compute(() -> calculate());
Work.main(() -> updateUi());
```

## 2. Pipeline Chaining Prefix
Intermediate chaining operators now require the `then` prefix.

**Old API:**
```java
Work.io(() -> fetch())
    .compute(data -> process(data))
    .write(result -> save(result));
```

**New API:**
```java
Work.io(() -> fetch())
    .thenCompute(data -> process(data))
    .thenWrite(result -> save(result));
```

## 3. Lazy Context Initialization
`withContext()` now requires a `Supplier<C>` to ensure lazy initialization during pipeline execution.

**Old API:**
```java
Work.withContext(new MyContext());
```

**New API:**
```java
Work.withContext(() -> new MyContext());
```

## 4. `Observe` Component
`Monitor` is renamed to `Observe`. Entry point `observe()` is renamed to `flow()`.

**Old API:**
```java
Monitor.observe(flowable).trigger(work);
```

**New API:**
```java
Observe.flow(flowable).trigger(work);
```

## 5. Mandatory Terminal Operators
Pipelines are deferred and require a terminal operator for execution. Ensure all chains conclude with `.execute()`, `.executeOn()`, or `.asTerminalSingle()`.
