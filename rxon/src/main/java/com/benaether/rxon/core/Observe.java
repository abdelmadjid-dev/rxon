/*
 * Copyright 2026 Abdelmadjid
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.benaether.rxon.core;

import com.benaether.rxon.rx.RxLog;
import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;

/**
 * {@code Observe<T, C>} is a lazy signal orchestration engine for streaming data.
 * It remains at functional parity with {@link Work} but handles multiple emissions.
 */
public final class Observe<T> {

    private static final String TAG = Observe.class.getName();

    private final Flowable<Object> source;
    private final List<PipelineStage> stages;
    private final String tag;
    
    private Observe(Flowable<Object> source, List<PipelineStage> stages) {
        this(source, stages, "Observe");
    }

    private Observe(Flowable<Object> source, List<PipelineStage> stages, String tag) {
        this.source = source;
        this.stages = Collections.unmodifiableList(stages);
        this.tag = tag;
    }

    private <R> Observe<R> append(PipelineStage stage) {
        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        newStages.add(stage);
        return new Observe<>(source, newStages, tag);
    }

    List<PipelineStage> getStages() {
        return stages;
    }

    String getTag() {
        return tag;
    }

    /**
     * Assign a semantic name to this Observe instance for debugging purposes.
     */
    public Observe<T> tag(String name) {
        return new Observe<>(this.source, this.stages, name);
    }

    // Entry Points

    public static <T> Observe<T> flow(WorkScheduler scheduler, Flowable<T> source) {
        return new Observe<>(
            source.subscribeOn(SchedulerResolver.resolve(scheduler)).map(v -> (Object) v),
            new ArrayList<>(),
            "Observe"
        );
    }

    public static <T> Observe<T> observable(WorkScheduler scheduler, Observable<T> source) {
        return flow(scheduler, source.toFlowable(BackpressureStrategy.BUFFER));
    }

    public static <T> Observe<T> iterable(WorkScheduler scheduler, Iterable<T> source) {
        return flow(scheduler, Flowable.fromIterable(source));
    }

    public static <T> Observe<T> single(WorkScheduler scheduler, Single<T> source) {
        return flow(scheduler, source.toFlowable());
    }

    public static <T> Observe<T> maybe(WorkScheduler scheduler, Maybe<T> source) {
        return flow(scheduler, source.toFlowable());
    }

    public static Observe<Done> completable(WorkScheduler scheduler, Completable source) {
        return flow(scheduler, source.toFlowable());
    }

    // Chaining Operators

    public Observe<Done> thenAction(WorkScheduler scheduler, Runnable action) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> { action.run(); return Done.INSTANCE; },
            scheduler
        ));
    }

    public <R> Observe<R> thenAction(WorkScheduler scheduler, Runnable action, BiFunction<T, Done, R> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> { action.run(); return merger.apply((T) val, Done.INSTANCE); },
            scheduler
        ));
    }

    public <R> Observe<R> thenCallable(WorkScheduler scheduler, Callable<R> block) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { return block.call(); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public <R> Observe<R> thenFunction(WorkScheduler scheduler, Function<T, R> task) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> task.apply((T) val),
            scheduler
        ));
    }

    public <R, U> Observe<U> thenFunction(WorkScheduler scheduler, Function<T, R> task, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> merger.apply((T) val, task.apply((T) val)),
            scheduler
        ));
    }

    public Observe<T> thenConsumer(WorkScheduler scheduler, Consumer<T> action) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { action.accept((T) val); return val; }
                catch (Throwable e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public <R> Observe<R> thenConsumer(WorkScheduler scheduler, Consumer<T> action, BiFunction<T, T, R> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { action.accept((T) val); return merger.apply((T) val, (T) val); }
                catch (Throwable e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public Observe<T> doOnNext(WorkScheduler scheduler, Consumer<T> action) {
        return thenConsumer(scheduler, action);
    }

    public <R> Observe<R> thenSingle(WorkScheduler scheduler, Function<T, Single<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> (Single<Object>) (Single<?>) mapper.apply((T) val),
            scheduler
        ));
    }

    public <R, U> Observe<U> thenSingle(WorkScheduler scheduler, Function<T, Single<R>> mapper, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    public <R> Observe<R> thenMaybe(WorkScheduler scheduler, Function<T, Maybe<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> (Single<Object>) (Single<?>) mapper.apply((T) val).toSingle(),
            scheduler
        ));
    }

    public <R, U> Observe<U> thenMaybe(WorkScheduler scheduler, Function<T, Maybe<R>> mapper, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).toSingle().map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    public Observe<Done> thenCompletable(WorkScheduler scheduler, Function<T, Completable> mapper) {
        return append(new PipelineStage.StreamingStage(
            (val, ignored) -> mapper.apply((T) val).andThen(Flowable.just(Done.INSTANCE)),
            scheduler
        ));
    }

    public <R> Observe<R> thenCompletable(WorkScheduler scheduler, Function<T, Completable> mapper, BiFunction<T, Done, R> merger) {
        return append(new PipelineStage.StreamingStage(
            (val, ignored) -> mapper.apply((T) val).andThen(Flowable.just(Done.INSTANCE)).map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    // Streaming Chaining

    public <R> Observe<R> thenFlowable(WorkScheduler scheduler, Function<T, Flowable<R>> mapper) {
        return append(new PipelineStage.StreamingStage(
            (val, ignored) -> (Flowable<Object>) (Flowable<?>) mapper.apply((T) val),
            scheduler
        ));
    }

    public <R> Observe<R> thenObservable(WorkScheduler scheduler, Function<T, Observable<R>> mapper) {
        return thenFlowable(scheduler, val -> mapper.apply(val).toFlowable(BackpressureStrategy.BUFFER));
    }

    public <R> Observe<R> thenChain(Function<T, Work<R>> task) {
        return append(new PipelineStage.ChainStage(
            val -> task.apply((T) val)
        ));
    }

    public <R, U> Observe<U> thenChain(Function<T, Work<R>> task, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.ChainStage(
            val -> task.apply((T) val).thenFunction(WorkScheduler.COMPUTE, res -> merger.apply((T) val, res))
        ));
    }

    /**
     * Executes one of two pipeline segments based on a condition for each emission.
     */
    @SuppressWarnings("unchecked")
    public <R> Observe<R> thenBranch(io.reactivex.rxjava3.functions.Predicate<T> condition, Work<R> onTrue, Work<R> onFalse) {
        return append(new PipelineStage.ChainStage(
            val -> {
                try {
                    if (condition.test((T) val)) return onTrue;
                    return onFalse;
                } catch (Throwable e) {
                    return (Work<R>) (Work<?>) Work.fail(e);
                }
            }
        ));
    }

    // Composition Operators

    @SuppressWarnings("unchecked")
    public <U, R> Observe<R> zipWith(Work<U> other, BiFunction<T, U, R> zipper) {
        return append(new PipelineStage.ZipStage(
            other,
            (v1, v2) -> zipper.apply((T) v1, (U) v2)
        ));
    }

    /**
     * Zips each emission with multiple work segments.
     */
    public Observe<List<Object>> zip(WorkScheduler scheduler, Work<?>... others) {
        Observe<List<Object>> current = thenFunction(scheduler, val -> {
            List<Object> list = new ArrayList<>();
            list.add(val);
            return list;
        });

        for (Work<?> other : others) {
            current = current.zipWith(other, (list, nextVal) -> {
                list.add(nextVal);
                return list;
            });
        }
        return current;
    }

    // Signal Conditioning (Lazy)

    public Observe<T> debounce(WorkScheduler scheduler, long time, TimeUnit unit) {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.DEBOUNCE, time, unit, scheduler));
    }

    public Observe<T> throttle(WorkScheduler scheduler, long time, TimeUnit unit) {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.THROTTLE, time, unit, scheduler));
    }

    public Observe<T> distinct(WorkScheduler scheduler) {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.DISTINCT, scheduler));
    }

    /**
     * Converts the cold stream into a hot shared stream for multiple subscribers.
     */
    public Observe<T> share(WorkScheduler scheduler) {
        return new Observe<>(source.observeOn(SchedulerResolver.resolve(scheduler)).share(), stages, tag);
    }

    /**
     * Configures the backpressure strategy for this observation stream.
     */
    public Observe<T> withBackpressure(WorkScheduler scheduler, io.reactivex.rxjava3.core.BackpressureStrategy strategy) {
        return new Observe<>(source.observeOn(SchedulerResolver.resolve(scheduler)).onBackpressureBuffer().onBackpressureDrop().toObservable().toFlowable(strategy).map(v -> v), stages, tag);
    }

    // Specialized Operators (Parity with Work)

    public Observe<T> require(WorkScheduler scheduler, Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenFunction(scheduler, t -> {
            try {
                if (condition.test(t)) return t;
                Throwable error = errorSupplier.apply(t);
                if (error instanceof RuntimeException re) throw re;
                throw new RuntimeException(error);
            } catch (Throwable e) {
                if (e instanceof RuntimeException re) throw re;
                throw new RuntimeException(e);
            }
        });
    }

    public Observe<T> reject(WorkScheduler scheduler, Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenFunction(scheduler, t -> {
            try {
                if (!condition.test(t)) return t;
                Throwable error = errorSupplier.apply(t);
                if (error instanceof RuntimeException re) throw re;
                throw new RuntimeException(error);
            } catch (Throwable e) {
                if (e instanceof RuntimeException re) throw re;
                throw new RuntimeException(e);
            }
        });
    }

    // Debugging & Logging

    /**
     * Declarative stage to log the current pipeline state.
     */
    public Observe<T> log(LogLevel level) {
        return log(level, null);
    }

    /**
     * Declarative stage to log the current pipeline state with a custom message.
     */
    public Observe<T> log(LogLevel level, String message) {
        return append(new PipelineStage.LogStage(level, message));
    }

    // Resilience & Recovery

    public Observe<T> recover(java.util.function.Function<Throwable, T> fallback) {
        return recover(Throwable.class, fallback);
    }

    public Observe<T> recover(Class<? extends Throwable> type, java.util.function.Function<Throwable, T> fallback) {
        return append(new PipelineStage.RecoverStage(type, err -> fallback.apply(err)));
    }

    public Observe<T> retry(int max, long delay, ResiliencePolicy.BackoffStrategy strategy) {
        return retryIf(max, delay, strategy, err -> true);
    }

    public Observe<T> retryIf(int max, long delay, ResiliencePolicy.BackoffStrategy strategy, java.util.function.Predicate<Throwable> condition) {
        return resilience(r -> r.retryIf(max, delay, strategy, condition));
    }

    /**
     * Applies a resilience policy to the entire preceding pipeline segment.
     * Note: This applies the policy PER-EMISSION to the preceding stages.
     */
    public Observe<T> resilience(java.util.function.Consumer<ResiliencePolicy.ResilienceBuilder> config) {
        ResiliencePolicy.ResilienceBuilder builder = new ResiliencePolicy.ResilienceBuilder();
        config.accept(builder);
        ResiliencePolicy.ResilienceMetadata m = builder.build();

        // Encapsulate previous stages into a single ChainStage
        Work<T> segment = new Work<>(this.stages, tag);
        PipelineStage segmentStage = new PipelineStage.ChainStage(val -> segment, m);

        List<PipelineStage> newStages = new ArrayList<>();
        newStages.add(segmentStage);
        return new Observe<>(source, newStages, tag);
    }

    // Resource Management

    /**
     * Executes the provided action regardless of success or failure.
     */
    public Observe<T> doFinally(Runnable action) {
        return append(new PipelineStage.FinalStage(action));
    }

    /**
     * Applies a transformation to this observe pipeline.
     */
    public <R> Observe<R> compose(java.util.function.Function<Observe<T>, Observe<R>> transformer) {
        return transformer.apply(this);
    }

    // Terminal Operators

    public Flowable<T> asFlowable() {
        return ObserveCompiler.compile(stages, tag, source, null);
    }

    public Disposable execute() {
        return asFlowable().subscribe(
                t -> { },
                throwable -> RxLog.e(TAG, "Unhandled Throwable", throwable)
        );
    }

    public Disposable executeOn(WorkScheduler workScheduler, Consumer<T> onNext, Consumer<Throwable> onError) {
        return asFlowable()
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .subscribe(onNext, onError);
    }

    public Disposable trigger(Work<?> work) {
        return asFlowable().subscribe(
            signal -> work.execute(),
            throwable -> RxLog.e(TAG, "Observe trigger error", throwable)
        );
    }

    public <R> Disposable trigger(Work<R> work, Consumer<R> onSuccess, Consumer<Throwable> onError) {
        return asFlowable().flatMapSingle(signal -> 
            work.asTerminalSingle()
        ).subscribe(onSuccess, onError);
    }
}
