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
import java.util.function.Supplier;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * {@code Observe<T, C>} is a lazy signal orchestration engine for streaming data.
 * It remains at functional parity with {@link Work} but handles multiple emissions.
 */
public final class Observe<T, C> {

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

    private <R, NewC> Observe<R, NewC> append(PipelineStage stage) {
        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        newStages.add(stage);
        return new Observe<>(source, newStages, tag);
    }

    /**
     * Assign a semantic name to this Observe instance for debugging purposes.
     */
    public Observe<T, C> tag(String name) {
        return new Observe<>(this.source, this.stages, name);
    }

    // Entry Points

    public static <T> Observe<T, Object> flow(WorkScheduler scheduler, Flowable<T> source) {
        return new Observe<>(
            source.subscribeOn(SchedulerResolver.resolve(scheduler)).map(v -> (Object) v),
            new ArrayList<>()
        );
    }

    public static <T> Observe<T, Object> observable(WorkScheduler scheduler, Observable<T> source) {
        return flow(scheduler, source.toFlowable(io.reactivex.rxjava3.core.BackpressureStrategy.BUFFER));
    }

    public static <T> Observe<T, Object> iterable(WorkScheduler scheduler, Iterable<T> source) {
        return flow(scheduler, Flowable.fromIterable(source));
    }

    // Generic Chaining Operators (Parity with Work)

    public Observe<Done, C> thenAction(WorkScheduler scheduler, Runnable action) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> { action.run(); return Done.INSTANCE; },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public <R> Observe<R, C> thenCallable(WorkScheduler scheduler, Callable<R> block) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> {
                try { return block.call(); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public <R> Observe<R, C> thenFunction(WorkScheduler scheduler, BiFunction<C, T, R> task) {
        return thenFunction(scheduler, task, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <R, NewC> Observe<R, NewC> thenFunction(WorkScheduler scheduler, BiFunction<C, T, R> task, BiFunction<C, R, NewC> contextMapper) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> {
                try { return task.apply((C) ctx, (T) val); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            (ctx, res) -> contextMapper.apply((C) ctx, (R) res),
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public Observe<T, C> thenConsumer(WorkScheduler scheduler, BiConsumer<C, T> action) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> { action.accept((C) ctx, (T) val); return val; },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public <R> Observe<R, C> thenSingle(WorkScheduler scheduler, BiFunction<C, T, Single<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> (Single<Object>) (Single<?>) mapper.apply((C) ctx, (T) val),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public <R> Observe<R, C> thenMaybe(WorkScheduler scheduler, BiFunction<C, T, Maybe<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> (Single<Object>) (Single<?>) mapper.apply((C) ctx, (T) val).toSingle(),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public Observe<Done, C> thenCompletable(WorkScheduler scheduler, BiFunction<C, T, Completable> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> mapper.apply((C) ctx, (T) val).toSingle(() -> Done.INSTANCE),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    // Streaming Chaining

    @SuppressWarnings("unchecked")
    public <R> Observe<R, C> thenFlowable(WorkScheduler scheduler, BiFunction<C, T, Flowable<R>> mapper) {
        return append(new PipelineStage.StreamingStage(
            (ctx, val) -> (Flowable<Object>) (Flowable<?>) mapper.apply((C) ctx, (T) val),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public <R> Observe<R, C> thenObservable(WorkScheduler scheduler, BiFunction<C, T, Observable<R>> mapper) {
        return thenFlowable(scheduler, (ctx, val) -> mapper.apply(ctx, val).toFlowable(io.reactivex.rxjava3.core.BackpressureStrategy.BUFFER));
    }

    // Signal Conditioning (Lazy)

    public Observe<T, C> debounce(long time, TimeUnit unit) {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.DEBOUNCE, time, unit));
    }

    public Observe<T, C> throttle(long time, TimeUnit unit) {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.THROTTLE, time, unit));
    }

    public Observe<T, C> distinct() {
        return append(new PipelineStage.ConditioningStage(PipelineStage.ConditioningType.DISTINCT));
    }

    // Debugging & Logging

    /**
     * Declarative stage to log the current pipeline state.
     */
    public Observe<T, C> log(LogLevel level) {
        return log(level, null);
    }

    /**
     * Declarative stage to log the current pipeline state with a custom message.
     */
    public Observe<T, C> log(LogLevel level, String message) {
        return append(new PipelineStage.LogStage(level, message));
    }

    // Terminal Operators

    public Flowable<T> asFlowable() {
        return ObserveCompiler.compile(stages, tag, source, () -> null);
    }

    public Disposable trigger(Work<?, ?> work) {
        return asFlowable().subscribe(
            signal -> work.execute(),
            throwable -> RxLog.e(TAG, "Observe trigger error", throwable)
        );
    }

    public <R> Disposable trigger(Work<R, ?> work, Consumer<R> onSuccess, Consumer<Throwable> onError) {
        return asFlowable().flatMapSingle(signal -> 
            work.asTerminalSingle()
        ).subscribe(onSuccess, onError);
    }
}
