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
import com.benaether.rxon.scopes.ScopedBoundaries;
import com.benaether.rxon.scopes.ScopedFunctions;
import com.benaether.rxon.scopes.ScopedWorkflows;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * {@code Stream<T>} is a semantic DSL built on top of RxJava {@link Flowable}
 * for modeling continuous, multi-emission data streams with explicit,
 * semantic threading and composition rules.
 *
 * <p>A {@code Stream<T>} represents <b>meaningful data over time</b>.</p>
 *
 * <h2>Core principles</h2>
 *
 * <ul>
 *   <li><b>Multiple emissions</b>: backed by {@link Flowable}</li>
 *   <li><b>No null values</b>: emitting {@code null} will crash</li>
 *   <li>No {@link Done}: side-effects belong in {@link Work}</li>
 *   <li><b>Explicit threading</b>: execution always uses {@link WorkScheduler}</li>
 *   <li><b>Semantic composition</b>: workflows encapsulate their own execution</li>
 * </ul>
 *
 * <h2>Nullability rules (CRITICAL)</h2>
 *
 * <p>Never emit {@code null}. RxJava forbids null emissions.</p>
 *
 * <h2>When to use Stream</h2>
 *
 * <ul>
 *   <li>Database observation</li>
 *   <li>UI state streams</li>
 *   <li>LiveData bridging</li>
 *   <li>Continuous data flows</li>
 * </ul>
 *
 * <p>Use {@link Work} for single-shot operations or side-effects.</p>
 *
 * @param <T> emission type
 */
public final class Stream<T> {

    private static final String TAG = Stream.class.getName();

    private final Flowable<T> flowable;

    Stream(Flowable<T> flowable) {
        this.flowable = flowable;
    }

    // ===========================================================================================
    // INTERNAL BUILDER
    // ===========================================================================================

    private static <T> Stream<T> fromFlowable(
            Flowable<T> source,
            Scheduler scheduler
    ) {
        return new Stream<>(
                mapErrors(source).subscribeOn(scheduler)
        );
    }

    // ===========================================================================================
    // PIPELINE DSL — ENTRY POINTS
    // ===========================================================================================

    /**
     * Start a new Stream pipeline from an asynchronous source.
     */
    public static <T> Stream<T> start(WorkScheduler scheduler, Flowable<T> source) {
        return fromFlowable(source, SchedulerResolver.resolve(scheduler));
    }

    /**
     * Start a new Stream pipeline from a Single.
     */
    public static <T> Stream<T> start(WorkScheduler scheduler, io.reactivex.rxjava3.core.Single<T> source) {
        return start(scheduler, source.toFlowable());
    }

    /**
     * Start a new Stream pipeline from a Completable.
     */
    public static Stream<Done> start(WorkScheduler scheduler, io.reactivex.rxjava3.core.Completable source) {
        return start(scheduler, source.toFlowable());
    }

    // ===========================================================================================
    // PIPELINE DSL — TERMINATION
    // ===========================================================================================

    /** @deprecated Use {@link #finish(Object)}. Will be deleted in future releases. */
    @Deprecated
    public static <T> Stream<T> halt(T value) {
        return finish(value);
    }

    /**
     * Immediately terminate the stream successfully with a value.
     */
    public static <T> Stream<T> finish(T value) {
        if (value == null) {
            return new Stream<>(Flowable.error(
                    new NullPointerException("finish() value cannot be null")
            ));
        }
        return new Stream<>(Flowable.error(new Work.PipelineFinishException(value)));
    }

    /**
     * Immediately terminate the stream with an error.
     */
    public static <T> Stream<T> fail(Throwable throwable) {
        return new Stream<>(Flowable.error(throwable));
    }

    // ===========================================================================================
    // ENTRY POINTS — LEGACY
    // ===========================================================================================

    /** @deprecated Use {@link #start(WorkScheduler, Flowable)}. Will be deleted in future releases. */
    @Deprecated
    public static <T> Stream<T> onAsync(
            WorkScheduler scheduler,
            Flowable<T> source
    ) {
        return start(scheduler, source);
    }

    // ===========================================================================================
    // ENTRY POINTS — WORKFLOWS
    // ===========================================================================================

    public static <I, O> Stream<O> fromWorkflow(
            ScopedWorkflows.StreamWorkflow<I, O> wf,
            I input
    ) {
        return wf.apply(input);
    }

    public static <O> Stream<O> fromWorkflow(
            ScopedWorkflows.StreamWorkflow0<O> wf
    ) {
        return wf.apply();
    }

    // ===========================================================================================
    // PIPELINE DSL — CHAINING
    // ===========================================================================================

    /**
     * Chain another Stream.
     */
    public <R> Stream<R> then(ScopedBoundaries.StreamScope<T, R> fn) {
        return new Stream<>(asFlowable().flatMap(t -> {
            Stream<R> next = fn.apply(t);
            if (next == null) {
                return Flowable.error(new IllegalStateException("then() mapping returned null Stream"));
            }
            return next.asFlowable().onErrorResumeNext(this::recoverFinish);
        }));
    }

    /**
     * Chain another Stream on a specific scheduler.
     */
    public <R> Stream<R> then(WorkScheduler scheduler, ScopedBoundaries.StreamScope<T, R> fn) {
        return new Stream<>(asFlowable()
                .observeOn(SchedulerResolver.resolve(scheduler))
                .flatMap(t -> {
                    Stream<R> next = fn.apply(t);
                    if (next == null) {
                        return Flowable.error(new IllegalStateException("then() mapping returned null Stream"));
                    }
                    return next.asFlowable().onErrorResumeNext(this::recoverFinish);
                }));
    }

    /**
     * Chain an asynchronous transformation or another Stream.
     */
    public <R> Stream<R> then(ScopedBoundaries.StreamAsyncScope<T, R> fn) {
        return new Stream<>(asFlowable().flatMap(t -> {
            org.reactivestreams.Publisher<R> publisher = fn.apply(t);
            if (publisher == null) {
                return Flowable.error(new IllegalStateException("then() returned null Publisher"));
            }
            return Flowable.fromPublisher(publisher)
                    .onErrorResumeNext(this::recoverFinish);
        }));
    }

    /**
     * Chain an asynchronous transformation or another Stream on a specific scheduler.
     */
    public <R> Stream<R> then(WorkScheduler scheduler, ScopedBoundaries.StreamAsyncScope<T, R> fn) {
        return new Stream<>(asFlowable()
                .observeOn(SchedulerResolver.resolve(scheduler))
                .flatMap(t -> {
                    org.reactivestreams.Publisher<R> publisher = fn.apply(t);
                    if (publisher == null) {
                        return Flowable.error(new IllegalStateException("then() returned null Publisher"));
                    }
                    return Flowable.fromPublisher(publisher)
                            .onErrorResumeNext(this::recoverFinish);
                }));
    }

    /**
     * Chain a synchronous transformation (Map).
     */
    public <R> Stream<R> then(ScopedFunctions.ThrowingFn<T, R> fn) {
        return thenMap(fn);
    }

    /**
     * Chain a synchronous transformation (Map) on a specific scheduler.
     */
    public <R> Stream<R> then(WorkScheduler scheduler, ScopedFunctions.ThrowingFn<T, R> fn) {
        return thenMap(fn, SchedulerResolver.resolve(scheduler));
    }

    // ===========================================================================================
    // PIPELINE DSL — CONDITIONAL
    // ===========================================================================================
 
    private <R> Stream<R> thenMap(Function<T, R> fn) {
        return new Stream<>(flowable.map(fn));
    }

    /**
     * Continue the stream only if the condition is satisfied.
     * Otherwise finishes the entire stream early with the current value.
     */
    public <R> Stream<R> thenOnlyIf(Predicate<T> condition, Function<T, Stream<R>> mapping) {
        return new Stream<>(asFlowable().flatMap(value -> {
            if (condition.test(value)) {
                Stream<R> next = mapping.apply(value);
                if (next == null) {
                    return Flowable.error(new IllegalStateException("thenOnlyIf() mapping returned null Stream"));
                }
                return next.asFlowable().onErrorResumeNext(this::recoverFinish);
            }
            return Flowable.error(new Work.PipelineFinishException(value));
        }));
    }

    // ===========================================================================================
    // CHAINING — LEGACY
    // ===========================================================================================

    private <R> Stream<R> thenMap(
            Function<T, R> fn,
            Scheduler scheduler
    ) {
        return new Stream<>(
                flowable.observeOn(scheduler).map(fn)
        );
    }

    /** @deprecated Use semantic operators. Will be deleted in future releases. */
    @Deprecated
    public <R> Stream<R> on(
            WorkScheduler scheduler,
            ScopedFunctions.ThrowingFn<T, R> fn
    ) {
        return thenMap(fn, SchedulerResolver.resolve(scheduler));
    }

    /** @deprecated Use {@link #then(WorkScheduler, ScopedBoundaries.StreamScope)}. Will be deleted in future releases. */
    @Deprecated
    public <R> Stream<R> onAsync(
            WorkScheduler scheduler,
            ScopedBoundaries.StreamAsyncScope<T, R> fn
    ) {
        return then(scheduler, fn);
    }


    // ===========================================================================================
    // COMPOSITION — STREAM WORKFLOWS
    // ===========================================================================================

    /**
     * @deprecated Use {@link #then(ScopedBoundaries.StreamScope)}. Will be deleted in future releases.
     * <p>Compose another Stream workflow.</p>
     */
    @Deprecated
    public <R> Stream<R> compose(
            ScopedWorkflows.StreamWorkflow<T, R> wf
    ) {
        return new Stream<>(
                flowable.flatMap(value -> {
                    Stream<R> next = wf.apply(value);

                    if (next == null) {
                        return Flowable.error(
                                new IllegalStateException(
                                        "compose() workflow returned null Stream"
                                )
                        );
                    }

                    return next.asFlowable();
                })
        );
    }

    /** @deprecated Use {@link #then(WorkScheduler, ScopedBoundaries.StreamScope)}. Will be deleted in future releases. */
    @Deprecated
    public <R> Stream<R> composeOn(
            WorkScheduler scheduler,
            ScopedWorkflows.StreamWorkflow<T, R> wf
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Stream<>(
                flowable.observeOn(s)
                        .flatMap(value -> {
                            Stream<R> next = wf.apply(value);

                            if (next == null) {
                                return Flowable.error(
                                        new IllegalStateException(
                                                "composeOn() workflow returned null Stream"
                                        )
                                );
                            }

                            return next.asFlowable();
                        })
        );
    }

    // ===========================================================================================
    // GUARDS (CHAIN BREAKING)
    // ===========================================================================================

    /**
     * Continue only if condition is satisfied.
     * Otherwise fail the stream.
     */
    public Stream<T> require(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Throwable> errorSupplier
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Stream<>(
                flowable.observeOn(s)
                        .flatMap(value -> {
                            if (condition.test(value)) {
                                return Flowable.just(value);
                            }

                            Throwable error = java.util.Objects.requireNonNullElseGet(errorSupplier.apply(value),
                                    () -> new IllegalStateException("require() errorSupplier returned null"));
                            return Flowable.error(error);
                        })
        );
    }

    /**
     * Fail if condition is satisfied.
     */
    public Stream<T> reject(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Throwable> errorSupplier
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Stream<>(
                flowable.observeOn(s)
                        .flatMap(value -> {
                            if (condition.test(value)) {
                                Throwable error = java.util.Objects.requireNonNullElseGet(errorSupplier.apply(value),
                                        () -> new IllegalStateException("reject() errorSupplier returned null"));
                                return Flowable.error(error);
                            }

                            return Flowable.just(value);
                        })
        );
    }

    // ===========================================================================================
    // BRANCHING
    // ===========================================================================================

    /**
     * Branch stream based on condition.
     */
    public <R> Stream<R> branch(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Stream<R>> ifTrue,
            Function<T, Stream<R>> ifFalse
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Stream<>(
                flowable.observeOn(s)
                        .flatMap(value -> {
                            Stream<R> next =
                                    condition.test(value)
                                            ? ifTrue.apply(value)
                                            : ifFalse.apply(value);

                            if (next == null) {
                                return Flowable.error(
                                        new IllegalStateException(
                                                "branch() returned null Stream"
                                        )
                                );
                            }

                            return next.asFlowable();
                        })
        );
    }

    /**
     * @deprecated Use {@link #then(WorkScheduler, ScopedBoundaries.StreamScope)}. Will be deleted in future releases.
     * <p>Dynamic branch selection.</p>
     */
    @Deprecated
    public <R> Stream<R> switchOn(
            WorkScheduler scheduler,
            Function<T, Stream<R>> decision
    ) {
        return then(scheduler, decision::apply);
    }
    // ===========================================================================================
    // INTEROP
    // ===========================================================================================

    /**
     * @return the underlying RxJava {@link Flowable} for framework integration,
     * with any 'Finish' signal recovered into a normal success.
     */
    public Flowable<T> asTerminalFlowable() {
        return flowable.onErrorResumeNext(this::recoverFinish);
    }

    /**
     * @return the underlying RxJava {@link Flowable} for framework integration.
     * Note: This Flowable may fail with an internal PipelineFinishException if a step finished early.
     * Use {@link #asTerminalFlowable()} if you want a terminal Flowable.
     */
    public Flowable<T> asFlowable() {
        return flowable;
    }
    // ===========================================================================================
    // TERMINAL
    // ===========================================================================================

    public Disposable execute() {
        return flowable
                .onErrorResumeNext(this::recoverFinish)
                .subscribe(
                        t -> { },
                        throwable -> RxLog.e(TAG, "Unhandled Throwable", throwable)
                );
    }

    public Disposable executeOn(
            WorkScheduler scheduler,
            Consumer<T> onNext,
            Consumer<Throwable> onError
    ) {
        return flowable
                .onErrorResumeNext(this::recoverFinish)
                .observeOn(SchedulerResolver.resolve(scheduler))
                .subscribe(onNext, onError);
    }

    @SuppressWarnings("unchecked")
    private <R> Flowable<R> recoverFinish(Throwable e) {
        if (e instanceof Work.PipelineFinishException) {
            return Flowable.just((R) ((Work.PipelineFinishException) e).value);
        }
        return Flowable.error(e);
    }

    // ===========================================================================================
    // UTIL
    // ===========================================================================================

    private static <T> Flowable<T> mapErrors(Flowable<T> source) {
        return source.onErrorResumeNext(e ->
                Flowable.error(RxOnConfig.mapError(e))
        );
    }
}

