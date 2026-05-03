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
import com.benaether.rxon.scopes.ScopedBoundaries;
import com.benaether.rxon.scopes.ScopedFunctions;
import com.benaether.rxon.scopes.ScopedWorkflows;
import com.benaether.rxon.scopes.Unit;

import org.reactivestreams.Publisher;

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
 *   <li><b>No {@link Unit}</b>: side-effects belong in {@link Work}</li>
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
 */
public final class Stream<T> {

    private static final String TAG = Stream.class.getName();

    final Flowable<T> flowable;

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
    // ENTRY POINTS — ASYNC SOURCES
    // ===========================================================================================

    public static <T> Stream<T> onAsync(
            WorkScheduler scheduler,
            Flowable<T> source
    ) {
        return fromFlowable(source, SchedulerResolver.resolve(scheduler));
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
    // CHAINING — SYNC
    // ===========================================================================================

    private <R> Stream<R> thenMap(
            Function<T, R> fn,
            Scheduler scheduler
    ) {
        return new Stream<>(
                flowable.observeOn(scheduler).map(fn)
        );
    }

    public <R> Stream<R> on(
            WorkScheduler scheduler,
            ScopedFunctions.ThrowingFn<T, R> fn
    ) {
        return thenMap(fn, SchedulerResolver.resolve(scheduler));
    }

    // ===========================================================================================
    // CHAINING — ASYNC
    // ===========================================================================================

    private <R> Stream<R> thenFlatMap(
            Function<T, Publisher<R>> fn,
            Scheduler scheduler
    ) {
        return new Stream<>(
                flowable.observeOn(scheduler)
                        .flatMap(t -> {
                            Publisher<R> publisher = fn.apply(t);

                            if (publisher == null) {
                                return Flowable.error(
                                        new IllegalStateException(
                                                "onAsync() returned null Publisher"
                                        )
                                );
                            }

                            return mapErrors(Flowable.fromPublisher(publisher));
                        })
        );
    }

    public <R> Stream<R> onAsync(
            WorkScheduler scheduler,
            ScopedBoundaries.StreamAsyncScope<T, R> fn
    ) {
        return thenFlatMap(fn::apply, SchedulerResolver.resolve(scheduler));
    }


    // ===========================================================================================
    // COMPOSITION — STREAM WORKFLOWS
    // ===========================================================================================

    /**
     * Compose another Stream workflow without introducing a new scheduling boundary.
     *
     * <p>The current emission is passed directly to the provided workflow.</p>
     *
     * <p>No additional {@link WorkScheduler} is applied at this boundary.
     * The composed workflow controls its own execution context.</p>
     *
     * <p>This should be used when you want semantic composition only,
     * without altering threading behavior.</p>
     *
     * @param wf workflow to compose
     * @param <R> resulting stream type
     * @return composed Stream
     *
     * @throws IllegalStateException if the workflow returns null
     */
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

                    return next.flowable;
                })
        );
    }

    /**
     * <p>Compose another Stream workflow.</p>
     * <p>The workflow controls its own internal execution.</p>
     * <p>The composition boundary executes on the provided scheduler.</p>
     */
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

                            return next.flowable;
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

                            Throwable error = errorSupplier.apply(value);

                            if (error == null) {
                                return Flowable.error(
                                        new IllegalStateException(
                                                "require() errorSupplier returned null"
                                        )
                                );
                            }

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
                                Throwable error = errorSupplier.apply(value);

                                if (error == null) {
                                    return Flowable.error(
                                            new IllegalStateException(
                                                    "reject() errorSupplier returned null"
                                            )
                                    );
                                }

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

                            return next.flowable;
                        })
        );
    }

    /**
     * Dynamic branch selection.
     */
    public <R> Stream<R> switchOn(
            WorkScheduler scheduler,
            Function<T, Stream<R>> decision
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Stream<>(
                flowable.observeOn(s)
                        .flatMap(value -> {
                            Stream<R> next = decision.apply(value);

                            if (next == null) {
                                return Flowable.error(
                                        new IllegalStateException(
                                                "switchOn() returned null Stream"
                                        )
                                );
                            }

                            return next.flowable;
                        })
        );
    }

    // ===========================================================================================
    // TERMINAL
    // ===========================================================================================

    public Disposable execute() {
        return flowable.subscribe(
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
                .observeOn(SchedulerResolver.resolve(scheduler))
                .subscribe(onNext, onError);
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

