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

import java.util.concurrent.Callable;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * {@code Work<T>} is a lightweight, opinionated DSL built on top of RxJava {@link Single}
 * for modeling synchronous and asynchronous units of work with explicit, semantic
 * threading and composition rules.
 *
 * <p>This type represents a <b>single, terminal computation</b> that emits exactly one
 * non-null value or an error.</p>
 *
 * <h2>Core principles</h2>
 *
 * <ul>
 *   <li>
 *     <b>Exactly one value</b>:
 *     {@code Work<T>} always emits exactly one value or fails
 *     (it is backed by {@link Single}).
 *   </li>
 *
 *   <li>
 *     <b>No null values</b>:
 *     Returning {@code null} from any transformation is illegal and will result
 *     in a {@link NullPointerException} thrown by RxJava.
 *   </li>
 *
 *   <li>
 *     <b>Explicit threading</b>:
 *     All execution happens on an explicitly provided {@link WorkScheduler}.
 *     Callers never manipulate raw {@link io.reactivex.rxjava3.core.Scheduler}s.
 *   </li>
 *
 *   <li>
 *     <b>Semantic composition</b>:
 *     Workflows encapsulate their own execution logic and threading.
 *     Callers only compose them; they never override internal schedulers.
 *   </li>
 * </ul>
 *
 * <h2>Nullability rules (CRITICAL)</h2>
 *
 * <p><b>Never return {@code null}</b> from any {@code Work} chain, including:</p>
 *
 * <ul>
 *   <li>Sync chaining ({@code on(...)})</li>
 *   <li>Async chaining ({@code onAsync(...)})</li>
 *   <li>Workflow composition</li>
 *   <li>Branching logic</li>
 * </ul>
 *
 * <p>RxJava does not permit {@code null} emissions. Violations will crash with:</p>
 *
 * <pre>{@code
 * NullPointerException: The mapper function returned a null value
 * }</pre>
 *
 * <h3>When absence of a value is valid</h3>
 *
 * <ul>
 *   <li>Return {@link java.util.Optional Optional&lt;T&gt;}</li>
 *   <li>Or return {@link Unit} for no-value workflows</li>
 * </ul>
 *
 * <pre>{@code
 * // CORRECT
 * work.on(COMPUTE, value -> Optional.ofNullable(value));
 *
 * // INCORRECT (will crash)
 * work.on(COMPUTE, value -> null);
 * }</pre>
 *
 * <h3>{@code Work<Void>} warning</h3>
 *
 * <p>{@code Void} has no valid value and cannot be emitted safely.</p>
 *
 * <ul>
 *   <li>Returning {@code null} will crash</li>
 *   <li>{@code Work<Void>} should be avoided</li>
 * </ul>
 *
 * <p>Always prefer {@link Unit} for workflows that conceptually return no value.</p>
 *
 * <h2>Branching &amp; conditional flow</h2>
 *
 * <p>{@code Work} supports explicit, first-class branching and early termination:</p>
 *
 * <ul>
 *   <li>
 *     <b>Conditional branching</b> via {@code branch(...)} and {@code branchAsync(...)}
 *   </li>
 *   <li>
 *     <b>Early termination without failure</b> via {@code haltIf(...)}
 *   </li>
 *   <li>
 *     <b>Fail-fast termination</b> via {@code failIf(...)}
 *   </li>
 * </ul>
 *
 * <p>This enables a Railway-Oriented Programming style where:</p>
 *
 * <ul>
 *   <li>Success continues the chain</li>
 *   <li>Failures stop execution immediately</li>
 *   <li>Errors are handled as early as possible</li>
 * </ul>
 *
 * <h2>Execution</h2>
 *
 * <ul>
 *   <li>
 *     {@link #executeOn(WorkScheduler, Consumer, Consumer)} executes the chain
 *     and delivers terminal callbacks on the provided scheduler.
 *   </li>
 * </ul>
 *
 * <h2>Encapsulation rule</h2>
 *
 * <p>
 * RxJava primitives ({@link Single}, {@link io.reactivex.rxjava3.core.Scheduler}, etc.)
 * are <b>never exposed</b> outside infrastructure boundaries.
 * </p>
 *
 * <p>
 * All creation and composition must go through semantic DSL entry points
 * such as {@code on(...)}, {@code onAsync(...)}, and workflows.
 * </p>
 *
 * <h2>Intended usage</h2>
 *
 * <ul>
 *   <li>Repositories and services return {@code Work<T>}</li>
 *   <li>ViewModels compose {@code Work}</li>
 *   <li>UI layers execute and observe results</li>
 * </ul>
 *
 * @param <T> emission type
 */
public final class Work<T> {
    private static final String TAG = Work.class.getName();

    final Single<T> single;

    Work(Single<T> single) {
        this.single = single;
    }

    // ===========================================================================================
    // INTERNAL PRIMITIVE BUILDERS
    // ===========================================================================================

    private static <T> Work<T> fromCallable(Callable<T> task, Scheduler scheduler) {
        return new Work<>(Single.fromCallable(task).subscribeOn(scheduler));
    }

    private static <T> Work<T> fromSingle(Single<T> source, Scheduler scheduler) {
        return new Work<>(mapErrors(source).subscribeOn(scheduler));
    }

    // ===========================================================================================
    // ENTRY POINTS — SYNC
    // ===========================================================================================

    public static <T> Work<T> on(WorkScheduler workScheduler, Callable<T> task) {
        return fromCallable(task, SchedulerResolver.resolve(workScheduler));
    }

    // ===========================================================================================
    // ENTRY POINTS — ASYNC
    // ===========================================================================================

    public static <T> Work<T> onAsync(WorkScheduler workScheduler, Single<T> source) {
        return fromSingle(source, SchedulerResolver.resolve(workScheduler));
    }

    public static Work<Unit> onAsync(WorkScheduler workScheduler, Completable source) {
        return fromSingle(source.toSingleDefault(Unit.INSTANCE),
                SchedulerResolver.resolve(workScheduler));
    }

    // ===========================================================================================
    // POINTS — UNIT (NO RETURN VALUE)
    // ===========================================================================================

    public static Work<Unit> onUnit(WorkScheduler workScheduler, Runnable task) {
        return on(workScheduler,
                () -> {
                    task.run();
                    return Unit.INSTANCE;
                });
    }

    // ===========================================================================================
    // ENTRY POINTS — WORKFLOWS
    // ===========================================================================================

    public static <I, O> Work<O> fromWorkflow(ScopedWorkflows.Workflow<I, O> wf, I input) {
        return wf.apply(input);
    }

    public static <O> Work<O> fromWorkflow(ScopedWorkflows.Workflow0<O> wf) {
        return wf.apply();
    }

    // ===========================================================================================
    // CHAINING — SYNC
    // ===========================================================================================

    private <R> Work<R> thenMap(Function<T, R> fn, Scheduler scheduler) {
        return new Work<>(single.observeOn(scheduler).map(fn));
    }

    public <R> Work<R> on(WorkScheduler workScheduler, ScopedFunctions.ThrowingFn<T, R> fn) {
        return thenMap(fn, SchedulerResolver.resolve(workScheduler));
    }

    // ===========================================================================================
    // CHAINING — ASYNC
    // ===========================================================================================

    private <R> Work<R> thenFlatMap(Function<T, Single<R>> fn, Scheduler scheduler) {
        return new Work<>(single.observeOn(scheduler)
                .flatMap(t -> mapErrors(fn.apply(t))));
    }

    public <R> Work<R> onAsync(WorkScheduler workScheduler,
                               ScopedBoundaries.AsyncScope<T, R> fn) {
        return thenFlatMap(fn::apply, SchedulerResolver.resolve(workScheduler));
    }


    // ===========================================================================================
    // UNIT CHAINING (SYNC SIDE EFFECTS)
    // ===========================================================================================

    public Work<Unit> onUnit(WorkScheduler workScheduler, ScopedFunctions.ThrowingUnitFn<T> fn) {
        return thenMap(t -> {
            fn.apply(t);
            return Unit.INSTANCE;
        }, SchedulerResolver.resolve(workScheduler));
    }

    // ===========================================================================================
    // CHAINING — UNIT (ASYNC SIDE EFFECTS)
    // ===========================================================================================

    public Work<Unit> onAsyncUnit(WorkScheduler workScheduler, ScopedBoundaries.AsyncScope<T,
            ?> fn) {
        return thenFlatMap(t ->
                        fn.apply(t).map(ignored -> Unit.INSTANCE),
                SchedulerResolver.resolve(workScheduler)
        );
    }

    public Work<Unit> onAsyncCompletable(WorkScheduler workScheduler,
                                         ScopedBoundaries.AsyncCompletableScope<T> fn) {
        return thenFlatMap(t ->
                        fn.apply(t).toSingleDefault(Unit.INSTANCE),
                SchedulerResolver.resolve(workScheduler)
        );
    }

    // ===========================================================================================
    // Scoped COMPOSITION — WORKFLOWS INPUT/NO-INPUT
    // ===========================================================================================

    /**
     * <p>composition with no extra logic, because otherwise the logic will be executed on the last scheduler</p>
     */
    public <R> Work<R> compose(ScopedWorkflows.Workflow<T, R> wf) {
        return new Work<>(single
                .flatMap(t -> wf.apply(t).single));
    }

    /**
     * <p>composition with no extra logic, because otherwise the logic will be executed on the last scheduler</p>
     */
    public <R> Work<R> compose(ScopedWorkflows.Workflow0<R> wf) {
        return new Work<>(single
                .flatMap(ignored -> wf.apply().single));
    }

    /**
     * <p>Compose another Workflow.</p>
     * <p>The workflow controls its own internal execution.</p>
     * <p>The composition boundary executes on the provided scheduler.</p>
     */
    public <R> Work<R> composeOn(WorkScheduler workScheduler, ScopedWorkflows.Workflow<T, R> wf) {
        return new Work<>(single
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .flatMap(t -> wf.apply(t).single));
    }

    /**
     * <p>Compose another Workflow.</p>
     * <p>The workflow controls its own internal execution.</p>
     * <p>The composition boundary executes on the provided scheduler.</p>
     */
    public <R> Work<R> composeOn(WorkScheduler workScheduler, ScopedWorkflows.Workflow0<R> wf) {
        return new Work<>(single
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .flatMap(ignored -> wf.apply().single));
    }

    // ===========================================================================================
    // GUARDS (CHAIN BREAKING)
    // ===========================================================================================

    /**
     * Continue only if condition is satisfied.
     * Otherwise fail the chain with supplied error.
     */
    public Work<T> require(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Throwable> errorSupplier
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Work<>(
                single.observeOn(s)
                        .flatMap(value -> {
                            if (condition.test(value)) {
                                return Single.just(value);
                            }

                            Throwable error = errorSupplier.apply(value);
                            if (error == null) {
                                return Single.error(
                                        new IllegalStateException("require() errorSupplier returned null")
                                );
                            }

                            return Single.error(error);
                        })
        );
    }


    /**
     * Fail if condition is satisfied.
     * Otherwise continue normally.
     */
    public Work<T> reject(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Throwable> errorSupplier
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Work<>(
                single.observeOn(s)
                        .flatMap(value -> {
                            if (condition.test(value)) {
                                Throwable error = errorSupplier.apply(value);

                                if (error == null) {
                                    return Single.error(
                                            new IllegalStateException("reject() errorSupplier returned null")
                                    );
                                }

                                return Single.error(error);
                            }

                            return Single.just(value);
                        })
        );
    }

    /**
     * Continue if condition satisfied.
     * Otherwise halt gracefully by replacing the value.
     * <p>
     * NOTE:
     * fallback must never return null.
     */
    public Work<T> requireOrHalt(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, T> fallback
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Work<>(
                single.observeOn(s)
                        .flatMap(value -> {
                            if (condition.test(value)) {
                                return Single.just(value);
                            }

                            T newValue = fallback.apply(value);

                            if (newValue == null) {
                                return Single.error(
                                        new IllegalStateException("requireOrHalt() fallback returned null")
                                );
                            }

                            return Single.just(newValue);
                        })
        );
    }


    // ===========================================================================================
    // BRANCHING (IF / ELSE)
    // ===========================================================================================

    /**
     * Branch the chain into two possible Work paths.
     * <p>
     * If condition is true  -> execute ifTrue
     * If condition is false -> execute ifFalse
     * <p>
     * Both branches must return non-null Work.
     */
    public <R> Work<R> branch(
            WorkScheduler scheduler,
            Predicate<T> condition,
            Function<T, Work<R>> ifTrue,
            Function<T, Work<R>> ifFalse
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Work<>(
                single.observeOn(s)
                        .flatMap(value -> {
                            Work<R> next =
                                    condition.test(value)
                                            ? ifTrue.apply(value)
                                            : ifFalse.apply(value);

                            if (next == null) {
                                return Single.error(
                                        new IllegalStateException("branch() returned null Work")
                                );
                            }

                            return next.single;
                        })
        );
    }

    // ===========================================================================================
    // BRANCHING — CONTROL FLOW
    // ===========================================================================================

    /**
     * Perform controlled branching inside a Work chain.
     *
     * <p>This operator allows dynamic selection of the next {@link Work} to execute
     * based on the current value.</p>
     *
     * <p><b>Semantics</b></p>
     *
     * <ul>
     *   <li>The provided {@code decision} function must return a non-null {@code Work}.</li>
     *   <li>The returned {@code Work} determines how the chain continues.</li>
     *   <li>If the returned Work fails, the entire chain fails.</li>
     *   <li>If the returned Work completes normally, the chain continues with its result.</li>
     * </ul>
     *
     * <p><b>Threading</b></p>
     *
     * <p>The branching decision itself runs on the provided {@link WorkScheduler}.
     * The returned {@code Work} controls its own internal execution context.</p>
     *
     * <p><b>Typical use cases</b></p>
     *
     * <ul>
     *   <li>Conditional workflow routing</li>
     *   <li>Replacing nested flatMap pyramids</li>
     *   <li>Guard-based branching</li>
     *   <li>Early exit (via {@link #halt(Object)} or {@link #fail(Throwable)})</li>
     * </ul>
     *
     * <p><b>Example</b></p>
     *
     * <pre>{@code
     * Work.onAsync(IO, api.getUser())
     *     .switchOn(COMPUTE, user -> {
     *         if (!user.isActive()) {
     *             return Work.fail(new IllegalStateException("Inactive user"));
     *         }
     *
     *         if (user.isGuest()) {
     *             return Work.halt(defaultProfile());
     *         }
     *
     *         return loadFullProfile(user);
     *     });
     * }</pre>
     *
     * @param scheduler execution context for the branching decision
     * @param decision function that returns the next Work to execute
     * @param <R> result type of the next Work
     * @return new Work representing the selected branch
     *
     * @throws IllegalStateException if the decision function returns null
     */
    public <R> Work<R> switchOn(
            WorkScheduler scheduler,
            Function<T, Work<R>> decision
    ) {
        Scheduler s = SchedulerResolver.resolve(scheduler);

        return new Work<>(
                single.observeOn(s)
                        .flatMap(value -> {
                            Work<R> next = decision.apply(value);

                            if (next == null) {
                                return Single.error(
                                        new IllegalStateException(
                                                "switchOn() decision returned null Work"
                                        )
                                );
                            }

                            return next.single;
                        })
        );
    }

    /**
     * Immediately terminate the current branch successfully with a value.
     *
     * <p>This is a semantic early-return mechanism.</p>
     *
     * <p><b>Behavior</b></p>
     * <ul>
     *   <li>No further downstream operations in the original branch execute.</li>
     *   <li>The chain continues from this returned Work.</li>
     *   <li>No error is thrown.</li>
     * </ul>
     *
     * <p><b>Important</b></p>
     * <p>The value must not be null.</p>
     *
     * <p><b>Example</b></p>
     *
     * <pre>{@code
     * if (cacheHit) {
     *     return Work.halt(cachedValue);
     * }
     * }</pre>
     *
     * @param value non-null value to emit
     * @param <T> type of value
     * @return Work that immediately emits the value
     */
    public static <T> Work<T> halt(T value) {
        if (value == null) {
            return new Work<>(Single.error(
                    new NullPointerException("halt() value cannot be null")
            ));
        }

        return new Work<>(Single.just(value));
    }

    /**
     * Immediately terminate the current branch with an error.
     *
     * <p>This is a semantic failure mechanism used inside branching logic.</p>
     *
     * <p><b>Behavior</b></p>
     * <ul>
     *   <li>Stops further execution of the current chain.</li>
     *   <li>Propagates the error downstream.</li>
     *   <li>Will be delivered to the terminal {@code executeOn(...)} error handler.</li>
     * </ul>
     *
     * <p><b>Example</b></p>
     *
     * <pre>{@code
     * if (!authorized) {
     *     return Work.fail(new UnauthorizedException());
     * }
     * }</pre>
     *
     * @param error non-null Throwable
     * @param <T> expected type of Work
     * @return Work that fails immediately
     */
    public static <T> Work<T> fail(Throwable error) {
        if (error == null) {
            return new Work<>(Single.error(
                    new NullPointerException("fail() error cannot be null")
            ));
        }

        return new Work<>(Single.error(error));
    }






    // ===========================================================================================
    // TERMINAL
    // ===========================================================================================

    public Disposable execute() {
        return single.subscribe(
                t -> { },
                throwable -> RxLog.e(TAG, "Unhandled Throwable", throwable)
        );
    }

    public Disposable executeOn(WorkScheduler workScheduler, Consumer<T> onSuccess,
                                Consumer<Throwable> onError) {
        return single
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .subscribe(onSuccess, onError);
    }

    public Disposable executeResOn(WorkScheduler workScheduler, Consumer<T> onSuccess) {
        return single
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .subscribe(
                        onSuccess,
                        throwable -> RxLog.e(TAG, "Unhandled Throwable", throwable)
                );
    }

    public Disposable executeErrOn(WorkScheduler workScheduler, Consumer<Throwable> onError) {
        return single
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .subscribe(t -> { }, onError);
    }

    // ===========================================================================================
    // UTIL
    // ===========================================================================================

    private static <T> Single<T> mapErrors(Single<T> source) {
        return source.onErrorResumeNext(e ->
                Single.error(RxOnConfig.mapError(e)));
    }

}

