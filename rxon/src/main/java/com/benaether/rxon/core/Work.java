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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * {@code Work<T, C>} is a Lazy Orchestration Engine.
 * It defines a "Pipeline" of semantic stages that are compiled into an executable 
 * RxJava chain only when a terminal operator is invoked.
 */
public final class Work<T, C> {
    private static final String TAG = Work.class.getName();

    private final List<PipelineStage> stages;
    private final String tag;

    private Work(List<PipelineStage> stages) {
        this(stages, "Work");
    }

    private Work(List<PipelineStage> stages, String tag) {
        this.stages = Collections.unmodifiableList(stages);
        this.tag = tag;
    }

    private <R, NewC> Work<R, NewC> append(PipelineStage stage) {
        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        newStages.add(stage);
        return new Work<>(newStages, tag);
    }

    List<PipelineStage> getStages() {
        return stages;
    }

    String getTag() {
        return tag;
    }

    /**
     * Assign a semantic name to this Work instance for debugging purposes.
     */
    public Work<T, C> tag(String name) {
        return new Work<>(this.stages, name);
    }

    /**
     * Start a fresh pipeline with a lazy initial context state.
     */
    public static <C> Work<Done, C> withContext(Supplier<C> contextSupplier) {
        return new Work<Done, Object>(new ArrayList<>())
            .updateContext((ctx, val) -> contextSupplier.get());
    }

    /**
     * Set the context for subsequent stages based on the current value.
     */
    public <NewC> Work<T, NewC> usingContext(java.util.function.Function<T, NewC> mapper) {
        return updateContext((ctx, val) -> mapper.apply(val));
    }

    /**
     * Change the context type entirely.
     */
    public <NewC> Work<T, NewC> updateContext(BiFunction<C, T, NewC> mapper) {
        return thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val, mapper);
    }

    // Generic Entry Points

    public static Work<Done, Object> action(WorkScheduler scheduler, Runnable action) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.SyncStage(
            (ctx, val) -> { action.run(); return Done.INSTANCE; },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public static <T> Work<T, Object> callable(WorkScheduler scheduler, Callable<T> block) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.SyncStage(
            (ctx, val) -> {
                try { return block.call(); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public static <T> Work<T, Object> single(WorkScheduler scheduler, Single<T> single) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.AsyncStage(
            (ctx, val) -> single.map(v -> (Object) v),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public static <T> Work<T, Object> maybe(WorkScheduler scheduler, Maybe<T> maybe) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.AsyncStage(
            (ctx, val) -> maybe.map(v -> (Object) v).toSingle(),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public static Work<Done, Object> completable(WorkScheduler scheduler, Completable completable) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.AsyncStage(
            (ctx, val) -> completable.toSingle(() -> Done.INSTANCE),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    // Generic Chaining Operators

    public Work<Done, C> thenAction(WorkScheduler scheduler, Runnable action) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> { action.run(); return Done.INSTANCE; },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public <R> Work<R, C> thenCallable(WorkScheduler scheduler, Callable<R> block) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> {
                try { return block.call(); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    public <R> Work<R, C> thenFunction(WorkScheduler scheduler, BiFunction<C, T, R> task) {
        return thenFunction(scheduler, task, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <R, NewC> Work<R, NewC> thenFunction(WorkScheduler scheduler, BiFunction<C, T, R> task, BiFunction<C, R, NewC> contextMapper) {
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
    public Work<T, C> thenConsumer(WorkScheduler scheduler, BiConsumer<C, T> action) {
        return append(new PipelineStage.SyncStage(
            (ctx, val) -> { action.accept((C) ctx, (T) val); return val; },
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public <R> Work<R, C> thenSingle(WorkScheduler scheduler, BiFunction<C, T, Single<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> (Single<Object>) (Single<?>) mapper.apply((C) ctx, (T) val),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public <R> Work<R, C> thenMaybe(WorkScheduler scheduler, BiFunction<C, T, Maybe<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> (Single<Object>) (Single<?>) mapper.apply((C) ctx, (T) val).toSingle(),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    @SuppressWarnings("unchecked")
    public Work<Done, C> thenCompletable(WorkScheduler scheduler, BiFunction<C, T, Completable> mapper) {
        return append(new PipelineStage.AsyncStage(
            (ctx, val) -> mapper.apply((C) ctx, (T) val).toSingle(() -> Done.INSTANCE),
            (ctx, res) -> ctx,
            scheduler
        ));
    }

    // Control Flow & Short-Circuiting

    /**
     * If the condition is met, terminates the pipeline early with the given default value.
     */
    @SuppressWarnings("unchecked")
    public Work<T, C> breakIf(Predicate<T> condition, T defaultValue) {
        return append(new PipelineStage.ConditionalBreakStage(
            val -> condition.test((T) val),
            defaultValue
        ));
    }

    /**
     * If the condition is met, fails the pipeline with the provided error.
     */
    @SuppressWarnings("unchecked")
    public Work<T, C> failIf(Predicate<T> condition, Throwable error) {
        return append(new PipelineStage.ConditionalFailStage(
            val -> condition.test((T) val),
            error
        ));
    }

    /**
     * Side-effect that runs on the current scheduler without modifying value or context.
     */
    public Work<T, C> peek(java.util.function.Consumer<T> action) {
        return thenConsumer(WorkScheduler.COMPUTE, (ctx, val) -> action.accept(val));
    }

    // Debugging & Logging

    /**
     * Declarative stage to log the current pipeline state.
     */
    public Work<T, C> log(LogLevel level) {
        return log(level, null);
    }

    /**
     * Declarative stage to log the current pipeline state with a custom message.
     */
    public Work<T, C> log(LogLevel level, String message) {
        return append(new PipelineStage.LogStage(level, message));
    }

    // Static Composition

    public static <T> Work<T, Object> breakWork(T value) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.BreakStage(value));
    }

    public static <T> Work<T, Object> continueWork(T value) {
        return callable(WorkScheduler.COMPUTE, () -> value);
    }

    public static <T> Work<T, Object> fail(Throwable error) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.FailStage(error));
    }

    @SuppressWarnings("unchecked")
    public <R> Work<R, C> then(Work<R, ?> other) {
        List<PipelineStage> newStages = new java.util.ArrayList<>(this.stages);
        newStages.addAll(other.getStages());
        return new Work<>(newStages, tag);
    }

    @SuppressWarnings("unchecked")
    public <R> Work<R, C> thenChain(BiFunction<C, T, Work<R, C>> task) {
        return append(new PipelineStage.ChainStage(
            (ctx, val) -> task.apply((C) ctx, (T) val)
        ));
    }

    public <R> Work<R, C> thenChain(java.util.function.Function<T, Work<R, C>> task) {
        return thenChain((ctx, val) -> task.apply(val));
    }

    @SuppressWarnings("unchecked")
    public Work<T, C> peekChain(BiFunction<C, T, Work<?, C>> task) {
        return thenChain((ctx, val) -> {
            Work<?, C> sub = task.apply((C) ctx, (T) val);
            List<PipelineStage> newStages = new java.util.ArrayList<>(sub.getStages());
            newStages.add(new PipelineStage.BreakStage(val));
            return new Work<>(newStages, tag);
        });
    }

    @SuppressWarnings("unchecked")
    public <U, R> Work<R, C> zipWith(Work<U, ?> other, BiFunction<T, U, R> zipper) {
        return append(new PipelineStage.ZipStage(
            other,
            (v1, v2) -> zipper.apply((T) v1, (U) v2)
        ));
    }

    // Specialized Operators

    public Work<T, C> require(Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenFunction(WorkScheduler.COMPUTE, (ctx, t) -> {
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

    public Work<T, C> reject(Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenFunction(WorkScheduler.COMPUTE, (ctx, t) -> {
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

    // Resilience & Recovery

    public Work<T, C> recover(java.util.function.Function<Throwable, T> fallback) {
        return recover(Throwable.class, fallback);
    }

    public Work<T, C> recover(Class<? extends Throwable> type, java.util.function.Function<Throwable, T> fallback) {
        return append(new PipelineStage.RecoverStage(type, err -> fallback.apply(err)));
    }

    public Work<T, C> retry(int max, long delay, ResiliencePolicy.BackoffStrategy strategy) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                new ResiliencePolicy.RetryPolicy(max, delay, strategy),
                m.timeout(), m.fallback(), m.compensation()
        ));
    }

    public Work<T, C> timeout(long duration, java.util.concurrent.TimeUnit unit) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(),
                new ResiliencePolicy.TimeoutPolicy(duration, unit),
                m.fallback(), m.compensation()
        ));
    }

    public Work<T, C> fallback(Work<T, C> fallbackWork) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(), m.timeout(), fallbackWork, m.compensation()
        ));
    }

    public Work<T, C> compensate(Work<Done, ?> compensationWork) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(), m.timeout(), m.fallback(), compensationWork
        ));
    }

    private Work<T, C> updateLastResilience(java.util.function.Function<ResiliencePolicy.ResilienceMetadata, ResiliencePolicy.ResilienceMetadata> updater) {
        if (stages.isEmpty()) return this;

        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        PipelineStage last = newStages.remove(newStages.size() - 1);

        ResiliencePolicy.ResilienceMetadata newM = updater.apply(last.resilience());

        PipelineStage updated;
        if (last instanceof PipelineStage.SyncStage s) {
            updated = new PipelineStage.SyncStage(s.task(), s.contextMapper(), s.scheduler(), newM);
        } else if (last instanceof PipelineStage.AsyncStage s) {
            updated = new PipelineStage.AsyncStage(s.task(), s.contextMapper(), s.scheduler(), newM);
        } else if (last instanceof PipelineStage.ChainStage s) {
            updated = new PipelineStage.ChainStage(s.task(), newM);
        } else if (last instanceof PipelineStage.RecoverStage s) {
            updated = new PipelineStage.RecoverStage(s.type(), s.fallback(), newM);
        } else if (last instanceof PipelineStage.ConditionalBreakStage s) {
            updated = new PipelineStage.ConditionalBreakStage(s.condition(), s.defaultValue(), newM);
        } else if (last instanceof PipelineStage.ConditionalFailStage s) {
            updated = new PipelineStage.ConditionalFailStage(s.condition(), s.error(), newM);
        } else if (last instanceof PipelineStage.ZipStage s) {
            updated = new PipelineStage.ZipStage(s.other(), s.zipper(), newM);
        } else {
            updated = last;
        }

        newStages.add(updated);
        return new Work<>(newStages, tag);
    }

    // Breaking & Recovery

    public Work<T, C> thenBreak(T value) {
        return append(new PipelineStage.BreakStage(value));
    }

    public <R> Work<R, C> thenContinue(R value) {
        return thenCallable(WorkScheduler.COMPUTE, () -> value);
    }

    public Work<T, C> recoverBreak(T recoveryValue) {
        return append(new PipelineStage.RecoverBreakStage(recoveryValue));
    }

    // Terminal Operators

    public Single<T> asTerminalSingle() {
        return PipelineCompiler.compile(stages, tag, () -> null);
    }

    public Disposable execute() {
        return asTerminalSingle().subscribe(
                t -> { },
                throwable -> RxLog.e(TAG, "Unhandled Throwable", throwable)
        );
    }

    public Disposable executeOn(WorkScheduler workScheduler, Consumer<T> onSuccess, Consumer<Throwable> onError) {
        return asTerminalSingle()
                .observeOn(SchedulerResolver.resolve(workScheduler))
                .subscribe(onSuccess, onError);
    }
}
