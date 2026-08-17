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
import java.util.function.Function;
import java.util.function.Predicate;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;

/**
 * {@code Work<T, C>} is a lazy signal orchestration engine. 
 * It remains idle until a terminal operator is invoked.
 */
public final class Work<T> {

    private static final String TAG = Work.class.getName();

    private final List<PipelineStage> stages;
    private final String tag;
    
    Work(List<PipelineStage> stages) {
        this(stages, "Work");
    }

    Work(List<PipelineStage> stages, String tag) {
        this.stages = Collections.unmodifiableList(stages);
        this.tag = tag;
    }

    List<PipelineStage> getStages() {
        return stages;
    }

    String getTag() {
        return tag;
    }

    @SuppressWarnings("unchecked")
    private <R> Work<R> append(PipelineStage stage) {
        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        newStages.add(stage);
        return new Work<>(newStages, tag);
    }

    /**
     * Assign a semantic name to this Work instance for debugging purposes.
     */
    public Work<T> tag(String name) {
        return new Work<>(this.stages, name);
    }

    public static <T> Work<T> just(WorkScheduler scheduler, T value) {
        return new Work<Object>(new ArrayList<>(), "Work").thenCallable(scheduler, () -> value);
    }

    public static Work<Done> action(WorkScheduler scheduler, Runnable action) {
        return new Work<Object>(new ArrayList<>(), "Work").thenAction(scheduler, action);
    }

    public static <R> Work<R> callable(WorkScheduler scheduler, Callable<R> block) {
        return new Work<Object>(new ArrayList<>(), "Work").thenCallable(scheduler, block);
    }

    public static <R> Work<R> single(WorkScheduler scheduler, Single<R> source) {
        return new Work<Object>(new ArrayList<>(), "Work").thenSingle(scheduler, val -> source);
    }

    public static <R> Work<R> maybe(WorkScheduler scheduler, Maybe<R> source) {
        return new Work<Object>(new ArrayList<>(), "Work").thenMaybe(scheduler, val -> source);
    }

    public static Work<Done> completable(WorkScheduler scheduler, Completable source) {
        return new Work<Object>(new ArrayList<>(), "Work").thenCompletable(scheduler, val -> source);
    }

    public static Work<Done> completableEmitter(WorkScheduler scheduler, io.reactivex.rxjava3.functions.Consumer<io.reactivex.rxjava3.core.CompletableEmitter> body) {
        return Work.completable(scheduler, Completable.create(rawEmitter -> {
            io.reactivex.rxjava3.core.CompletableEmitter emitter = new io.reactivex.rxjava3.core.CompletableEmitter() {
                @Override
                public void onComplete() {
                    rawEmitter.onComplete();
                }

                @Override
                public void onError(Throwable t) {
                    rawEmitter.onError(RxOnConfig.mapError(t));
                }

                @Override
                public boolean tryOnError(Throwable t) {
                    return rawEmitter.tryOnError(RxOnConfig.mapError(t));
                }

                @Override
                public void setDisposable(io.reactivex.rxjava3.disposables.Disposable d) {
                    rawEmitter.setDisposable(d);
                }

                @Override
                public void setCancellable(io.reactivex.rxjava3.functions.Cancellable c) {
                    rawEmitter.setCancellable(c);
                }

                @Override
                public boolean isDisposed() {
                    return rawEmitter.isDisposed();
                }
            };

            try {
                body.accept(emitter);
            } catch (Throwable t) {
                if (!emitter.isDisposed()) {
                    emitter.onError(t);
                }
            }
        }));
    }

    public static <R> Work<R> singleEmitter(WorkScheduler scheduler, io.reactivex.rxjava3.functions.Consumer<io.reactivex.rxjava3.core.SingleEmitter<R>> body) {
        return Work.single(scheduler, Single.create(rawEmitter -> {
            io.reactivex.rxjava3.core.SingleEmitter<R> emitter = new io.reactivex.rxjava3.core.SingleEmitter<R>() {
                @Override
                public void onSuccess(R value) {
                    rawEmitter.onSuccess(value);
                }

                @Override
                public void onError(Throwable t) {
                    rawEmitter.onError(RxOnConfig.mapError(t));
                }

                @Override
                public boolean tryOnError(Throwable t) {
                    return rawEmitter.tryOnError(RxOnConfig.mapError(t));
                }

                @Override
                public void setDisposable(io.reactivex.rxjava3.disposables.Disposable d) {
                    rawEmitter.setDisposable(d);
                }

                @Override
                public void setCancellable(io.reactivex.rxjava3.functions.Cancellable c) {
                    rawEmitter.setCancellable(c);
                }

                @Override
                public boolean isDisposed() {
                    return rawEmitter.isDisposed();
                }
            };

            try {
                body.accept(emitter);
            } catch (Throwable t) {
                if (!emitter.isDisposed()) {
                    emitter.onError(t);
                }
            }
        }));
    }

    public static Work<Long> timer(WorkScheduler scheduler, long delay, java.util.concurrent.TimeUnit unit) {
        return Work.single(scheduler, Single.timer(delay, unit));
    }

    public static Work<Done> consumer(WorkScheduler scheduler, java.util.function.Consumer<Object> action) {
        return new Work<Object>(new ArrayList<>(), "Work").thenAction(scheduler, () -> action.accept(null));
    }

    /**
     * Sequentially executes a work segment for each item in the provided iterable.
     */
    public static <I, R> Work<Done> forEach(WorkScheduler scheduler, Iterable<I> items, Function<I, Work<R>> task) {
        return Work.completable(scheduler, 
            Observe.iterable(scheduler, items)
                .thenChain(task)
                .asFlowable()
                .ignoreElements()
        );
    }

    /**
     * Executes all provided work segments in parallel and returns a list of results.
     */
    @SuppressWarnings("unchecked")
    public static <T> Work<List<T>> allOf(WorkScheduler scheduler, List<Work<T>> works) {
        if (works.isEmpty()) return Work.just(scheduler, java.util.Collections.emptyList());
        
        java.util.List<io.reactivex.rxjava3.core.Single<T>> singles = new java.util.ArrayList<>();
        for (Work<T> work : works) {
            singles.add(work.asTerminalSingle());
        }

        return Work.single(scheduler, io.reactivex.rxjava3.core.Single.zip(singles, objects -> {
            java.util.List<T> results = new java.util.ArrayList<>();
            for (Object obj : objects) results.add((T) obj);
            return results;
        }));
    }

    @SuppressWarnings("unchecked")
    public static <R> Work<R> fail(Throwable error) {
        return (Work<R>) (Work<?>) new Work<Object>(new ArrayList<>()).append(new PipelineStage.FailStage(error));
    }

    public Work<Done> thenAction(WorkScheduler scheduler, Runnable action) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> { action.run(); return Done.INSTANCE; },
            scheduler
        ));
    }

    public <R> Work<R> thenAction(WorkScheduler scheduler, Runnable action, BiFunction<T, Done, R> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> { action.run(); return merger.apply((T) val, Done.INSTANCE); },
            scheduler
        ));
    }

    public <R> Work<R> thenCallable(WorkScheduler scheduler, Callable<R> block) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { return block.call(); }
                catch (Exception e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public <R> Work<R> thenFunction(WorkScheduler scheduler, Function<T, R> task) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> task.apply((T) val),
            scheduler
        ));
    }

    public <R, U> Work<U> thenFunction(WorkScheduler scheduler, Function<T, R> task, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> merger.apply((T) val, task.apply((T) val)),
            scheduler
        ));
    }

    public Work<T> thenConsumer(WorkScheduler scheduler, Consumer<T> action) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { action.accept((T) val); return val; }
                catch (Throwable e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public <R> Work<R> thenConsumer(WorkScheduler scheduler, Consumer<T> action, BiFunction<T, T, R> merger) {
        return append(new PipelineStage.SyncStage(
            (val, ignored) -> {
                try { action.accept((T) val); return merger.apply((T) val, (T) val); }
                catch (Throwable e) { throw new RuntimeException(e); }
            },
            scheduler
        ));
    }

    public Work<T> doOnNext(WorkScheduler scheduler, Consumer<T> action) {
        return thenConsumer(scheduler, action);
    }

    public <R> Work<R> thenSingle(WorkScheduler scheduler, Function<T, Single<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> (Single<Object>) mapper.apply((T) val),
            scheduler
        ));
    }

    public <R, U> Work<U> thenSingle(WorkScheduler scheduler, Function<T, Single<R>> mapper, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    public <R> Work<R> thenMaybe(WorkScheduler scheduler, Function<T, Maybe<R>> mapper) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> (Single<Object>) (Single<?>) mapper.apply((T) val).toSingle(),
            scheduler
        ));
    }

    public <R, U> Work<U> thenMaybe(WorkScheduler scheduler, Function<T, Maybe<R>> mapper, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).toSingle().map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    public Work<Done> thenCompletable(WorkScheduler scheduler, Function<T, Completable> mapper) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).toSingle(() -> Done.INSTANCE),
            scheduler
        ));
    }

    public <R> Work<R> thenCompletable(WorkScheduler scheduler, Function<T, Completable> mapper, BiFunction<T, Done, R> merger) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> mapper.apply((T) val).toSingle(() -> Done.INSTANCE).map(res -> merger.apply((T) val, res)),
            scheduler
        ));
    }

    public <R> Work<R> thenChain(Function<T, Work<R>> task) {
        return append(new PipelineStage.ChainStage(
            val -> task.apply((T) val)
        ));
    }

    public <R, U> Work<U> thenChain(Function<T, Work<R>> task, BiFunction<T, R, U> merger) {
        return append(new PipelineStage.ChainStage(
            val -> task.apply((T) val).thenFunction(WorkScheduler.COMPUTE, res -> merger.apply((T) val, res))
        ));
    }

    /**
     * Sequentially chains another work segment, ignoring the result of the current one.
     */
    public <R> Work<R> then(Work<R> other) {
        return thenChain(ignore -> other);
    }

    public Work<T> peekChain(WorkScheduler scheduler, Function<T, Work<?>> task) {
        return thenChain(val -> {
            Work<?> sub = task.apply(val);
            List<PipelineStage> newStages = new java.util.ArrayList<>(sub.getStages());
            newStages.add(new PipelineStage.BreakStage(val));
            return new Work<>(newStages, tag);
        });
    }

    @SuppressWarnings("unchecked")
    public <U, R> Work<R> zipWith(Work<U> other, BiFunction<T, U, R> zipper) {
        return append(new PipelineStage.ZipStage(
            other,
            (v1, v2) -> zipper.apply((T) v1, (U) v2)
        ));
    }

    /**
     * Zips multiple work segments into a single list of results.
     */
    public Work<List<Object>> zip(WorkScheduler scheduler, Work<?>... others) {
        Work<List<Object>> current = thenFunction(scheduler, val -> {
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

    /**
     * Executes one of two pipeline segments based on a condition.
     */
    @SuppressWarnings("unchecked")
    public <R> Work<R> thenBranch(io.reactivex.rxjava3.functions.Predicate<T> condition, Work<R> onTrue, Work<R> onFalse) {
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

    public Work<T> require(WorkScheduler scheduler, Predicate<T> condition, Function<T, Throwable> errorSupplier) {
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

    public Work<T> reject(WorkScheduler scheduler, Predicate<T> condition, Function<T, Throwable> errorSupplier) {
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

    public Work<T> recover(java.util.function.Function<Throwable, T> fallback) {
        return recover(Throwable.class, fallback);
    }

    public Work<T> recover(Class<? extends Throwable> type, java.util.function.Function<Throwable, T> fallback) {
        return append(new PipelineStage.RecoverStage(type, err -> fallback.apply(err)));
    }

    public Work<T> recoverWith(java.util.function.Function<Throwable, Work<T>> fallbackWorkFunction) {
        return recoverWith(Throwable.class, fallbackWorkFunction);
    }

    @SuppressWarnings("unchecked")
    public Work<T> recoverWith(Class<? extends Throwable> type, java.util.function.Function<Throwable, Work<T>> fallbackWorkFunction) {
        return append(new PipelineStage.RecoverChainStage(type, err -> (Work<Object>) (Work<?>) fallbackWorkFunction.apply(err)));
    }

    public Work<T> retry(int max, long delay, ResiliencePolicy.BackoffStrategy strategy) {
        return retryIf(max, delay, strategy, err -> true);
    }

    public Work<T> retryIf(int max, long delay, ResiliencePolicy.BackoffStrategy strategy, java.util.function.Predicate<Throwable> condition) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                new ResiliencePolicy.RetryPolicy(max, delay, strategy, condition),
                m.timeout(), m.fallback(), m.compensation()
        ));
    }

    public Work<T> timeout(long duration, java.util.concurrent.TimeUnit unit) {
        return timeout(duration, unit, (Throwable) null);
    }

    public Work<T> timeout(long duration, java.util.concurrent.TimeUnit unit, String customMessage) {
        return timeout(duration, unit, new java.util.concurrent.TimeoutException(customMessage));
    }

    public Work<T> timeout(long duration, java.util.concurrent.TimeUnit unit, Throwable customError) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(),
                new ResiliencePolicy.TimeoutPolicy(duration, unit, customError),
                m.fallback(), m.compensation()
        ));
    }

    public Work<T> delay(WorkScheduler scheduler, long delay, java.util.concurrent.TimeUnit unit) {
        return append(new PipelineStage.AsyncStage(
            (val, ignored) -> Single.just(val).delay(delay, unit, SchedulerResolver.resolve(scheduler)),
            scheduler
        ));
    }

    public Work<T> fallback(Work<T> fallbackWork) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(), m.timeout(), fallbackWork, m.compensation()
        ));
    }

    public Work<T> compensate(Work<Done> compensationWork) {
        return updateLastResilience(m -> new ResiliencePolicy.ResilienceMetadata(
                m.retry(), m.timeout(), m.fallback(), compensationWork
        ));
    }

    /**
     * Applies a resilience policy to the entire preceding pipeline segment.
     */
    public Work<T> resilience(java.util.function.Consumer<ResiliencePolicy.ResilienceBuilder> config) {
        ResiliencePolicy.ResilienceBuilder builder = new ResiliencePolicy.ResilienceBuilder();
        config.accept(builder);
        ResiliencePolicy.ResilienceMetadata m = builder.build();

        Work<T> segment = new Work<>(this.stages, tag);
        PipelineStage segmentStage = new PipelineStage.ChainStage(val -> segment, m);

        List<PipelineStage> newStages = new ArrayList<>();
        newStages.add(segmentStage);
        return new Work<>(newStages, tag);
    }

    /**
     * Applies a transformation to this work pipeline.
     */
    public <R> Work<R> compose(java.util.function.Function<Work<T>, Work<R>> transformer) {
        return transformer.apply(this);
    }

    private Work<T> updateLastResilience(java.util.function.Function<ResiliencePolicy.ResilienceMetadata, ResiliencePolicy.ResilienceMetadata> updater) {
        if (stages.isEmpty()) return this;

        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        PipelineStage last = newStages.remove(newStages.size() - 1);

        ResiliencePolicy.ResilienceMetadata newM = updater.apply(last.resilience());
        PipelineStage updated = last.withResilience(newM);

        newStages.add(updated);
        return new Work<>(newStages, tag);
    }

    public Work<T> thenBreak(T value) {
        return append(new PipelineStage.BreakStage(value));
    }

    public <R> Work<R> thenContinue(WorkScheduler scheduler, R value) {
        return thenCallable(scheduler, () -> value);
    }

    public Work<T> recoverBreak(T recoveryValue) {
        return append(new PipelineStage.RecoverBreakStage(recoveryValue));
    }

    /**
     * Executes the provided action regardless of success or failure.
     */
    public Work<T> doFinally(Runnable action) {
        return append(new PipelineStage.FinalStage(action));
    }

    public Work<T> log(LogLevel level) {
        return log(level, null);
    }

    public Work<T> log(LogLevel level, String message) {
        return append(new PipelineStage.LogStage(level, message));
    }

    public Single<T> asTerminalSingle() {
        return PipelineCompiler.compile(stages, tag, null);
    }

    public Disposable execute() {
        return PipelineSubscriber.subscribe(asTerminalSingle(), null, null, TAG);
    }

    public Disposable executeOn(WorkScheduler workScheduler, io.reactivex.rxjava3.functions.Consumer<T> onSuccess, io.reactivex.rxjava3.functions.Consumer<Throwable> onError) {
        return PipelineSubscriber.subscribe(
                asTerminalSingle().observeOn(SchedulerResolver.resolve(workScheduler)),
                onSuccess,
                onError,
                TAG
        );
    }
}
