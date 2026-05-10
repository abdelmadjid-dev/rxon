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

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Predicate;

/**
 * {@code Work<T, C>} is a Lazy Orchestration Engine.
 * It defines a "Pipeline" of semantic stages that are compiled into an executable 
 * RxJava chain only when a terminal operator is invoked.
 *
 * <p>Operations are deferred, allowing for declarative resilience policies,
 * implicit state (context) propagation, and strict Clean Architecture boundaries.</p>
 *
 * @param <T> emission type
 * @param <C> context type
 */
public final class Work<T, C> {
    private static final String TAG = Work.class.getName();

    private final List<PipelineStage> stages;

    private Work(List<PipelineStage> stages) {
        this.stages = Collections.unmodifiableList(stages);
    }

    private <R, NewC> Work<R, NewC> append(PipelineStage stage) {
        List<PipelineStage> newStages = new ArrayList<>(this.stages);
        newStages.add(stage);
        return new Work<>(newStages);
    }

    /**
     * Start a fresh pipeline with a lazy initial context state.
     * The supplier runs on the computation scheduler.
     */
    public static <C> Work<Done, C> withContext(Supplier<C> contextSupplier) {
        return new Work<Done, Object>(new ArrayList<>())
            .updateContext((ctx, val) -> contextSupplier.get());
    }

    /**
     * Set the context for subsequent stages based on the current value.
     * This replaces any existing context.
     */
    public <NewC> Work<T, NewC> usingContext(java.util.function.Function<T, NewC> mapper) {
        return updateContext((ctx, val) -> mapper.apply(val));
    }

    /**
     * Change the context type entirely into a different object/type.
     */
    public <NewC> Work<T, NewC> updateContext(BiFunction<C, T, NewC> mapper) {
        return thenCompute((ctx, val) -> val, mapper);
    }

    /**
     * Create a pipeline that starts with a synchronous read operation.
     * Resolves to {@link WorkScheduler#DATA_READ}.
     */
    public static <T> Work<T, Object> read(Callable<T> task) {
        return new Work<Done, Object>(new ArrayList<>()).thenRead(task);
    }

    /**
     * Create a pipeline that starts with an asynchronous IO operation.
     * Resolves to {@link WorkScheduler#IO}.
     */
    public static <T> Work<T, Object> io(Callable<T> task) {
        return new Work<Done, Object>(new ArrayList<>()).thenIo(task);
    }

    /**
     * Create a pipeline that starts with a synchronous write operation.
     * Resolves to {@link WorkScheduler#DATA_WRITE}.
     */
    public static Work<Done, Object> write(Runnable task) {
        return new Work<Done, Object>(new ArrayList<>()).thenWrite(task);
    }

    /**
     * Create a pipeline that starts with a CPU-intensive computation.
     * Resolves to {@link WorkScheduler#COMPUTE}.
     */
    public static <T> Work<T, Object> compute(Callable<T> task) {
        return new Work<Done, Object>(new ArrayList<>()).thenCompute(ignored -> {
            try { return task.call(); }
            catch (Exception e) { throw new RuntimeException(e); }
        });
    }

    /**
     * Create a pipeline that starts on the main application thread.
     * Resolves to {@link WorkScheduler#MAIN}.
     */
    public static Work<Done, Object> main(Runnable task) {
        return new Work<Done, Object>(new ArrayList<>()).thenMain(ignored -> task.run());
    }

    public static <T> Work<T, Object> finish(T value) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.FinishStage(value));
    }

    public static <T> Work<T, Object> fail(Throwable error) {
        return new Work<Done, Object>(new ArrayList<>()).append(new PipelineStage.FailStage(error));
    }

    public <R> Work<R, C> thenRead(Callable<R> task) {
        return thenRead((ctx, ignored) -> {
            try { return task.call(); }
            catch (Exception e) { throw new RuntimeException(e); }
        }, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <R, NewC> Work<R, NewC> thenRead(BiFunction<C, Object, R> task, BiFunction<C, R, NewC> mapper) {
        return append(new PipelineStage.ReadStage(
            (ctx, input) -> task.apply((C) ctx, input),
            (ctx, res) -> mapper.apply((C) ctx, (R) res)
        ));
    }

    public <R> Work<R, C> thenIo(Callable<R> task) {
        return thenIo((ctx, input) -> {
            try { return task.call(); }
            catch (Exception e) { throw new RuntimeException(e); }
        }, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <R, NewC> Work<R, NewC> thenIo(BiFunction<C, T, R> task, BiFunction<C, R, NewC> mapper) {
        return append(new PipelineStage.IoStage(
            (ctx, val) -> task.apply((C) ctx, (T) val),
            (ctx, res) -> mapper.apply((C) ctx, (R) res)
        ));
    }

    public Work<T, C> thenWrite(Runnable task) {
        return thenWrite((ctx, input) -> task.run(), (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <NewC> Work<T, NewC> thenWrite(BiConsumer<C, T> task, BiFunction<C, T, NewC> mapper) {
        return append(new PipelineStage.WriteStage(
            (ctx, val) -> task.accept((C) ctx, (T) val),
            (ctx, val) -> mapper.apply((C) ctx, (T) val)
        ));
    }

    public <R> Work<R, C> thenCompute(Function<T, R> task) {
        return thenCompute((ctx, input) -> {
            try { return task.apply(input); }
            catch (Throwable e) { throw new RuntimeException(e); }
        }, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <R, NewC> Work<R, NewC> thenCompute(BiFunction<C, T, R> task, BiFunction<C, R, NewC> mapper) {
        return append(new PipelineStage.ComputeStage(
            (ctx, input) -> {
                try { return task.apply((C) ctx, (T) input); }
                catch (RuntimeException e) { throw e; }
                catch (Throwable e) { throw new RuntimeException(e); }
            },
            (ctx, res) -> mapper.apply((C) ctx, (R) res)
        ));
    }

    public Work<T, C> thenMain(Consumer<T> task) {
        return thenMain((ctx, input) -> {
            try { task.accept(input); }
            catch (Throwable e) { throw new RuntimeException(e); }
        }, (ctx, res) -> ctx);
    }

    @SuppressWarnings("unchecked")
    public <NewC> Work<T, NewC> thenMain(BiConsumer<C, T> task, BiFunction<C, T, NewC> mapper) {
        return append(new PipelineStage.MainStage(
            (ctx, val) -> task.accept((C) ctx, (T) val),
            (ctx, val) -> mapper.apply((C) ctx, (T) val)
        ));
    }

    public Work<T, C> require(Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenCompute((ctx, t) -> {
            try {
                if (condition.test(t)) return t;
                Throwable error = errorSupplier.apply(t);
                if (error instanceof RuntimeException re) throw re;
                throw new RuntimeException(error);
            } catch (Throwable e) {
                if (e instanceof RuntimeException re) throw re;
                throw new RuntimeException(e);
            }
        }, (ctx, res) -> ctx);
    }

    public Work<T, C> reject(Predicate<T> condition, Function<T, Throwable> errorSupplier) {
        return thenCompute((ctx, t) -> {
            try {
                if (!condition.test(t)) return t;
                Throwable error = errorSupplier.apply(t);
                if (error instanceof RuntimeException re) throw re;
                throw new RuntimeException(error);
            } catch (Throwable e) {
                if (e instanceof RuntimeException re) throw re;
                throw new RuntimeException(e);
            }
        }, (ctx, res) -> ctx);
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
        if (last instanceof PipelineStage.ReadStage s) {
            updated = new PipelineStage.ReadStage(s.task(), s.contextMapper(), newM);
        } else if (last instanceof PipelineStage.WriteStage s) {
            updated = new PipelineStage.WriteStage(s.task(), s.contextMapper(), newM);
        } else if (last instanceof PipelineStage.IoStage s) {
            updated = new PipelineStage.IoStage(s.task(), s.contextMapper(), newM);
        } else if (last instanceof PipelineStage.ComputeStage s) {
            updated = new PipelineStage.ComputeStage(s.task(), s.contextMapper(), newM);
        } else if (last instanceof PipelineStage.MainStage s) {
            updated = new PipelineStage.MainStage(s.task(), s.contextMapper(), newM);
        } else {
            updated = last;
        }

        newStages.add(updated);
        return new Work<>(newStages);
    }

    public Single<T> asTerminalSingle() {
        return PipelineCompiler.compile(stages, () -> null);
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
