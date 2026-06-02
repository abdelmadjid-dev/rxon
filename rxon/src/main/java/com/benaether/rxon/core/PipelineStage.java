package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import java.util.concurrent.TimeUnit;

/**
 * Represents a deferred stage in the RxOn orchestration engine.
 * Stages are immutable definitions of work to be performed.
 * Generics are erased at this level to allow for heterogeneous pipeline transitions.
 */
public sealed interface PipelineStage permits
    PipelineStage.ChainStage,
    PipelineStage.BreakStage,
    PipelineStage.RecoverBreakStage,
    PipelineStage.SyncStage,
    PipelineStage.AsyncStage,
    PipelineStage.FailStage,
    PipelineStage.RecoverStage,
    PipelineStage.ConditionalBreakStage,
    PipelineStage.ConditionalFailStage,
    PipelineStage.ZipStage,
    PipelineStage.StreamingStage,
    PipelineStage.ConditioningStage,
    PipelineStage.NoOpStage,
    PipelineStage.FinalStage,
    PipelineStage.LogStage {

    enum ConditioningType { DEBOUNCE, THROTTLE, DISTINCT, BUFFER }

    ResiliencePolicy.ResilienceMetadata resilience();
    
    PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience);

    record NoOpStage(WorkScheduler scheduler, ResiliencePolicy.ResilienceMetadata resilience) implements PipelineStage {
        public NoOpStage(WorkScheduler scheduler) { this(scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY); }
        @Override public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) { return new NoOpStage(scheduler, resilience); }
    }

    record FinalStage(Runnable action) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
        @Override public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) { return this; }
    }

    record ChainStage(
        java.util.function.Function<Object, Work<?>> task,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ChainStage(java.util.function.Function<Object, Work<?>> task) {
            this(task, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new ChainStage(task, resilience);
        }
    }

    record BreakStage(Object value) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
        @Override public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) { return this; }
    }

    record RecoverBreakStage(Object value) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
        @Override public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) { return this; }
    }

    record SyncStage(
        java.util.function.BiFunction<Object, Object, Object> task,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        SyncStage(java.util.function.BiFunction<Object, Object, Object> task, WorkScheduler scheduler) {
            this(task, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new SyncStage(task, scheduler, resilience);
        }
    }

    record AsyncStage(
        java.util.function.BiFunction<Object, Object, io.reactivex.rxjava3.core.Single<Object>> task,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        AsyncStage(java.util.function.BiFunction<Object, Object, io.reactivex.rxjava3.core.Single<Object>> task, WorkScheduler scheduler) {
            this(task, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new AsyncStage(task, scheduler, resilience);
        }
    }

    record FailStage(Throwable error) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
        @Override public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) { return this; }
    }

    record RecoverStage(
        Class<? extends Throwable> type,
        java.util.function.Function<Throwable, Object> fallback,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        RecoverStage(Class<? extends Throwable> type, java.util.function.Function<Throwable, Object> fallback) {
            this(type, fallback, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new RecoverStage(type, fallback, resilience);
        }
    }

    record ConditionalBreakStage(
        io.reactivex.rxjava3.functions.Predicate<Object> condition,
        Object defaultValue,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ConditionalBreakStage(io.reactivex.rxjava3.functions.Predicate<Object> condition, Object defaultValue) {
            this(condition, defaultValue, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new ConditionalBreakStage(condition, defaultValue, resilience);
        }
    }

    record ConditionalFailStage(
        io.reactivex.rxjava3.functions.Predicate<Object> condition,
        Throwable error,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ConditionalFailStage(io.reactivex.rxjava3.functions.Predicate<Object> condition, Throwable error) {
            this(condition, error, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new ConditionalFailStage(condition, error, resilience);
        }
    }

    record ZipStage(
        Work<?> other,
        java.util.function.BiFunction<Object, Object, Object> zipper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ZipStage(Work<?> other, java.util.function.BiFunction<Object, Object, Object> zipper) {
            this(other, zipper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new ZipStage(other, zipper, resilience);
        }
    }

    record StreamingStage(
        java.util.function.BiFunction<Object, Object, io.reactivex.rxjava3.core.Flowable<Object>> task,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        StreamingStage(java.util.function.BiFunction<Object, Object, io.reactivex.rxjava3.core.Flowable<Object>> task, WorkScheduler scheduler) {
            this(task, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new StreamingStage(task, scheduler, resilience);
        }
    }

    record ConditioningStage(
        ConditioningType type,
        long time,
        TimeUnit unit,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ConditioningStage(ConditioningType type, long time, TimeUnit unit, WorkScheduler scheduler) {
            this(type, time, unit, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
        
        ConditioningStage(ConditioningType type, WorkScheduler scheduler) {
            this(type, 0, null, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new ConditioningStage(type, time, unit, scheduler, resilience);
        }
    }

    record LogStage(
        LogLevel level,
        String message,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        LogStage(LogLevel level, String message) {
            this(level, message, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }

        @Override
        public PipelineStage withResilience(ResiliencePolicy.ResilienceMetadata resilience) {
            return new LogStage(level, message, resilience);
        }

        @Override public String toString() {
            return "Log(" + level + "): " + (message != null ? message : "");
        }
    }
}
