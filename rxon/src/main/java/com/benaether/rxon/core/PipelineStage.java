package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
    PipelineStage.LogStage {

    ResiliencePolicy.ResilienceMetadata resilience();

    record ChainStage(
        BiFunction<Object, Object, Work<?, ?>> task,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ChainStage(BiFunction<Object, Object, Work<?, ?>> task) {
            this(task, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record BreakStage(Object value) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
    }

    record RecoverBreakStage(Object value) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
    }

    record SyncStage(
        BiFunction<Object, Object, Object> task,
        BiFunction<Object, Object, Object> contextMapper,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        SyncStage(BiFunction<Object, Object, Object> task, BiFunction<Object, Object, Object> contextMapper, WorkScheduler scheduler) {
            this(task, contextMapper, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record AsyncStage(
        BiFunction<Object, Object, io.reactivex.rxjava3.core.Single<Object>> task,
        BiFunction<Object, Object, Object> contextMapper,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        AsyncStage(BiFunction<Object, Object, io.reactivex.rxjava3.core.Single<Object>> task, BiFunction<Object, Object, Object> contextMapper, WorkScheduler scheduler) {
            this(task, contextMapper, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record FailStage(Throwable error) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
    }

    record RecoverStage(
        Class<? extends Throwable> type,
        java.util.function.Function<Throwable, Object> fallback,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        RecoverStage(Class<? extends Throwable> type, java.util.function.Function<Throwable, Object> fallback) {
            this(type, fallback, ResiliencePolicy.ResilienceMetadata.EMPTY);
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
    }

    record ConditionalFailStage(
        io.reactivex.rxjava3.functions.Predicate<Object> condition,
        Throwable error,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ConditionalFailStage(io.reactivex.rxjava3.functions.Predicate<Object> condition, Throwable error) {
            this(condition, error, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record ZipStage(
        Work<?, ?> other,
        BiFunction<Object, Object, Object> zipper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ZipStage(Work<?, ?> other, BiFunction<Object, Object, Object> zipper) {
            this(other, zipper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record StreamingStage(
        BiFunction<Object, Object, io.reactivex.rxjava3.core.Flowable<Object>> task,
        BiFunction<Object, Object, Object> contextMapper,
        WorkScheduler scheduler,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        StreamingStage(BiFunction<Object, Object, io.reactivex.rxjava3.core.Flowable<Object>> task, BiFunction<Object, Object, Object> contextMapper, WorkScheduler scheduler) {
            this(task, contextMapper, scheduler, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    enum ConditioningType { DEBOUNCE, THROTTLE, DISTINCT }

    record ConditioningStage(
        ConditioningType type,
        long time,
        TimeUnit unit,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ConditioningStage(ConditioningType type, long time, TimeUnit unit) {
            this(type, time, unit, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
        
        ConditioningStage(ConditioningType type) {
            this(type, 0, null, ResiliencePolicy.ResilienceMetadata.EMPTY);
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

        @Override public String toString() {
            return "Log(" + level + "): " + (message != null ? message : "");
        }
    }
}
