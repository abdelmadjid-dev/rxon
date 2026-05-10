package com.benaether.rxon.core;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Represents a deferred stage in the RxOn orchestration engine.
 * Stages are immutable definitions of work to be performed.
 * Generics are erased at this level to allow for heterogeneous pipeline transitions.
 */
sealed interface PipelineStage permits
    PipelineStage.ReadStage,
    PipelineStage.WriteStage,
    PipelineStage.IoStage,
    PipelineStage.ComputeStage,
    PipelineStage.MainStage,
    PipelineStage.FinishStage,
    PipelineStage.FailStage {

    ResiliencePolicy.ResilienceMetadata resilience();

    record ReadStage(
        BiFunction<Object, Object, Object> task, 
        BiFunction<Object, Object, Object> contextMapper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ReadStage(BiFunction<Object, Object, Object> task, BiFunction<Object, Object, Object> contextMapper) {
            this(task, contextMapper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record WriteStage(
        BiConsumer<Object, Object> task, 
        BiFunction<Object, Object, Object> contextMapper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        WriteStage(BiConsumer<Object, Object> task, BiFunction<Object, Object, Object> contextMapper) {
            this(task, contextMapper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record IoStage(
        BiFunction<Object, Object, Object> task, 
        BiFunction<Object, Object, Object> contextMapper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        IoStage(BiFunction<Object, Object, Object> task, BiFunction<Object, Object, Object> contextMapper) {
            this(task, contextMapper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record ComputeStage(
        BiFunction<Object, Object, Object> task, 
        BiFunction<Object, Object, Object> contextMapper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        ComputeStage(BiFunction<Object, Object, Object> task, BiFunction<Object, Object, Object> contextMapper) {
            this(task, contextMapper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record MainStage(
        BiConsumer<Object, Object> task, 
        BiFunction<Object, Object, Object> contextMapper,
        ResiliencePolicy.ResilienceMetadata resilience
    ) implements PipelineStage {
        MainStage(BiConsumer<Object, Object> task, BiFunction<Object, Object, Object> contextMapper) {
            this(task, contextMapper, ResiliencePolicy.ResilienceMetadata.EMPTY);
        }
    }

    record FinishStage(Object value) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
    }

    record FailStage(Throwable error) implements PipelineStage {
        @Override public ResiliencePolicy.ResilienceMetadata resilience() { return ResiliencePolicy.ResilienceMetadata.EMPTY; }
    }
}
