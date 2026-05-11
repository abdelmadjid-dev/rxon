package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;
import com.benaether.rxon.rx.RxOnLogger;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

import java.util.List;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compiles a list of PipelineStages into an executable RxJava Flowable chain.
 */
final class ObserveCompiler {

    @SuppressWarnings("unchecked")
    static <T> Flowable<T> compile(List<PipelineStage> stages, String tag, Flowable<Object> source, Supplier<Object> initialContextSupplier) {
        final long pipelineStartTime = System.currentTimeMillis();
        Flowable<PipelineResult<Object>> chain = source.map(val -> PipelineResult.of(val, initialContextSupplier.get()));

        for (int i = 0; i < stages.size(); i++) {
            PipelineStage stage = stages.get(i);
            final int stageIndex = i;
            
            Flowable<PipelineResult<Object>> next = appendToChain(chain, stage);

            if (RxOnConfig.isDebug()) {
                final String stageDesc = stage.toString();
                final AtomicLong stageStartTime = new AtomicLong();
                
                next = next
                    .doOnNext(res -> {
                        // Per-emission timing in streams is tricky, 
                        // we track first emission start or just log event
                        RxOnConfig.getLogger().onStageStart(tag, stageIndex, stageDesc);
                        stageStartTime.set(System.currentTimeMillis());
                    })
                    .doAfterNext(res -> {
                        long duration = System.currentTimeMillis() - stageStartTime.get();
                        RxOnConfig.getLogger().onStageEnd(tag, stageIndex, stageDesc, duration);
                    });
            }
            chain = next;
        }

        if (RxOnConfig.isDebug()) {
            chain = chain
                .doOnComplete(() -> {
                    long totalDuration = System.currentTimeMillis() - pipelineStartTime;
                    RxOnConfig.getLogger().onPipelineFinish(tag, totalDuration);
                })
                .doOnError(err -> {
                    long totalDuration = System.currentTimeMillis() - pipelineStartTime;
                    RxOnConfig.getLogger().onPipelineError(tag, err, totalDuration);
                });
        }

        return chain.map(res -> (T) res.value());
    }

    private static Flowable<PipelineResult<Object>> appendToChain(Flowable<PipelineResult<Object>> chain, PipelineStage stage) {
        if (stage instanceof PipelineStage.SyncStage s) {
            return chain.observeOn(SchedulerResolver.resolve(s.scheduler())).concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                try {
                    Object res = s.task().apply(prev.context(), prev.value());
                    Object nextCtx = s.contextMapper().apply(prev.context(), res);
                    return Flowable.just(new PipelineResult<>(res, nextCtx, prev.compensationStack(), false));
                } catch (Throwable e) {
                    return Flowable.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return chain.concatMapSingle(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.context(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.StreamingStage s) {
            return chain.concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                return s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Flowable.error(new PipelineException(e, prev.context(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.ConditioningStage s) {
            return switch (s.type()) {
                case DEBOUNCE -> chain.debounce(s.time(), s.unit());
                case THROTTLE -> chain.throttleFirst(s.time(), s.unit());
                case DISTINCT -> chain.distinctUntilChanged((p1, p2) -> 
                    java.util.Objects.equals(p1.value(), p2.value())
                );
            };
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                return PipelineResult.terminated(s.value(), prev.context(), prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return chain.concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                return Flowable.error(new PipelineException(s.error(), prev.context(), prev.compensationStack()));
            });
        } else if (stage instanceof PipelineStage.ConditionalBreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                try {
                    if (s.condition().test(prev.value())) {
                        return PipelineResult.terminated(s.defaultValue(), prev.context(), prev.compensationStack());
                    }
                    return prev;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        } else if (stage instanceof PipelineStage.ConditionalFailStage s) {
            return chain.concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                try {
                    if (s.condition().test(prev.value())) {
                        return Flowable.error(new PipelineException(s.error(), prev.context(), prev.compensationStack()));
                    }
                    return Flowable.just(prev);
                } catch (Throwable e) {
                    return Flowable.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.LogStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                logPipelineState("Observe", s, prev.context(), prev.value());
                return prev;
            });
        } else {
            return chain;
        }
    }

    private static void logPipelineState(String source, PipelineStage.LogStage s, Object context, Object value) {
        String msg = (s.message() != null ? s.message() + " | " : "") + 
                     "Value: " + value + " | Context: " + context;
        RxOnLogger logger = RxOnConfig.getLogger();
        switch (s.level()) {
            case DEBUG -> logger.d("RxOn:" + source, msg);
            case INFO -> logger.i("RxOn:" + source, msg);
            case WARN -> logger.w("RxOn:" + source, msg, null);
            case ERROR -> logger.e("RxOn:" + source, msg, null);
        }
    }
}
