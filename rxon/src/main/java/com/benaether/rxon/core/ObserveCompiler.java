package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;
import com.benaether.rxon.rx.RxOnLogger;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compiles a list of PipelineStages into an executable RxJava Flowable chain.
 */
final class ObserveCompiler {

    @SuppressWarnings("unchecked")
    static <T> Flowable<T> compile(List<PipelineStage> stages, String tag, Flowable<Object> source) {
        final long pipelineStartTime = System.currentTimeMillis();
        
        // Initialize chain
        Flowable<PipelineResult<Object>> chain = source.map(PipelineResult::of);

        for (int i = 0; i < stages.size(); i++) {
            PipelineStage stage = stages.get(i);
            final int stageIndex = i;
            final String stageDesc = stage.toString();
            final AtomicLong stageStartTime = new AtomicLong();

            if (RxOnConfig.isDebug()) {
                chain = chain.doOnNext(v -> {
                    stageStartTime.set(System.currentTimeMillis());
                    RxOnConfig.getLogger().onStageStart(tag, stageIndex, stageDesc);
                });
            }

            Flowable<PipelineResult<Object>> next = appendToChain(chain, stage);

            if (RxOnConfig.isDebug()) {
                next = next.doOnNext(res -> {
                    long duration = System.currentTimeMillis() - stageStartTime.get();
                    RxOnConfig.getLogger().onStageEnd(tag, stageIndex, stageDesc, duration);
                });
            }

            // Apply resilience policy for this stage
            chain = applyResilience(next, stage);
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

        return chain.onErrorResumeNext(err -> {
            if (err instanceof PipelineException pe) {
                return PipelineCompiler.runCompensations(pe.getCompensationStack(), pe.getValue(), pe.getCause()).toFlowable();
            }
            return Flowable.error(err);
        }).map(res -> (T) res.value());
    }

    private static Flowable<PipelineResult<Object>> appendToChain(Flowable<PipelineResult<Object>> chain, PipelineStage stage) {
        if (stage instanceof PipelineStage.NoOpStage s) {
            return chain.observeOn(SchedulerResolver.resolve(s.scheduler()));
        } else if (stage instanceof PipelineStage.SyncStage s) {
            return chain.observeOn(SchedulerResolver.resolve(s.scheduler())).concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                try {
                    Object res = s.task().apply(prev.value(), null);
                    return Flowable.just(new PipelineResult<>(res, prev.compensationStack(), false));
                } catch (Throwable e) {
                    return Flowable.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return chain.concatMapSingle(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return s.task().apply(prev.value(), null)
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.value(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.StreamingStage s) {
            return chain.concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                return s.task().apply(prev.value(), null)
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Flowable.error(new PipelineException(e, prev.value(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.ConditioningStage s) {
            Flowable<PipelineResult<Object>> conditioningChain = chain.observeOn(SchedulerResolver.resolve(s.scheduler()));
            return switch (s.type()) {
                case DEBOUNCE -> conditioningChain.debounce(s.time(), s.unit(), SchedulerResolver.resolve(s.scheduler()));
                case THROTTLE -> conditioningChain.throttleFirst(s.time(), s.unit(), SchedulerResolver.resolve(s.scheduler()));
                case DISTINCT -> conditioningChain.distinctUntilChanged((p1, p2) -> 
                    java.util.Objects.equals(p1.value(), p2.value())
                );
                case BUFFER -> conditioningChain
                        .buffer(s.time(), s.unit(), SchedulerResolver.resolve(s.scheduler()))
                        .map(list -> {
                            List<Object> values = new ArrayList<>(list.size());
                            List<Work<Done>> combinedCompensations = new ArrayList<>();
                            for (PipelineResult<Object> result : list) {
                                values.add(result.value());
                                combinedCompensations.addAll(result.compensationStack());
                            }
                            return new PipelineResult<>(values, combinedCompensations, false);
                        });
            };
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return chain.concatMapSingle(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    Work<?> subWork = (Work<?>) s.task().apply(prev.value());
                    return PipelineCompiler.compileInternal(subWork.getStages(), subWork.getTag(), prev.value())
                        .map(subRes -> subRes.mergeCompensations(prev.compensationStack()))
                        .onErrorResumeNext(e -> {
                            if (e instanceof PipelineException) return Single.error(e);
                            return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                        });
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                return PipelineResult.terminated(s.value(), prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return chain.concatMap(prev -> {
                if (prev.terminated()) return Flowable.just(prev);
                return Flowable.error(new PipelineException(s.error(), prev.value(), prev.compensationStack()));
            });
        } else if (stage instanceof PipelineStage.ConditionalBreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                try {
                    if (s.condition().test(prev.value())) {
                        return PipelineResult.terminated(s.defaultValue(), prev.compensationStack());
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
                        return Flowable.error(new PipelineException(s.error(), prev.value(), prev.compensationStack()));
                    }
                    return Flowable.just(prev);
                } catch (Throwable e) {
                    return Flowable.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.FinalStage s) {
            return chain.doFinally(s.action()::run);
        } else if (stage instanceof PipelineStage.LogStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                logPipelineState("Observe", s, prev.value());
                return prev;
            });
        } else if (stage instanceof PipelineStage.ZipStage s) {
            return chain.concatMapSingle(prev -> {
                if (prev.terminated()) return Single.just(prev);
                Work<?> otherWork = (Work<?>) s.other();
                return PipelineCompiler.compileInternal(otherWork.getStages(), otherWork.getTag(), prev.value())
                    .map(otherRes -> {
                        Object zippedValue = s.zipper().apply(prev.value(), otherRes.value());
                        return otherRes.mergeCompensations(prev.compensationStack()).withValue(zippedValue);
                    });
            });
        } else if (stage instanceof PipelineStage.RecoverStage s) {
            return chain.onErrorResumeNext(e -> {
                Throwable cause = (e instanceof PipelineException pe) ? pe.getCause() : e;
                if (s.type().isInstance(cause)) {
                    List<Work<Done>> stack = 
                        (e instanceof PipelineException pe) ? pe.getCompensationStack() : java.util.Collections.emptyList();
                    return Flowable.just(PipelineResult.of(s.fallback().apply(cause), stack));
                }
                return Flowable.error(e);
            });
        } else if (stage instanceof PipelineStage.RecoverBreakStage s) {
            return chain.onErrorResumeNext(e -> {
                List<Work<Done>> stack = 
                    (e instanceof PipelineException pe) ? pe.getCompensationStack() : java.util.Collections.emptyList();
                return Flowable.just(PipelineResult.terminated(s.value(), stack));
            });
        } else {
            return chain;
        }
    }

    @SuppressWarnings("unchecked")
    private static Flowable<PipelineResult<Object>> applyResilience(Flowable<PipelineResult<Object>> stageFlowable, PipelineStage stage) {
        ResiliencePolicy.ResilienceMetadata m = stage.resilience();
        
        Flowable<PipelineResult<Object>> current = stageFlowable;

        // 1. Timeout
        if (m.hasTimeout()) {
            current = current.timeout(m.timeout().duration(), m.timeout().unit());
        }

        // 2. Retry
        if (m.hasRetry()) {
            final int max = m.retry().maxRetries();
            final long delay = m.retry().delayMs();
            final ResiliencePolicy.BackoffStrategy backoff = m.retry().backoff();

            current = current.retryWhen(errors -> errors.zipWith(
                Flowable.range(1, max + 1),
                (err, i) -> {
                    if (i > max || !m.retry().condition().test(err)) {
                        throw (err instanceof RuntimeException ? (RuntimeException) err : new RuntimeException(err));
                    }
                    return i;
                }
            ).flatMap(retryCount -> {
                long actualDelay = backoff == ResiliencePolicy.BackoffStrategy.EXPONENTIAL
                    ? delay * (long) Math.pow(2, retryCount - 1)
                    : delay;
                return Flowable.timer(actualDelay, java.util.concurrent.TimeUnit.MILLISECONDS);
            }));
        }

        // 3. Compensation Registration (on Success)
        if (m.hasCompensation()) {
            current = current.map(res -> res.pushCompensation((Work<Done>) m.compensation()));
        }

        // 4. Fallback or Compensation (on Failure)
        current = current.onErrorResumeNext(err -> {
            if (m.hasFallback()) {
                Object failedValue = (err instanceof PipelineException pe) ? pe.getValue() : null;
                Work<Object> fallbackWork = (Work<Object>) m.fallback();
                return PipelineCompiler.compileInternal(fallbackWork.getStages(), fallbackWork.getTag(), failedValue).toFlowable();
            }
            return Flowable.error(err);
        });

        return current;
    }

    private static void logPipelineState(String source, PipelineStage.LogStage s, Object value) {
        String msg = (s.message() != null ? s.message() + " | " : "") + "Value: " + value;
        RxOnLogger logger = RxOnConfig.getLogger();
        switch (s.level()) {
            case DEBUG -> logger.d("RxOn:" + source, msg);
            case INFO -> logger.i("RxOn:" + source, msg);
            case WARN -> logger.w("RxOn:" + source, msg, null);
            case ERROR -> logger.e("RxOn:" + source, msg, null);
        }
    }
}
