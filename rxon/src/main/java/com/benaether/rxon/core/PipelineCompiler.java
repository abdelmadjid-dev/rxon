package com.benaether.rxon.core;

import com.benaether.rxon.rx.RxLog;
import com.benaether.rxon.rx.RxOnLogger;
import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import java.util.List;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicLong;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Compiles a list of PipelineStages into an executable RxJava Single chain with context support
 * and resilience policies including SAGA-style compensations.
 */
final class PipelineCompiler {

    @SuppressWarnings("unchecked")
    static <T> Single<T> compile(List<PipelineStage> stages, String tag, Object initialValue) {
        return compileInternal(stages, tag, initialValue)
                .map(res -> (T) res.value())
                .onErrorResumeNext(err -> Single.error(RxOnConfig.mapError(err)));
    }

    static Single<PipelineResult<Object>> compileInternal(List<PipelineStage> stages, String tag, Object initialValue) {
        if (stages == null || stages.isEmpty()) {
            return Single.fromCallable(() -> PipelineResult.of(initialValue != null ? initialValue : Done.INSTANCE));
        }

        final long pipelineStartTime = System.currentTimeMillis();
        Single<PipelineResult<Object>> chain = null;

        for (int i = 0; i < stages.size(); i++) {
            PipelineStage stage = stages.get(i);
            final int stageIndex = i;
            
            Single<PipelineResult<Object>> stageSingle;
            if (chain == null) {
                stageSingle = initializeChain(stage, initialValue);
            } else {
                stageSingle = appendToChain(chain, stage);
            }

            if (RxOnConfig.isDebug()) {
                final String stageDesc = stage.toString();
                final AtomicLong stageStartTime = new AtomicLong();
                
                stageSingle = stageSingle
                    .doOnSubscribe(d -> {
                        stageStartTime.set(System.currentTimeMillis());
                        RxOnConfig.getLogger().onStageStart(tag, stageIndex, stageDesc);
                    })
                    .doOnSuccess(res -> {
                        long duration = System.currentTimeMillis() - stageStartTime.get();
                        RxOnConfig.getLogger().onStageEnd(tag, stageIndex, stageDesc, duration);
                    });
            }

            chain = applyResilience(stageSingle, stage);
        }

        if (RxOnConfig.isDebug()) {
            chain = chain
                .doOnSuccess(res -> {
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
                return runCompensations(pe.getCompensationStack(), pe.getValue(), pe.getCause());
            }
            return Single.error(err);
        });
    }

    private static Single<PipelineResult<Object>> initializeChain(PipelineStage stage, Object initialValue) {
        Object startVal = initialValue != null ? initialValue : Done.INSTANCE;
        if (stage instanceof PipelineStage.SyncStage s) {
            return Single.fromCallable(() -> {
                Object res = s.task().apply(startVal, Done.INSTANCE);
                return PipelineResult.of(res);
            }).subscribeOn(SchedulerResolver.resolve(s.scheduler()));
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return Single.defer(() -> s.task().apply(startVal, Done.INSTANCE)
                .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                .map(PipelineResult::of));
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return Single.fromCallable(() -> PipelineResult.terminated(s.value(), java.util.Collections.emptyList()));
        } else if (stage instanceof PipelineStage.RecoverBreakStage s) {
            return Single.fromCallable(() -> PipelineResult.of(s.value()));
        } else if (stage instanceof PipelineStage.FailStage s) {
            return Single.error(s.error());
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return Single.defer(() -> {
                Work<?> subWork = s.task().apply(startVal);
                return compileInternal(subWork.getStages(), subWork.getTag(), startVal);
            });
        } else if (stage instanceof PipelineStage.RecoverStage s) {
            return Single.fromCallable(() -> PipelineResult.of(startVal));
        } else if (stage instanceof PipelineStage.ZipStage s) {
            return Single.defer(() -> compileInternal(s.other().getStages(), s.other().getTag(), startVal)
                .map(otherRes -> PipelineResult.of(s.zipper().apply(startVal, otherRes.value()))));
        } else if (stage instanceof PipelineStage.NoOpStage s) {
            return Single.fromCallable(() -> (PipelineResult<Object>) (PipelineResult<?>) PipelineResult.of(startVal))
                .subscribeOn(SchedulerResolver.resolve(s.scheduler()));
        } else if (stage instanceof PipelineStage.LogStage s) {
            return Single.fromCallable(() -> {
                logPipelineState("Entry", s, startVal);
                return PipelineResult.of(startVal);
            });
        } else {
            return Single.error(new IllegalStateException("Invalid entry stage type: " + stage.getClass().getName()));
        }
    }

    private static Single<PipelineResult<Object>> appendToChain(Single<PipelineResult<Object>> chain, PipelineStage stage) {
        if (stage instanceof PipelineStage.SyncStage s) {
            return chain.observeOn(SchedulerResolver.resolve(s.scheduler())).flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    Object res = s.task().apply(prev.value(), Done.INSTANCE);
                    return Single.just(new PipelineResult<>(res, prev.compensationStack(), false));
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return s.task().apply(prev.value(), Done.INSTANCE)
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.value(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                return PipelineResult.terminated(s.value(), prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.RecoverBreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) {
                    return new PipelineResult<>(s.value(), prev.compensationStack(), false);
                }
                return prev;
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return Single.error(new PipelineException(s.error(), prev.value(), prev.compensationStack()));
            });
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    Work<?> subWork = s.task().apply(prev.value());
                    return compileInternal(subWork.getStages(), subWork.getTag(), prev.value())
                        .map(subRes -> subRes.mergeCompensations(prev.compensationStack()))
                        .onErrorResumeNext(e -> {
                            if (e instanceof PipelineException) return Single.error(e);
                            return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                        });
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.RecoverStage s) {
            return chain.onErrorResumeNext(err -> {
                Throwable root = err instanceof PipelineException pe ? pe.getCause() : err;
                Throwable match = null;
                Throwable current = root;
                while (current != null) {
                    if (s.type().isInstance(current)) {
                        match = current;
                        break;
                    }
                    if (current == current.getCause()) break;
                    current = current.getCause();
                }

                if (match != null) {
                    Object val = err instanceof PipelineException pe ? pe.getValue() : null;
                    List<Work<Done>> stack = err instanceof PipelineException pe ? pe.getCompensationStack() : java.util.Collections.emptyList();
                    try {
                        Object recoveredValue = s.fallback().apply(match);
                        return Single.just(new PipelineResult<>(recoveredValue, stack, false));
                    } catch (Throwable e) {
                        return Single.error(new PipelineException(e, val, stack));
                    }
                }
                return Single.error(err);
            });
        } else if (stage instanceof PipelineStage.ConditionalBreakStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    if (s.condition().test(prev.value())) {
                        return Single.just(PipelineResult.terminated(s.defaultValue(), prev.compensationStack()));
                    }
                    return Single.just(prev);
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.ConditionalFailStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    if (s.condition().test(prev.value())) {
                        return Single.error(new PipelineException(s.error(), prev.value(), prev.compensationStack()));
                    }
                    return Single.just(prev);
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.value(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.ZipStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return compileInternal(s.other().getStages(), s.other().getTag(), prev.value())
                    .map(otherRes -> {
                        Object combined = s.zipper().apply(prev.value(), otherRes.value());
                        return otherRes.mergeCompensations(prev.compensationStack()).withValue(combined);
                    })
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.value(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.NoOpStage s) {
            return chain.observeOn(SchedulerResolver.resolve(s.scheduler()));
        } else if (stage instanceof PipelineStage.FinalStage s) {
            return chain.doFinally(s.action()::run);
        } else if (stage instanceof PipelineStage.LogStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                logPipelineState("Step", s, prev.value());
                return prev;
            });
        } else {
            return chain.flatMap(ignored -> Single.error(new IllegalStateException("Unknown stage type: " + stage.getClass().getName())));
        }
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

    @SuppressWarnings("unchecked")
    private static Single<PipelineResult<Object>> applyResilience(Single<PipelineResult<Object>> stageSingle, PipelineStage stage) {
        ResiliencePolicy.ResilienceMetadata m = stage.resilience();
        Single<PipelineResult<Object>> current = stageSingle;

        if (m.hasTimeout()) {
            if (m.timeout().customError() != null) {
                current = current.timeout(m.timeout().duration(), m.timeout().unit(), Single.error(m.timeout().customError()));
            } else {
                current = current.timeout(m.timeout().duration(), m.timeout().unit());
            }
        }

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

        if (m.hasCompensation()) {
            current = current.map(res -> res.pushCompensation((Work<Done>) m.compensation()));
        }

        current = current.onErrorResumeNext(err -> {
            if (m.hasFallback()) {
                Object failedVal = (err instanceof PipelineException pe) ? pe.getValue() : null;
                Work<Object> fallbackWork = (Work<Object>) m.fallback();
                return compileInternal(fallbackWork.getStages(), fallbackWork.getTag(), failedVal);
            }
            return Single.error(err);
        });

        return current;
    }

    static Single<PipelineResult<Object>> runCompensations(List<Work<Done>> stack, Object val, Throwable originalError) {
        if (stack == null || stack.isEmpty()) {
            return Single.error(originalError);
        }

        List<Work<Done>> mutableStack = new java.util.ArrayList<>(stack);
        Work<Done> compensation = mutableStack.remove(0);

        return compileInternal(compensation.getStages(), compensation.getTag(), val)
            .onErrorReturnItem(PipelineResult.of(Done.INSTANCE))
            .flatMap(ignored -> runCompensations(mutableStack, val, originalError));
    }
}
