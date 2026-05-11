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
    static <T> Single<T> compile(List<PipelineStage> stages, String tag, Supplier<Object> initialContextSupplier) {
        return compileInternal(stages, tag, initialContextSupplier).map(res -> (T) res.value());
    }

    static Single<PipelineResult<Object>> compileInternal(List<PipelineStage> stages, String tag, Supplier<Object> initialContextSupplier) {
        if (stages == null || stages.isEmpty()) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.of(Done.INSTANCE, initialContext);
            });
        }

        final long pipelineStartTime = System.currentTimeMillis();
        Single<PipelineResult<Object>> chain = null;

        for (int i = 0; i < stages.size(); i++) {
            PipelineStage stage = stages.get(i);
            final int stageIndex = i;
            
            Single<PipelineResult<Object>> stageSingle;
            if (chain == null) {
                stageSingle = initializeChain(stage, initialContextSupplier);
            } else {
                stageSingle = appendToChain(chain, stage);
            }

            // Wrap with logging if debug enabled
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
                return runCompensations(pe.getCompensationStack(), pe.getContext(), pe.getCause());
            }
            return Single.error(err);
        });
    }

    private static Single<PipelineResult<Object>> initializeChain(PipelineStage stage, Supplier<Object> initialContextSupplier) {
        if (stage instanceof PipelineStage.SyncStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                Object res = s.task().apply(initialContext, Done.INSTANCE);
                Object nextCtx = s.contextMapper().apply(initialContext, res);
                return PipelineResult.of(res, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(s.scheduler()));
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, Done.INSTANCE)
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.terminated(s.value(), initialContext, java.util.Collections.emptyList());
            });
        } else if (stage instanceof PipelineStage.RecoverBreakStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.of(s.value(), initialContext);
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return Single.error(s.error());
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                Work<?, ?> subWork = s.task().apply(initialContext, Done.INSTANCE);
                return compileInternal(subWork.getStages(), subWork.getTag(), () -> initialContext);
            });
        } else if (stage instanceof PipelineStage.RecoverStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.of(Done.INSTANCE, initialContext);
            });
        } else if (stage instanceof PipelineStage.ZipStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return compileInternal(s.other().getStages(), s.other().getTag(), () -> initialContext)
                    .map(otherRes -> PipelineResult.of(s.zipper().apply(Done.INSTANCE, otherRes.value()), initialContext));
            });
        } else if (stage instanceof PipelineStage.LogStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                logPipelineState("Entry", s, initialContext, Done.INSTANCE);
                return PipelineResult.of(Done.INSTANCE, initialContext);
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
                    Object res = s.task().apply(prev.context(), prev.value());
                    Object nextCtx = s.contextMapper().apply(prev.context(), res);
                    return Single.just(new PipelineResult<>(res, nextCtx, prev.compensationStack(), false));
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.AsyncStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(s.scheduler()))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack(), false))
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.context(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.BreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                return PipelineResult.terminated(s.value(), prev.context(), prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.RecoverBreakStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) {
                    return new PipelineResult<>(s.value(), prev.context(), prev.compensationStack(), false);
                }
                return prev;
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return Single.error(new PipelineException(s.error(), prev.context(), prev.compensationStack()));
            });
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    Work<?, ?> subWork = s.task().apply(prev.context(), prev.value());
                    return compileInternal(subWork.getStages(), subWork.getTag(), prev::context)
                        .map(subRes -> subRes.mergeCompensations(prev.compensationStack()))
                        .onErrorResumeNext(e -> {
                            if (e instanceof PipelineException) return Single.error(e);
                            return Single.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                        });
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.context(), prev.compensationStack()));
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
                    Object context = err instanceof PipelineException pe ? pe.getContext() : null;
                    List<Work<Done, ?>> stack = err instanceof PipelineException pe ? pe.getCompensationStack() : java.util.Collections.emptyList();
                    try {
                        Object recoveredValue = s.fallback().apply(match);
                        return Single.just(new PipelineResult<>(recoveredValue, context, stack, false));
                    } catch (Throwable e) {
                        return Single.error(new PipelineException(e, context, stack));
                    }
                }
                return Single.error(err);
            });
        } else if (stage instanceof PipelineStage.ConditionalBreakStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    if (s.condition().test(prev.value())) {
                        return Single.just(PipelineResult.terminated(s.defaultValue(), prev.context(), prev.compensationStack()));
                    }
                    return Single.just(prev);
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.ConditionalFailStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                try {
                    if (s.condition().test(prev.value())) {
                        return Single.error(new PipelineException(s.error(), prev.context(), prev.compensationStack()));
                    }
                    return Single.just(prev);
                } catch (Throwable e) {
                    return Single.error(new PipelineException(e, prev.context(), prev.compensationStack()));
                }
            });
        } else if (stage instanceof PipelineStage.ZipStage s) {
            return chain.flatMap(prev -> {
                if (prev.terminated()) return Single.just(prev);
                return compileInternal(s.other().getStages(), s.other().getTag(), prev::context)
                    .map(otherRes -> {
                        Object combined = s.zipper().apply(prev.value(), otherRes.value());
                        return new PipelineResult<>(combined, prev.context(), prev.compensationStack(), false);
                    })
                    .onErrorResumeNext(e -> Single.error(new PipelineException(e, prev.context(), prev.compensationStack())));
            });
        } else if (stage instanceof PipelineStage.LogStage s) {
            return chain.map(prev -> {
                if (prev.terminated()) return prev;
                logPipelineState("Step", s, prev.context(), prev.value());
                return prev;
            });
        } else {
            return chain.flatMap(ignored -> Single.error(new IllegalStateException("Unknown stage type: " + stage.getClass().getName())));
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

    @SuppressWarnings("unchecked")
    private static Single<PipelineResult<Object>> applyResilience(Single<PipelineResult<Object>> stageSingle, PipelineStage stage) {
        ResiliencePolicy.ResilienceMetadata m = stage.resilience();
        
        Single<PipelineResult<Object>> current = stageSingle;

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
                (err, i) -> i
            ).flatMap(retryCount -> {
                if (retryCount > max) {
                    return Flowable.error(new RuntimeException("Retries exhausted"));
                }
                long actualDelay = backoff == ResiliencePolicy.BackoffStrategy.EXPONENTIAL
                    ? delay * (long) Math.pow(2, retryCount - 1)
                    : delay;
                return Flowable.timer(actualDelay, java.util.concurrent.TimeUnit.MILLISECONDS);
            }));
        }

        // 3. Compensation Registration (on Success)
        if (m.hasCompensation()) {
            current = current.map(res -> res.pushCompensation((Work<Done, ?>) m.compensation()));
        }

        // 4. Fallback or Compensation (on Failure)
        current = current.onErrorResumeNext(err -> {
            if (m.hasFallback()) {
                return ((Work<Object, ?>) m.fallback()).asTerminalSingle()
                    .map(res -> PipelineResult.of(res, null)); 
            }
            return Single.error(err);
        });

        return current;
    }

    private static Single<PipelineResult<Object>> runCompensations(List<Work<Done, ?>> stack, Object context, Throwable originalError) {
        if (stack == null || stack.isEmpty()) {
            return Single.error(originalError);
        }

        List<Work<Done, ?>> mutableStack = new java.util.ArrayList<>(stack);
        Work<Done, ?> compensation = mutableStack.remove(0);

        return compileInternal(compensation.getStages(), compensation.getTag(), () -> context)
            .onErrorReturnItem(PipelineResult.of(com.benaether.rxon.scopes.Done.INSTANCE, context)) // SAGA: Ignore compensation failures
            .flatMap(ignored -> runCompensations(mutableStack, context, originalError));
    }

}
