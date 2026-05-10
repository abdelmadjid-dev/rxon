package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import java.util.List;
import java.util.function.Supplier;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Compiles a list of PipelineStages into an executable RxJava Single chain with context support
 * and resilience policies including SAGA-style compensations.
 */
final class PipelineCompiler {

    @SuppressWarnings("unchecked")
    static <T> Single<T> compile(List<PipelineStage> stages, Supplier<Object> initialContextSupplier) {
        return compileInternal(stages, initialContextSupplier).map(res -> (T) res.value());
    }

    static Single<PipelineResult<Object>> compileInternal(List<PipelineStage> stages, Supplier<Object> initialContextSupplier) {
        if (stages == null || stages.isEmpty()) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.of(Done.INSTANCE, initialContext);
            });
        }

        Single<PipelineResult<Object>> chain = null;

        for (PipelineStage stage : stages) {
            if (chain == null) {
                chain = applyResilience(initializeChain(stage, initialContextSupplier), stage);
            } else {
                chain = applyResilience(appendToChain(chain, stage), stage);
            }
        }

        return chain;
    }

    private static Single<PipelineResult<Object>> initializeChain(PipelineStage stage, Supplier<Object> initialContextSupplier) {
        if (stage instanceof PipelineStage.ReadStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                Object res = s.task().apply(initialContext, null);
                Object nextCtx = s.contextMapper().apply(initialContext, res);
                return PipelineResult.of(res, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_READ));
        } else if (stage instanceof PipelineStage.IoStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                Object res = s.task().apply(initialContext, null);
                Object nextCtx = s.contextMapper().apply(initialContext, res);
                return PipelineResult.of(res, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(WorkScheduler.IO));
        } else if (stage instanceof PipelineStage.WriteStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                s.task().accept(initialContext, null);
                Object nextCtx = s.contextMapper().apply(initialContext, null);
                return PipelineResult.of((Object) Done.INSTANCE, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_WRITE));
        } else if (stage instanceof PipelineStage.ComputeStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                Object res = s.task().apply(initialContext, Done.INSTANCE);
                Object nextCtx = s.contextMapper().apply(initialContext, res);
                return PipelineResult.of(res, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(WorkScheduler.COMPUTE));
        } else if (stage instanceof PipelineStage.MainStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                s.task().accept(initialContext, Done.INSTANCE);
                Object nextCtx = s.contextMapper().apply(initialContext, Done.INSTANCE);
                return PipelineResult.of((Object) Done.INSTANCE, nextCtx);
            }).subscribeOn(SchedulerResolver.resolve(WorkScheduler.MAIN));
        } else if (stage instanceof PipelineStage.FinishStage s) {
            return Single.fromCallable(() -> {
                Object initialContext = initialContextSupplier.get();
                return PipelineResult.of(s.value(), initialContext);
            });
        } else if (stage instanceof PipelineStage.FailStage s) {
            return Single.error(s.error());
        } else if (stage instanceof PipelineStage.AsyncReadStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, null)
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_READ))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.AsyncWriteStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, null)
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_WRITE))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.AsyncIoStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, null)
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.IO))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.AsyncComputeStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, Done.INSTANCE)
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.COMPUTE))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.AsyncMainStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                return s.task().apply(initialContext, Done.INSTANCE)
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.MAIN))
                    .map(res -> PipelineResult.of(res, s.contextMapper().apply(initialContext, res)));
            });
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return Single.defer(() -> {
                Object initialContext = initialContextSupplier.get();
                Work<?, ?> subWork = s.task().apply(initialContext, Done.INSTANCE);
                return compileInternal(subWork.getStages(), () -> initialContext);
            });
        } else {
            return Single.error(new IllegalStateException("Invalid entry stage type: " + stage.getClass().getName()));
        }
    }

    private static Single<PipelineResult<Object>> appendToChain(Single<PipelineResult<Object>> chain, PipelineStage stage) {
        if (stage instanceof PipelineStage.ReadStage s) {
            return chain.observeOn(SchedulerResolver.resolve(WorkScheduler.DATA_READ)).map(prev -> {
                Object res = s.task().apply(prev.context(), prev.value());
                Object nextCtx = s.contextMapper().apply(prev.context(), res);
                return new PipelineResult<>(res, nextCtx, prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.IoStage s) {
            return chain.observeOn(SchedulerResolver.resolve(WorkScheduler.IO)).map(prev -> {
                Object res = s.task().apply(prev.context(), prev.value());
                Object nextCtx = s.contextMapper().apply(prev.context(), res);
                return new PipelineResult<>(res, nextCtx, prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.WriteStage s) {
            return chain.observeOn(SchedulerResolver.resolve(WorkScheduler.DATA_WRITE)).map(prev -> {
                s.task().accept(prev.context(), prev.value());
                Object nextCtx = s.contextMapper().apply(prev.context(), prev.value());
                return new PipelineResult<>(prev.value(), nextCtx, prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.ComputeStage s) {
            return chain.observeOn(SchedulerResolver.resolve(WorkScheduler.COMPUTE)).map(prev -> {
                Object res = s.task().apply(prev.context(), prev.value());
                Object nextCtx = s.contextMapper().apply(prev.context(), res);
                return new PipelineResult<>(res, nextCtx, prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.MainStage s) {
            return chain.observeOn(SchedulerResolver.resolve(WorkScheduler.MAIN)).map(prev -> {
                s.task().accept(prev.context(), prev.value());
                Object nextCtx = s.contextMapper().apply(prev.context(), prev.value());
                return new PipelineResult<>(prev.value(), nextCtx, prev.compensationStack());
            });
        } else if (stage instanceof PipelineStage.FinishStage s) {
            return chain.map(prev -> new PipelineResult<>(s.value(), prev.context(), prev.compensationStack()));
        } else if (stage instanceof PipelineStage.FailStage s) {
            return chain.flatMap(ignored -> Single.error(s.error()));
        } else if (stage instanceof PipelineStage.AsyncReadStage s) {
            return chain.flatMap(prev -> 
                s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_READ))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack()))
            );
        } else if (stage instanceof PipelineStage.AsyncWriteStage s) {
            return chain.flatMap(prev -> 
                s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.DATA_WRITE))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack()))
            );
        } else if (stage instanceof PipelineStage.AsyncIoStage s) {
            return chain.flatMap(prev -> 
                s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.IO))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack()))
            );
        } else if (stage instanceof PipelineStage.AsyncComputeStage s) {
            return chain.flatMap(prev -> 
                s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.COMPUTE))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack()))
            );
        } else if (stage instanceof PipelineStage.AsyncMainStage s) {
            return chain.flatMap(prev -> 
                s.task().apply(prev.context(), prev.value())
                    .subscribeOn(SchedulerResolver.resolve(WorkScheduler.MAIN))
                    .map(res -> new PipelineResult<>(res, s.contextMapper().apply(prev.context(), res), prev.compensationStack()))
            );
        } else if (stage instanceof PipelineStage.ChainStage s) {
            return chain.flatMap(prev -> {
                Work<?, ?> subWork = s.task().apply(prev.context(), prev.value());
                return compileInternal(subWork.getStages(), prev::context)
                    .map(subRes -> subRes.mergeCompensations(prev.compensationStack()));
            });
        } else {
            return chain.flatMap(ignored -> Single.error(new IllegalStateException("Unknown stage type: " + stage.getClass().getName())));
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

}
