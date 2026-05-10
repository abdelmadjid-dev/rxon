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
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;

import java.util.concurrent.TimeUnit;

/**
 * {@code Observe<T>} is a signal orchestration component.
 * It is dedicated to observing asynchronous event sources, conditioning the signal
 * (filtering noise, debouncing), and triggering {@link Work} pipelines.
 *
 * <p>Unlike {@link Work}, which handles business logic and state transformation,
 * an {@code Observe} component is strictly for <b>event management</b>. It ensures that 
 * signals are clean and correctly timed before initiating a pipeline execution.</p>
 *
 * @param <T> signal type
 */
public final class Observe<T> {

    private static final String TAG = Observe.class.getName();

    private final Flowable<T> flowable;

    private Observe(Flowable<T> flowable) {
        this.flowable = flowable;
    }

    /**
     * Observe a reactive source.
     */
    public static <T> Observe<T> flow(Flowable<T> source) {
        return new Observe<>(source.onErrorResumeNext(e -> 
            Flowable.error(RxOnConfig.mapError(e))
        ));
    }

    /**
     * Filter out signals that are emitted too rapidly after each other.
     * Use this to ignore "noise" or jitter in high-frequency signal sources.
     */
    public Observe<T> debounce(long time, TimeUnit unit) {
        return new Observe<>(flowable.debounce(time, unit));
    }

    /**
     * Take the first signal in a window and ignore subsequent ones.
     * Useful for preventing double-triggers from UI events or hardware interrupts.
     */
    public Observe<T> throttle(long time, TimeUnit unit) {
        return new Observe<>(flowable.throttleFirst(time, unit));
    }

    /**
     * Only emit signals when the observed value actually changes.
     */
    public Observe<T> distinct() {
        return new Observe<>(flowable.distinctUntilChanged());
    }

    /**
     * Trigger a {@link Work} pipeline whenever a conditioned signal is emitted.
     * The pipeline will be compiled and executed independently for each signal.
     *
     * @param work the business logic pipeline to execute
     * @return a Disposable to manage the observation lifecycle
     */
    public Disposable trigger(Work<?, ?> work) {
        return flowable.subscribe(
            signal -> {
                work.execute();
            },
            throwable -> RxLog.e(TAG, "Observe error", throwable)
        );
    }

    /**
     * Trigger a {@link Work} pipeline and observe its final results.
     *
     * @param work the business logic pipeline to execute
     * @param onSuccess called with the final emission of the work pipeline
     * @param onError called if the signal source or the work pipeline fails
     * @return a Disposable to manage the observation lifecycle
     */
    public <R> Disposable trigger(Work<R, ?> work, Consumer<R> onSuccess, Consumer<Throwable> onError) {
        return flowable.flatMapSingle(signal -> 
            work.asTerminalSingle()
        ).subscribe(onSuccess, onError);
    }

    /**
     * Bridge back to standard RxJava Flowable for external integration.
     */
    public Flowable<T> asFlowable() {
        return flowable;
    }
}
