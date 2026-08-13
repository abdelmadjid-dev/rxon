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
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;

/**
 * Centralized subscriber helper that handles error mapping and stack trace cleaning across all RxOn terminal operations.
 */
final class PipelineSubscriber {

    private PipelineSubscriber() {}

    static <T> Disposable subscribe(Single<T> single, Consumer<T> onSuccess, Consumer<Throwable> onError, String tag) {
        return single.subscribe(
            onSuccess != null ? onSuccess : t -> {},
            wrapOnError(onError, tag)
        );
    }

    static <T> Disposable subscribe(Flowable<T> flowable, Consumer<T> onNext, Consumer<Throwable> onError, String tag) {
        return flowable.subscribe(
            onNext != null ? onNext : t -> {},
            wrapOnError(onError, tag)
        );
    }

    static Consumer<Throwable> wrapOnError(Consumer<Throwable> onError, String tag) {
        return throwable -> {
            Throwable cleaned = RxOnConfig.mapError(throwable);
            if (onError != null) {
                onError.accept(cleaned);
            } else {
                RxLog.e(tag != null ? tag : "RxOn", "Unhandled Throwable", cleaned);
            }
        };
    }
}
