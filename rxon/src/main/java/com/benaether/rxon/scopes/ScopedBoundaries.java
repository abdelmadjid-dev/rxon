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

package com.benaether.rxon.scopes;

import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;

public class ScopedBoundaries {
    // =========================
    // ASYNC EXTERNAL (flatMap)
    // =========================

    @FunctionalInterface
    public interface AsyncScope<T, R> {
        Single<R> apply(T t);
    }

    @FunctionalInterface
    public interface AsyncCompletableScope<T> {
        Completable apply(T t);
    }

    // =========================
    // ASYNC EXTERNAL — ZERO INPUT
    // =========================

    public interface AsyncScope0<R> {
        Single<R> apply();
    }

    // =========================
    // STREAM ASYNC BOUNDARIES
    // =========================

    @FunctionalInterface
    public interface StreamAsyncScope<T, R> {
        Publisher<R> apply(T t);
    }

}

