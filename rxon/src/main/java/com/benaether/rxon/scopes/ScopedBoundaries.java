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

/**
 * Functional interfaces for asynchronous boundaries.
 */
public class ScopedBoundaries {
    // =========================
    // ASYNC EXTERNAL (flatMap)
    // =========================

    /**
     * Functional interface for an async scope with input.
     * @param <T> input type
     * @param <R> output type
     */
    @FunctionalInterface
    public interface AsyncScope<T, R> {
        /** @param t input @return RxJava Single */
        Single<R> apply(T t);
    }

    /**
     * Functional interface for an async completable scope with input.
     * @param <T> input type
     */
    @FunctionalInterface
    public interface AsyncCompletableScope<T> {
        /** @param t input @return RxJava Completable */
        Completable apply(T t);
    }

    // =========================
    // ASYNC EXTERNAL — ZERO INPUT
    // =========================

    /**
     * Functional interface for an async scope with no input.
     * @param <R> output type
     */
    public interface AsyncScope0<R> {
        /** @return RxJava Single */
        Single<R> apply();
    }

    // =========================
    // STREAM ASYNC BOUNDARIES
    // =========================

    /**
     * Functional interface for a stream async scope.
     * @param <T> input type
     * @param <R> output type
     */
    @FunctionalInterface
    public interface StreamAsyncScope<T, R> {
        /** @param t input @return Reactive Publisher */
        Publisher<R> apply(T t);
    }

}

