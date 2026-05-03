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

import io.reactivex.rxjava3.functions.Function;

/**
 * Functional interfaces for synchronous operations that can throw exceptions.
 */
public class ScopedFunctions {


    /**
     * Functional interface for a function that can throw an exception.
     * @param <T> input type
     * @param <R> output type
     */
    @FunctionalInterface
    public interface ThrowingFn<T, R> extends Function<T, R> {
        /**
         * Apply the function.
         * @param t input
         * @return output
         * @throws Exception if something goes wrong
         */
        R apply(T t) throws Exception;
    }

    /**
     * Functional interface for a function that returns nothing and can throw an exception.
     * @param <T> input type
     */
    @FunctionalInterface
    public interface ThrowingUnitFn<T> {
        /**
         * Apply the function.
         * @param t input
         * @throws Exception if something goes wrong
         */
        void apply(T t) throws Exception;
    }

}

