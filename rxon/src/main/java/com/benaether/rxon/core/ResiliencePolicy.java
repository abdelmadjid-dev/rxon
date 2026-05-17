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

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Defines resilience policies for RxOn Pipeline stages.
 */
public final class ResiliencePolicy {

    public enum BackoffStrategy {
        LINEAR,
        EXPONENTIAL
    }

    public record RetryPolicy(
        int maxRetries,
        long delayMs,
        BackoffStrategy backoff,
        Predicate<Throwable> condition
    ) {
        public RetryPolicy(int maxRetries, long delayMs, BackoffStrategy backoff) {
            this(maxRetries, delayMs, backoff, err -> true);
        }
    }

    public record TimeoutPolicy(
        long duration,
        TimeUnit unit
    ) {}

    /**
     * Container for all resilience metadata attached to a stage.
     */
    public record ResilienceMetadata(
        RetryPolicy retry,
        TimeoutPolicy timeout,
        Work<?> fallback,
        Work<?> compensation
    ) {
        public static final ResilienceMetadata EMPTY = new ResilienceMetadata(null, null, null, null);

        public boolean hasRetry() { return retry != null; }
        public boolean hasTimeout() { return timeout != null; }
        public boolean hasFallback() { return fallback != null; }
        public boolean hasCompensation() { return compensation != null; }
    }

    public static final class ResilienceBuilder {
        private RetryPolicy retry;
        private TimeoutPolicy timeout;
        private Work<?> fallback;
        private Work<?> compensation;

        public ResilienceBuilder retry(int max) { return retry(max, 0, BackoffStrategy.LINEAR); }
        public ResilienceBuilder retry(int max, long delay, BackoffStrategy strategy) {
            this.retry = new RetryPolicy(max, delay, strategy);
            return this;
        }

        public ResilienceBuilder retryIf(int max, long delay, BackoffStrategy strategy, Predicate<Throwable> condition) {
            this.retry = new RetryPolicy(max, delay, strategy, condition);
            return this;
        }

        public ResilienceBuilder timeout(long duration, TimeUnit unit) {
            this.timeout = new TimeoutPolicy(duration, unit);
            return this;
        }

        public ResilienceBuilder fallback(Work<?> fallback) {
            this.fallback = fallback;
            return this;
        }

        public ResilienceBuilder compensate(Work<?> compensation) {
            this.compensation = compensation;
            return this;
        }

        ResilienceMetadata build() {
            return new ResilienceMetadata(retry, timeout, fallback, compensation);
        }
    }
}
