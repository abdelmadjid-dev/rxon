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

package com.benaether.rxon.schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Default internal schedulers used by the library.
 */
public final class DefaultSchedulers {
    private DefaultSchedulers() {}

    private static class DataWriteHolder {
        private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "data-write");
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        });
        static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR);
    }

    private static class DataReadHolder {
        private static final int THREADS = Math.min(3, Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
        private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(THREADS, r -> {
            Thread t = new Thread(r, "data-read");
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        });
        static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR);
    }

    private static class IoHolder {
        private static final int THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
        private static final ExecutorService EXECUTOR = new ThreadPoolExecutor(
                THREADS, THREADS, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r, "io-thread");
                    t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
        );
        static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR);
    }

    private static class ComputeHolder {
        private static final int CPU = Runtime.getRuntime().availableProcessors();
        private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(
                Math.max(1, CPU - 1),
                r -> {
                    Thread t = new Thread(r, "compute-thread");
                    t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
        );
        static final Scheduler SCHEDULER = Schedulers.from(EXECUTOR);
    }

    /** @return data write scheduler */
    public static Scheduler dataWrite() { return DataWriteHolder.SCHEDULER; }
    /** @return data read scheduler */
    public static Scheduler dataRead() { return DataReadHolder.SCHEDULER; }
    /** @return IO scheduler */
    public static Scheduler io() { return IoHolder.SCHEDULER; }
    /** @return compute scheduler */
    public static Scheduler compute() { return ComputeHolder.SCHEDULER; }
    /** @return main thread scheduler */
    public static Scheduler main() { return Schedulers.trampoline(); }
}

