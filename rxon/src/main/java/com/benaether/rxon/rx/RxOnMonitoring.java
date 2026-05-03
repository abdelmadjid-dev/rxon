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

package com.benaether.rxon.rx;

import com.benaether.rxon.core.RxOnConfig;

import hu.akarnokd.rxjava3.debug.RxJavaAssemblyTracking;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

public final class RxOnMonitoring {
    private static final String TAG = RxOnMonitoring.class.getName();
    private static final long SLOW_TASK_MS = 100;

    private RxOnMonitoring() {}

    public static void init() {
        if (!RxOnConfig.isDebug()) {
            return;
        }

        // Enable assembly tracking with the extensions library
        RxJavaAssemblyTracking.enable();

        // Global error handler
        RxJavaPlugins.setErrorHandler(throwable -> {
            RxOnConfig.getLogger().e(TAG, "Unhandled RxJava error", throwable);
        });

        // Monitor slow tasks
        RxJavaPlugins.setScheduleHandler(runnable -> () -> {
            long start = System.currentTimeMillis();
            try {
                runnable.run();
            } finally {
                long end = System.currentTimeMillis();
                long duration = end - start;

                if (duration > SLOW_TASK_MS) {
                    RxOnConfig.getLogger().w(
                            TAG,
                            "Scheduled task took " + duration + " ms on thread "
                                    + Thread.currentThread().getName(),
                            null
                    );
                }
            }
        });
    }


}

