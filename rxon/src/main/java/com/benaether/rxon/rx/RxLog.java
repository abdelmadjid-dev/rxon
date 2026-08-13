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

/**
 * Internal logger wrapper that handles root cause extraction for RxJava assembly exceptions.
 */
public final class RxLog {

    private RxLog() {
    }

    /**
     * Log an error.
     * @param tag tag
     * @param message message
     * @param throwable throwable
     */
    public static void e(String tag, String message, Throwable throwable) {
        RxOnConfig.getLogger().e(tag, message, RxOnConfig.mapError(throwable));
    }

    /**
     * Log a warning.
     * @param tag tag
     * @param message message
     * @param throwable throwable
     */
    public static void w(String tag, String message, Throwable throwable) {
        RxOnConfig.getLogger().w(tag, message, RxOnConfig.mapError(throwable));
    }

    /**
     * Log info.
     * @param tag tag
     * @param message message
     */
    public static void i(String tag, String message) {
        RxOnConfig.getLogger().i(tag, message);
    }

    /**
     * Log debug.
     * @param tag tag
     * @param message message
     */
    public static void d(String tag, String message) {
        RxOnConfig.getLogger().d(tag, message);
    }
}
