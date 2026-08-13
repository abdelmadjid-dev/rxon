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

/**
 * Interface for library logging.
 */
public interface RxOnLogger {
    /**
     * Log an error.
     * @param tag tag
     * @param message message
     * @param t throwable
     */
    void e(String tag, String message, Throwable t);

    /**
     * Log a warning.
     * @param tag tag
     * @param message message
     * @param t throwable
     */
    void w(String tag, String message, Throwable t);

    /**
     * Log info.
     * @param tag tag
     * @param message message
     */
    void i(String tag, String message);

    /**
     * Log debug.
     * @param tag tag
     * @param message message
     */
    void d(String tag, String message);

    /**
     * Called when a pipeline stage starts.
     */
    default void onStageStart(String tag, int stageIndex, String stageDescription) {}

    /**
     * Called when a pipeline stage ends successfully.
     */
    default void onStageEnd(String tag, int stageIndex, String stageDescription, long durationMs) {}

    /**
     * Called when a pipeline finishes successfully.
     */
    default void onPipelineFinish(String tag, long totalDurationMs) {}

    /**
     * Called when a pipeline fails.
     */
    default void onPipelineError(String tag, Throwable error, long totalDurationMs) {}
}
