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

import com.benaether.rxon.rx.DefaultRxOnLogger;
import com.benaether.rxon.rx.RxOnLogger;
import com.benaether.rxon.rx.RxOnMonitoring;
import com.benaether.rxon.schedulers.DefaultSchedulers;
import com.benaether.rxon.schedulers.WorkScheduler;

import java.util.EnumMap;
import java.util.Map;

import io.reactivex.rxjava3.core.Scheduler;

/**
 * Main configuration class for the RxOn library.
 * Use the {@link #builder()} to initialize the library.
 */
public final class RxOnConfig {
    private static RxOnErrorMapper rxOnErrorMapper = t -> t;
    private static final Map<WorkScheduler, Scheduler> customSchedulers =
            new EnumMap<>(WorkScheduler.class);
    private static boolean debug = false;
    private static RxOnLogger rxOnLogger = new DefaultRxOnLogger();

    private RxOnConfig() {}

    /**
     * Map an error using the configured error mapper.
     * @param t throwable
     * @return mapped throwable
     */
    public static Throwable mapError(Throwable t) {
        return rxOnErrorMapper.map(t);
    }

    /** @return true if debug mode is enabled */
    public static boolean isDebug() { return debug; }

    /** @return the configured logger */
    public static RxOnLogger getLogger() { return rxOnLogger; }

    /**
     * Get the scheduler for the given type.
     * @param type scheduler type
     * @return RxJava scheduler
     */
    public static Scheduler getScheduler(WorkScheduler type) {
        return customSchedulers.computeIfAbsent(type, t -> switch (t) {
            case DATA_READ -> DefaultSchedulers.dataRead();
            case DATA_WRITE -> DefaultSchedulers.dataWrite();
            case IO -> DefaultSchedulers.io();
            case COMPUTE -> DefaultSchedulers.compute();
            case MAIN -> DefaultSchedulers.main();
        });
    }

    /** Step for debug configuration */
    public interface DebugStep {
        /** @param isDebug enable debug mode
         * @return next step */
        LoggerStep debug(boolean isDebug);
    }

    /** Step for logger configuration */
    public interface LoggerStep {
        /** @param rxOnLogger custom logger
         * @return next step */
        ErrorMapperStep logger(RxOnLogger rxOnLogger);
    }

    /** Step for error mapper configuration */
    public interface ErrorMapperStep {
        /** @param mapper custom error mapper
         * @return next step */
        BuildStep errorMapper(RxOnErrorMapper mapper);
    }

    /** Step for final configuration and initialization */
    public interface BuildStep {
        /** @param type scheduler type
         * @param scheduler custom scheduler
         * @return this step */
        BuildStep scheduler(WorkScheduler type, Scheduler scheduler);
        /** Initialize the library with the configured values */
        void init();
    }

    /** @return a new builder instance */
    public static DebugStep builder() {
        return new Builder();
    }

    private static class Builder implements DebugStep, LoggerStep, ErrorMapperStep, BuildStep {
        private boolean isDebug;
        private RxOnLogger rxOnLogger;
        private RxOnErrorMapper rxOnErrorMapper;
        private final Map<WorkScheduler, Scheduler> customSchedulers = new EnumMap<>(WorkScheduler.class);

        @Override
        public LoggerStep debug(boolean isDebug) {
            this.isDebug = isDebug;
            return this;
        }

        @Override
        public ErrorMapperStep logger(RxOnLogger rxOnLogger) {
            this.rxOnLogger = rxOnLogger;
            return this;
        }

        @Override
        public BuildStep errorMapper(RxOnErrorMapper mapper) {
            this.rxOnErrorMapper = mapper;
            return this;
        }

        @Override
        public BuildStep scheduler(WorkScheduler type, Scheduler scheduler) {
            customSchedulers.put(type, scheduler);
            return this;
        }

        @Override
        public void init() {
            RxOnConfig.debug = isDebug;
            RxOnConfig.rxOnLogger = rxOnLogger != null ? rxOnLogger : new DefaultRxOnLogger();
            RxOnConfig.rxOnErrorMapper = rxOnErrorMapper != null ? rxOnErrorMapper : t -> t;
            RxOnConfig.customSchedulers.putAll(customSchedulers);
            RxOnMonitoring.init();
        }
    }
}

