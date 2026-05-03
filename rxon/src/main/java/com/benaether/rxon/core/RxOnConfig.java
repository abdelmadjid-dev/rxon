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

public final class RxOnConfig {
    private static RxOnErrorMapper rxOnErrorMapper = t -> t;
    private static final Map<WorkScheduler, Scheduler> customSchedulers =
            new EnumMap<>(WorkScheduler.class);
    private static boolean debug = false;
    private static RxOnLogger rxOnLogger = new DefaultRxOnLogger();

    private RxOnConfig() {}

    public static Throwable mapError(Throwable t) {
        return rxOnErrorMapper.map(t);
    }

    public static boolean isDebug() { return debug; }

    public static RxOnLogger getLogger() { return rxOnLogger; }

    public static Scheduler getScheduler(WorkScheduler type) {
        return customSchedulers.computeIfAbsent(type, t -> switch (t) {
            case DATA_READ -> DefaultSchedulers.dataRead();
            case DATA_WRITE -> DefaultSchedulers.dataWrite();
            case IO -> DefaultSchedulers.io();
            case COMPUTE -> DefaultSchedulers.compute();
            case MAIN -> DefaultSchedulers.main();
        });
    }

    public interface DebugStep {
        LoggerStep debug(boolean isDebug);
    }

    public interface LoggerStep {
        ErrorMapperStep logger(RxOnLogger rxOnLogger);
    }

    public interface ErrorMapperStep {
        BuildStep errorMapper(RxOnErrorMapper mapper);
    }

    public interface BuildStep {
        BuildStep scheduler(WorkScheduler type, Scheduler scheduler);
        void init();
    }

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

