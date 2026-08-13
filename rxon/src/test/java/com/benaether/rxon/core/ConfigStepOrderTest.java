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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.benaether.rxon.rx.RxOnLogger;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigStepOrderTest {

    private static final RxOnLogger DUMMY_LOGGER = new RxOnLogger() {
        @Override public void e(String tag, String message, Throwable t) {}
        @Override public void w(String tag, String message, Throwable t) {}
        @Override public void i(String tag, String message) {}
        @Override public void d(String tag, String message) {}
    };

    @Test
    public void testBuilderStepOrderAndExclusivity() {
        AtomicBoolean mapperCalled = new AtomicBoolean(false);
        
        RxOnConfig.builder()
                .debug(true)
                .logger(DUMMY_LOGGER)
                .cleanStackTrace(true)
                .errorMapper(new RxOnErrorMapper() {
                    @Override
                    public Throwable map(Throwable t) {
                        mapperCalled.set(true);
                        assertNull("RxJavaAssemblyException cause should be unlinked before reaching mapper", t.getCause());
                        return t;
                    }
                })
                .init();

        Throwable assemblyWrapper = new StackTraceCleanerTest.DummyAssemblyException("assembled", null);
        Throwable noiseException = new RuntimeException("test", assemblyWrapper);

        RxOnConfig.mapError(noiseException);
        assertTrue("Mapper should have been called", mapperCalled.get());
    }

    @Test
    public void testOnlyCleanStackTrace() {
        RxOnConfig.builder()
                .debug(true)
                .logger(DUMMY_LOGGER)
                .cleanStackTrace(true)
                .init();
    }

    @Test
    public void testOnlyErrorMapper() {
        RxOnConfig.builder()
                .debug(true)
                .logger(DUMMY_LOGGER)
                .errorMapper(t -> t)
                .init();
    }
}
