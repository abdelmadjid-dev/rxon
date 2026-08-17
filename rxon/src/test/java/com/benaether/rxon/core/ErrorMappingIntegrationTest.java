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

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Flowable;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class ErrorMappingIntegrationTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .cleanStackTrace(true)
            .errorMapper(t -> new IllegalStateException("Mapped: " + t.getMessage(), t))
            .init();
    }

    @Test
    public void testWork_ErrorMappingAndCleaning() {
        Throwable[] receivedError = new Throwable[1];

        Work.callable(WorkScheduler.IO, () -> {
            throw new RuntimeException("Original Work Failure");
        })
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertError(t -> {
            receivedError[0] = t;
            return t instanceof IllegalStateException && t.getMessage().contains("Original Work Failure");
        });

        assertNotNull(receivedError[0]);
        Throwable cause = receivedError[0].getCause();
        if (cause != null) {
            assertFalse("Cause should not be RxJavaAssemblyException",
                    cause.getClass().getName().contains("AssemblyException"));
        }
    }

    @Test
    public void testObserve_ErrorMappingAndCleaning() {
        Throwable[] receivedError = new Throwable[1];

        Observe.flow(WorkScheduler.IO, Flowable.error(new RuntimeException("Original Observe Failure")))
            .thenFunction(WorkScheduler.COMPUTE, val -> val)
            .asFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(t -> {
                receivedError[0] = t;
                return t instanceof IllegalStateException && t.getMessage().contains("Mapped: Original Observe Failure");
            });

        assertNotNull(receivedError[0]);
        Throwable cause = receivedError[0].getCause();
        if (cause != null) {
            assertFalse("Cause should not be RxJavaAssemblyException",
                    cause.getClass().getName().contains("AssemblyException"));
        }
    }

    @Test
    public void testWorkWithCompensations_ErrorMappingAndCleaning() {
        AtomicBoolean compensationExecuted = new AtomicBoolean(false);
        Work<com.benaether.rxon.scopes.Done> compWork = Work.action(WorkScheduler.IO, (Runnable) () -> compensationExecuted.set(true));

        Work.callable(WorkScheduler.IO, () -> "Step 1")
            .compensate(compWork)
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                throw new RuntimeException("Operation Timed Out");
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(t -> t instanceof IllegalStateException && t.getMessage().contains("Operation Timed Out"));

        assertTrue("Compensation should have executed", compensationExecuted.get());
    }

    @Test
    public void testSingleEmitter_ErrorMappingAndCleaning() {
        Throwable[] receivedError = new Throwable[1];

        Work.singleEmitter(WorkScheduler.IO, emitter -> {
            emitter.onError(new RuntimeException("SingleEmitter explicit error"));
        })
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertError(t -> {
            receivedError[0] = t;
            return t instanceof IllegalStateException && t.getMessage().contains("SingleEmitter explicit error");
        });

        assertNotNull(receivedError[0]);
        Throwable cause = receivedError[0].getCause();
        if (cause != null) {
            assertFalse("Cause should not be RxJavaAssemblyException",
                    cause.getClass().getName().contains("AssemblyException"));
        }
    }

    @Test
    public void testCompletableEmitter_ErrorMappingAndCleaning() {
        Throwable[] receivedError = new Throwable[1];

        Work.completableEmitter(WorkScheduler.IO, emitter -> {
            throw new RuntimeException("CompletableEmitter thrown error");
        })
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertError(t -> {
            receivedError[0] = t;
            return t instanceof IllegalStateException && t.getMessage().contains("CompletableEmitter thrown error");
        });

        assertNotNull(receivedError[0]);
        Throwable cause = receivedError[0].getCause();
        if (cause != null) {
            assertFalse("Cause should not be RxJavaAssemblyException",
                    cause.getClass().getName().contains("AssemblyException"));
        }
    }
}
