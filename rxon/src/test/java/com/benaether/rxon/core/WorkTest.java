package com.benaether.rxon.core;

import static org.junit.Assert.assertEquals;

import com.benaether.rxon.scopes.Done;
import com.benaether.rxon.schedulers.WorkScheduler;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link Work} showcasing pipeline orchestration.
 */
public class WorkTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testSimplePipeline_EmitsTransformedValue() {
        Work.callable(WorkScheduler.IO, () -> 10)
            .thenFunction(WorkScheduler.COMPUTE, i -> i * 2)
            .thenFunction(WorkScheduler.COMPUTE, i -> "Result: " + i)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Result: 20")
            .assertComplete();
    }

    @Test
    public void testPipelineFailure_PropagatesError() {
        RuntimeException expectedError = new RuntimeException("Something went wrong");
        
        Work.callable(WorkScheduler.IO, () -> 10)
            .thenFunction(WorkScheduler.COMPUTE, i -> { throw expectedError; })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != expectedError) {
                    current = current.getCause();
                }
                return current == expectedError;
            })
            .assertNotComplete();
    }

    @Test
    public void testStartFromWrite_EmitsDone() {
        AtomicInteger counter = new AtomicInteger(0);
        
        Work.action(WorkScheduler.DATA_WRITE, (Runnable) counter::incrementAndGet)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(Done.INSTANCE);
            
        assertEquals(1, counter.get());
    }

    @Test
    public void testBreak_ImmediatelyTerminatesSuccessfully() {
        Work.just(WorkScheduler.COMPUTE, "Early Return")
            .thenBreak("Early Return")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Early Return");
    }

    @Test
    public void testFail_ImmediatelyTerminatesWithError() {
        RuntimeException error = new RuntimeException("Fail Fast");
        
        Work.fail(error)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(error);
    }

    @Test
    public void testRequire_Success() {
        Work.callable(WorkScheduler.IO, () -> 10)
            .require(WorkScheduler.COMPUTE, i -> i > 5, i -> new RuntimeException("Too small"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(10);
    }

    @Test
    public void testRequire_Failure() {
        Work.callable(WorkScheduler.IO, () -> 3)
            .require(WorkScheduler.COMPUTE, i -> i > 5, i -> new RuntimeException("Too small"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && !"Too small".equals(current.getMessage())) {
                    current = current.getCause();
                }
                return "Too small".equals(current.getMessage());
            });
    }
}
