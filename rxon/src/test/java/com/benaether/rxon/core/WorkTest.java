package com.benaether.rxon.core;

import static org.junit.Assert.assertEquals;

import com.benaether.rxon.scopes.Done;
import com.benaether.rxon.schedulers.WorkScheduler;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link Work} showcasing context passing and pipeline orchestration.
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

    // ===========================================================================================
    // CONTEXT PASSING
    // ===========================================================================================

    @Test
    public void testContextPropagation() {
        Work.withContext(() -> "Initial")
            .thenCallable(WorkScheduler.DATA_READ, () -> "Data")
            .thenFunction(WorkScheduler.COMPUTE, (ctx, data) -> ctx + ":" + data, (ctx, res) -> res)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Initial:Data")
            .assertComplete();
    }

    @Test
    public void testContextMutation() {
        class MutableCtx {
            int count = 0;
            void inc() { count++; }
        }

        MutableCtx ctx = new MutableCtx();

        Work.withContext(() -> ctx)
            .thenFunction(WorkScheduler.COMPUTE, (c, val) -> {
                c.inc();
                return "Step 1";
            }, (c, res) -> c)
            .thenFunction(WorkScheduler.COMPUTE, (c, val) -> {
                c.inc();
                return "Step 2";
            }, (c, res) -> c)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Step 2")
            .assertComplete();

        assertEquals(2, ctx.count);
    }

    @Test
    public void testContextUpdate_Replacement() {
        Work.withContext(() -> 1)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val, (ctx, res) -> ctx + 10)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> "Value: " + ctx, (ctx, res) -> ctx)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Value: 11")
            .assertComplete();
    }

    @Test
    public void testContextSwitching() {
        // Start with no context (Object)
        Work.callable(WorkScheduler.IO, () -> "Hello")
            .usingContext(val -> 100) // Switch to Integer context based on value
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val + " " + ctx, (ctx, res) -> ctx)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Hello 100")
            .assertComplete();
    }

    @Test
    public void testContextTypeUpdate() {
        // Start with Integer context
        Work.withContext(() -> 100)
            .updateContext((ctx, val) -> {
                class MutableCtx {
                    String val = "";
                }
                MutableCtx newCtx = new MutableCtx();
                newCtx.val = "StringCtx:" + ctx;
                return newCtx;
            }) // Change to String context
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> "Value is " + ctx.val, (ctx, res) -> ctx)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Value is StringCtx:100")
            .assertComplete();
    }

    // ===========================================================================================
    // PIPELINE BASICS
    // ===========================================================================================

    @Test
    public void testSimplePipeline_EmitsTransformedValue() {
        Work.callable(WorkScheduler.IO, () -> 10)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, i) -> i * 2)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, i) -> "Result: " + i)
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
            .thenFunction(WorkScheduler.COMPUTE, (ctx, i) -> { throw expectedError; })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                // Handle possible wrapping
                while (e.getCause() != null && e != expectedError) {
                    e = e.getCause();
                }
                return e == expectedError;
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

    // ===========================================================================================
    // TERMINATION & FINISHING
    // ===========================================================================================

    @Test
    public void testBreak_ImmediatelyTerminatesSuccessfully() {
        Work.breakWork("Early Return")
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

    // ===========================================================================================
    // GUARDS
    // ===========================================================================================

    @Test
    public void testRequire_Success() {
        Work.callable(WorkScheduler.IO, () -> 10)
            .require(i -> i > 5, i -> new RuntimeException("Too small"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(10);
    }

    @Test
    public void testRequire_Failure() {
        Work.callable(WorkScheduler.IO, () -> 3)
            .require(i -> i > 5, i -> new RuntimeException("Too small"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && !"Too small".equals(e.getMessage())) {
                    e = e.getCause();
                }
                return "Too small".equals(e.getMessage());
            });
    }

    @Test
    public void testLazyContextInitialization() {
        AtomicInteger initCount = new AtomicInteger(0);
        Work<String, Integer> work = Work.withContext(() -> {
            initCount.incrementAndGet();
            return 10;
        }).thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> "Value:" + ctx, (ctx, res) -> ctx);

        assertEquals(0, initCount.get()); // Not initialized yet

        work.asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Value:10");

        assertEquals(1, initCount.get()); // Initialized once
    }
}
