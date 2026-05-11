package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WorkFinalizationTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testFinish_MidPipeline_BypassesSubsequentStages() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        
        String result = Work.io(() -> "Step 1")
            .thenBreak("Finished Early")
            .thenCompute(val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Finished Early", result);
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testFail_MidPipeline_BypassesSubsequentStages() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        RuntimeException expectedError = new RuntimeException("Forced Failure");
        
        Work.io(() -> "Step 1")
            .then(Work.fail(expectedError))
            .thenCompute(val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && e != expectedError) e = e.getCause();
                return e == expectedError;
            });
            
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testFinish_InThenChain_TerminatesPipeline() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        
        String result = Work.io(() -> "Step 1")
            .thenChain(val -> Work.breakWork("Finished in Chain"))
            .thenCompute(val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Finished in Chain", result);
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testFail_InThenChain_TerminatesPipeline() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        RuntimeException expectedError = new RuntimeException("Fail in Chain");
        
        Work.io(() -> "Step 1")
            .thenChain(val -> Work.fail(expectedError))
            .thenCompute(val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && e != expectedError) e = e.getCause();
                return e == expectedError;
            });
            
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testCompensation_ExecutionOnFailure() {
        AtomicInteger compensationRunCount = new AtomicInteger(0);
        Work<Done, Object> compensation = Work.write((Runnable) compensationRunCount::incrementAndGet);
        
        Work.io(() -> "Action")
            .compensate(compensation)
            .then(Work.fail(new RuntimeException("Trigger Compensation")))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(RuntimeException.class);
            
        assertEquals("Compensation should have run on failure", 1, compensationRunCount.get());
    }

    @Test
    public void testCompensation_LIFO_Order() {
        java.util.List<Integer> order = new java.util.ArrayList<>();
        
        Work.io(() -> "Action 1")
            .compensate(Work.write((Runnable) () -> order.add(1)))
            .thenCompute(val -> "Action 2")
            .compensate(Work.write((Runnable) () -> order.add(2)))
            .then(Work.fail(new RuntimeException("Rollback")))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS);
            
        assertEquals(java.util.Arrays.asList(2, 1), order);
    }

    @Test
    public void testCompensation_AccessesCurrentContext() {
        AtomicInteger contextValue = new AtomicInteger(0);
        
        Work.withContext(() -> 100)
            .thenCompute(val -> val) // A stage to attach compensation to
            .compensate(Work.write((Runnable) () -> {}).peekChain((ctx, val) -> {
                contextValue.set((Integer) ctx);
                return Work.breakWork(Done.INSTANCE);
            }))
            .then(Work.fail(new RuntimeException("Trigger")))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS);
            
        assertEquals(100, contextValue.get());
    }

    @Test
    public void testFinish_AsyncStage_BypassesSubsequent() {
        AtomicBoolean subsequentRun = new AtomicBoolean(false);
        
        Work.io(() -> "Start")
            .thenBreak("Done")
            .thenIoSingle(val -> {
                subsequentRun.set(true);
                return io.reactivex.rxjava3.core.Single.just("Async");
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertFalse("Subsequent async stage should be bypassed", subsequentRun.get());
    }

    @Test
    public void testFinish_WithOptionalEmpty() {
        java.util.Optional<Object> result = Work.breakWork(java.util.Optional.empty())
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals(java.util.Optional.empty(), result);
    }

    @Test
    public void testBreak_CanBeRecovered() {
        AtomicBoolean postRecoveryRun = new AtomicBoolean(false);
        
        String result = Work.io(() -> "Start")
            .thenBreak("Broken")
            .thenCompute(val -> {
                throw new RuntimeException("Should not run");
            })
            .recoverBreak("Recovered")
            .thenCompute(val -> {
                postRecoveryRun.set(true);
                return val + " and Continued";
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Recovered and Continued", result);
        assertTrue("Stages after recovery should have run", postRecoveryRun.get());
    }

    @Test
    public void testContinueWork() {
        String result = Work.continueWork("Direct Value")
            .thenCompute(val -> val + "!")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Direct Value!", result);
    }

    @Test
    public void testFail_CannotBeRecoveredByRecoverBreak() {
        AtomicBoolean recoveryStageRun = new AtomicBoolean(false);
        RuntimeException expectedError = new RuntimeException("Fatal");
        
        Work.fail(expectedError)
            .recoverBreak("Should not recover")
            .thenCompute(val -> {
                recoveryStageRun.set(true);
                return val;
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && e != expectedError) e = e.getCause();
                return e == expectedError;
            });
            
        assertFalse("Recovery should not run for Fail", recoveryStageRun.get());
    }
}
