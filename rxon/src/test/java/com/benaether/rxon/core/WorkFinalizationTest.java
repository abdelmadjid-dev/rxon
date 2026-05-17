package com.benaether.rxon.core;

import com.benaether.rxon.scopes.Done;
import com.benaether.rxon.schedulers.WorkScheduler;
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
    public void testBreak_MidPipeline_BypassesSubsequentStages() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        
        String result = Work.<String>callable(WorkScheduler.IO, () -> "Step 1")
            .thenBreak("Finished Early")
            .thenFunction(WorkScheduler.COMPUTE, val -> {
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
        
        Work.<String>callable(WorkScheduler.IO, () -> "Step 1")
            .thenChain(val -> Work.fail(expectedError))
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != expectedError) current = current.getCause();
                return current == expectedError;
            });
            
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testBreak_InThenChain_TerminatesPipeline() {
        AtomicBoolean subsequentStageRun = new AtomicBoolean(false);
        
        String result = Work.<String>callable(WorkScheduler.IO, () -> "Step 1")
            .thenChain(val -> Work.just(WorkScheduler.COMPUTE, "Finished in Chain").thenBreak("Finished in Chain"))
            .thenFunction(WorkScheduler.COMPUTE, val -> {
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
        
        Work.<String>callable(WorkScheduler.IO, () -> "Step 1")
            .thenChain(val -> Work.fail(expectedError))
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                subsequentStageRun.set(true);
                return val + " -> Step 2";
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != expectedError) current = current.getCause();
                return current == expectedError;
            });
            
        assertFalse("Subsequent stage should not have run", subsequentStageRun.get());
    }

    @Test
    public void testCompensation_ExecutionOnFailure() {
        AtomicInteger compensationRunCount = new AtomicInteger(0);
        Work<Done> compensation = Work.action(WorkScheduler.DATA_WRITE, (Runnable) compensationRunCount::incrementAndGet);
        
        Work.callable(WorkScheduler.IO, () -> "Action")
            .compensate(compensation)
            .thenChain(val -> Work.fail(new RuntimeException("Trigger Compensation")))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(RuntimeException.class);
            
        assertEquals("Compensation should have run on failure", 1, compensationRunCount.get());
    }

    @Test
    public void testCompensation_LIFO_Order() {
        java.util.List<Integer> order = new java.util.ArrayList<>();
        
        Work.callable(WorkScheduler.IO, () -> "Action 1")
            .compensate(Work.action(WorkScheduler.DATA_WRITE, (Runnable) () -> order.add(1)))
            .thenFunction(WorkScheduler.COMPUTE, val -> "Action 2")
            .compensate(Work.action(WorkScheduler.DATA_WRITE, (Runnable) () -> order.add(2)))
            .thenChain(val -> Work.fail(new RuntimeException("Rollback")))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS);
            
        assertEquals(java.util.Arrays.asList(2, 1), order);
    }

    @Test
    public void testBreak_AsyncStage_BypassesSubsequent() {
        AtomicBoolean subsequentRun = new AtomicBoolean(false);
        
        Work.<String>callable(WorkScheduler.IO, () -> "Start")
            .thenBreak("Done")
            .thenSingle(WorkScheduler.IO, val -> {
                subsequentRun.set(true);
                return io.reactivex.rxjava3.core.Single.just("Async");
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertFalse("Subsequent async stage should be bypassed", subsequentRun.get());
    }

    @Test
    public void testBreak_CanBeRecovered() {
        AtomicBoolean postRecoveryRun = new AtomicBoolean(false);
        
        String result = Work.<String>callable(WorkScheduler.IO, () -> "Start")
            .thenBreak("Broken")
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                throw new RuntimeException("Should not run");
            })
            .recoverBreak("Recovered")
            .thenFunction(WorkScheduler.COMPUTE, val -> {
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
        String result = Work.just(WorkScheduler.COMPUTE, "Direct Value")
            .thenFunction(WorkScheduler.COMPUTE, val -> val + "!")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Direct Value!", result);
    }

    @Test
    public void testFail_CannotBeRecoveredByRecoverBreak() {
        AtomicBoolean recoveryStageRun = new AtomicBoolean(false);
        RuntimeException expectedError = new RuntimeException("Fatal");
        
        Work.fail(expectedError)
            .recoverBreak(Done.INSTANCE)
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                recoveryStageRun.set(true);
                return val;
            })
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != expectedError) current = current.getCause();
                return current == expectedError;
            });
            
        assertFalse("Recovery should not run for Fail", recoveryStageRun.get());
    }
}
