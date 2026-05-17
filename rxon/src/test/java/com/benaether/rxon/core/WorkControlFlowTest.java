package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WorkControlFlowTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testBreakIf_ConditionMet_TerminatesEarly() {
        AtomicBoolean subsequentRun = new AtomicBoolean(false);
        
        Integer result = Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .thenChain(i -> i > 5 ? Work.just(WorkScheduler.COMPUTE, -1).thenBreak(-1) : Work.just(WorkScheduler.COMPUTE, i))
            .thenFunction(WorkScheduler.COMPUTE, val -> {
                subsequentRun.set(true);
                return 100;
            })
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals(Integer.valueOf(-1), result);
        assertFalse("Subsequent stage should be bypassed", subsequentRun.get());
    }

    @Test
    public void testBreakIf_ConditionNotMet_Continues() {
        String result = Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .thenChain(i -> i < 5 ? Work.just(WorkScheduler.COMPUTE, -1).thenBreak(-1) : Work.just(WorkScheduler.COMPUTE, i))
            .thenFunction(WorkScheduler.COMPUTE, val -> "Continued")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Continued", result);
    }

    @Test
    public void testFailIf_ConditionMet_ThrowsError() {
        RuntimeException error = new RuntimeException("Forced Fail");
        
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .reject(WorkScheduler.COMPUTE, i -> i > 5, i -> error)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != error) current = current.getCause();
                return current == error;
            });
    }

    @Test
    public void testFailIf_ConditionNotMet_Continues() {
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .reject(WorkScheduler.COMPUTE, i -> i < 5, i -> new RuntimeException("Fail"))
            .thenFunction(WorkScheduler.COMPUTE, val -> "Success")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Success");
    }
}
