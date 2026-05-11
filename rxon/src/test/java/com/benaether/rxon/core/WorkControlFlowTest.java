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
            .breakIf(i -> i > 5, -1)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> {
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
            .breakIf(i -> i < 5, -1)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> "Continued")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Continued", result);
    }

    @Test
    public void testFailIf_ConditionMet_ThrowsError() {
        RuntimeException error = new RuntimeException("Forced Fail");
        
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .failIf(i -> i > 5, error)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && e != error) e = e.getCause();
                return e == error;
            });
    }

    @Test
    public void testFailIf_ConditionNotMet_Continues() {
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .failIf(i -> i < 5, new RuntimeException("Fail"))
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> "Success")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Success");
    }
}
