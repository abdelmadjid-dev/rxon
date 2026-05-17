package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BranchingTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testWork_ThenBranch() {
        Work<String> onTrue = Work.callable(WorkScheduler.COMPUTE, () -> "TruePath");
        Work<String> onFalse = Work.callable(WorkScheduler.COMPUTE, () -> "FalsePath");

        String result1 = Work.<Boolean>callable(WorkScheduler.COMPUTE, () -> true)
            .thenBranch(val -> val, onTrue, onFalse)
            .asTerminalSingle()
            .blockingGet();

        String result2 = Work.<Boolean>callable(WorkScheduler.COMPUTE, () -> false)
            .thenBranch(val -> val, onTrue, onFalse)
            .asTerminalSingle()
            .blockingGet();

        assertEquals("TruePath", result1);
        assertEquals("FalsePath", result2);
    }

    @Test
    public void testWork_PeekBranch() {
        java.util.concurrent.atomic.AtomicBoolean checked = new java.util.concurrent.atomic.AtomicBoolean(false);
        
        Work<String> checkWork = Work.action(WorkScheduler.COMPUTE, () -> checked.set(true))
            .thenContinue(WorkScheduler.COMPUTE, "Ignored");

        String result = Work.<String>callable(WorkScheduler.COMPUTE, () -> "Original")
            .thenChain(val -> val.equals("Original") ? checkWork.thenContinue(WorkScheduler.COMPUTE, val) : Work.just(WorkScheduler.COMPUTE, val))
            .asTerminalSingle()
            .blockingGet();

        assertEquals("Original", result);
        assertEquals(true, checked.get());
    }
}
