package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class WorkPeekTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testPeek_RunsSideEffect_PreservesValue() {
        AtomicInteger sideEffect = new AtomicInteger(0);
        
        Integer result = Work.callable(WorkScheduler.IO, () -> 42)
            .doOnNext(WorkScheduler.COMPUTE, sideEffect::set)
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals(Integer.valueOf(42), result);
        assertEquals(42, sideEffect.get());
    }
}
