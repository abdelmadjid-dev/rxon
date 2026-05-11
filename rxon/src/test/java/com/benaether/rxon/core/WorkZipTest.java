package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class WorkZipTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testZipWith_CombinesResults() {
        Work<Integer, Object> other = Work.callable(WorkScheduler.COMPUTE, () -> 20);
        
        Integer result = Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .zipWith(other, (v1, v2) -> v1 + v2)
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals(Integer.valueOf(30), result);
    }

    @Test
    public void testZipWith_PropagatesErrorsFromOther() {
        RuntimeException otherError = new RuntimeException("Other Fail");
        Work<Integer, Object> other = Work.fail(otherError);
        
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .zipWith(other, (v1, v2) -> v1 + v2)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                while (e.getCause() != null && e != otherError) e = e.getCause();
                return e == otherError;
            });
    }

    @Test
    public void testZipWith_AccessesContext() {
        Work.withContext(() -> "Base")
            .thenCallable(WorkScheduler.IO, () -> 10)
            .zipWith(Work.callable(WorkScheduler.COMPUTE, () -> "Other"), (v1, v2) -> v1 + ":" + v2)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> ctx + "->" + val)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Base->10:Other");
    }
}
