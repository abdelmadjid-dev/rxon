package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;
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
        Work<Integer> other = Work.callable(WorkScheduler.COMPUTE, () -> 20);
        
        Integer result = Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .zipWith(other, (v1, v2) -> v1 + v2)
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals(Integer.valueOf(30), result);
    }

    @Test
    public void testZipWith_PropagatesErrorsFromOther() {
        RuntimeException otherError = new RuntimeException("Other Fail");
        Work<Done> other = Work.fail(otherError);
        
        Work.<Integer>callable(WorkScheduler.IO, () -> 10)
            .zipWith(other, (v1, v2) -> v1)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != otherError) current = current.getCause();
                return current == otherError;
            });
    }
}
