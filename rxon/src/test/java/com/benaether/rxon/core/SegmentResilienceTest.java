package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SegmentResilienceTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testWork_SegmentRetry() {
        AtomicInteger attempts = new AtomicInteger(0);
        
        String result = Work.<String>callable(WorkScheduler.COMPUTE, () -> {
                attempts.incrementAndGet();
                if (attempts.get() < 3) throw new RuntimeException("Fail");
                return "Success";
            })
            .thenFunction(WorkScheduler.COMPUTE, val -> val + "!")
            .resilience(policy -> policy.retry(3))
            .asTerminalSingle()
            .blockingGet();

        assertEquals("Success!", result);
        assertEquals(3, attempts.get());
    }

    @Test
    public void testWork_SegmentTimeout() {
        Work.callable(WorkScheduler.IO, () -> {
                Thread.sleep(200);
                return "Late";
            })
            .resilience(policy -> policy.timeout(100, TimeUnit.MILLISECONDS))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && !(current instanceof java.util.concurrent.TimeoutException)) {
                    current = current.getCause();
                }
                return current instanceof java.util.concurrent.TimeoutException;
            });
    }
}
