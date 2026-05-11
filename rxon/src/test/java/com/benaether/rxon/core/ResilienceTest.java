package com.benaether.rxon.core;

import static org.junit.Assert.assertEquals;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ResilienceTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testTimeout_TerminatesStage() {
        Work.callable(WorkScheduler.IO, () -> {
            Thread.sleep(500);
            return "Done";
        })
        .timeout(100, TimeUnit.MILLISECONDS)
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertError(java.util.concurrent.TimeoutException.class);
    }

    @Test
    public void testRetry_WithLinearBackoff() {
        AtomicInteger attempts = new AtomicInteger(0);
        
        Work.callable(WorkScheduler.IO, () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Fail");
        })
        .retry(2, 50, ResiliencePolicy.BackoffStrategy.LINEAR)
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertError(e -> e.getMessage().contains("Retries exhausted"));
        
        // 1 initial + 2 retries = 3 total attempts
        assertEquals(3, attempts.get());
    }

    @Test
    public void testRetry_SuccessAfterRetry() {
        AtomicInteger attempts = new AtomicInteger(0);
        
        Work.callable(WorkScheduler.IO, () -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException("Fail");
            }
            return "Success";
        })
        .retry(2, 50, ResiliencePolicy.BackoffStrategy.LINEAR)
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertValue("Success");
        
        assertEquals(2, attempts.get());
    }

    @Test
    public void testFallback_SwitchesOnFailure() {
        Work.callable(WorkScheduler.IO, () -> {
            throw new RuntimeException("Primary Fail");
        })
        .fallback(Work.breakWork("Fallback Success"))
        .asTerminalSingle()
        .test()
        .awaitDone(2, TimeUnit.SECONDS)
        .assertValue("Fallback Success");
    }

    @Test
    public void testCompensationRegistration_SuccessFlow() {
        AtomicInteger compCounter = new AtomicInteger(0);
        Work<Done, Object> compensation = Work.action(WorkScheduler.DATA_WRITE, (Runnable) compCounter::incrementAndGet);

        Work.callable(WorkScheduler.IO, () -> "Step 1")
            .compensate(compensation)
            .thenFunction(WorkScheduler.COMPUTE, (ctx, s) -> s + " -> Step 2")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Step 1 -> Step 2");

        // Compensation should NOT run on success
        assertEquals(0, compCounter.get());
    }
}
