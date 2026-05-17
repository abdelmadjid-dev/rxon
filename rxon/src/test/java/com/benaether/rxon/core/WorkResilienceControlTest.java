package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class WorkResilienceControlTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testRecover_UniversalCatch_ResumesPipeline() {
        String result = Work.<String>callable(WorkScheduler.IO, () -> {
                throw new RuntimeException("Crash");
            })
            .recover(err -> "Recovered")
            .thenFunction(WorkScheduler.COMPUTE, val -> val + " and Continued")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("Recovered and Continued", result);
    }

    @Test
    public void testRecover_TypedCatch_MatchesException() {
        String result = Work.<String>callable(WorkScheduler.IO, () -> {
                throw new IOException("IO Fail");
            })
            .recover(IOException.class, err -> "IO Recovered")
            .asTerminalSingle()
            .blockingGet();
            
        assertEquals("IO Recovered", result);
    }

    @Test
    public void testRecover_TypedCatch_DoesNotMatchException() {
        RuntimeException fatal = new RuntimeException("Fatal");
        
        Work.<String>callable(WorkScheduler.IO, () -> {
                throw fatal;
            })
            .recover(IOException.class, err -> "Should not run")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(e -> {
                Throwable current = e;
                while (current.getCause() != null && current != fatal) current = current.getCause();
                return current == fatal;
            });
    }
}
