package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class CompositionTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testWork_Composition() {
        Function<Work<String>, Work<String>> withSuffix = 
            work -> work.thenFunction(WorkScheduler.COMPUTE, val -> val + "_Suffix");

        String result = Work.callable(WorkScheduler.COMPUTE, () -> "Base")
            .compose(withSuffix)
            .asTerminalSingle()
            .blockingGet();

        assertEquals("Base_Suffix", result);
    }

    @Test
    public void testWork_LoggingComposition() {
        Function<Work<String>, Work<String>> withLog = 
            work -> work.log(LogLevel.INFO);

        String result = Work.callable(WorkScheduler.COMPUTE, () -> "Data")
            .compose(withLog)
            .asTerminalSingle()
            .blockingGet();

        assertEquals("Data", result);
    }
}
