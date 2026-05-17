package com.benaether.rxon.core;

import com.benaether.rxon.rx.RxOnLogger;
import com.benaether.rxon.schedulers.WorkScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WorkLoggingTest {

    private List<String> logEvents;

    @Before
    public void setup() {
        logEvents = new ArrayList<>();
        RxOnLogger customLogger = new RxOnLogger() {
            @Override public void e(String tag, String msg, Throwable t) { logEvents.add("ERROR:" + tag); }
            @Override public void w(String tag, String msg, Throwable t) { logEvents.add("WARN:" + tag); }
            @Override public void i(String tag, String msg) { logEvents.add("INFO:" + tag); }
            @Override public void d(String tag, String msg) { logEvents.add("DEBUG:" + tag); }
            @Override public void onStageStart(String tag, int i, String d) { logEvents.add("START:" + tag + ":" + i); }
            @Override public void onStageEnd(String tag, int i, String d, long t) { logEvents.add("END:" + tag + ":" + i); }
            @Override public void onPipelineFinish(String tag, long t) { logEvents.add("FINISH:" + tag); }
            @Override public void onPipelineError(String tag, Throwable e, long t) { logEvents.add("PIPELINE_ERROR:" + tag); }
        };

        RxOnConfig.builder()
            .debug(true)
            .logger(customLogger)
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testWork_TagAndLifecycleLogging() {
        Work.<String>callable(WorkScheduler.IO, () -> "Hello")
            .tag("MyWork")
            .thenFunction(WorkScheduler.COMPUTE, val -> val + " World")
            .asTerminalSingle()
            .blockingGet();

        assertTrue(logEvents.contains("START:MyWork:0"));
        assertTrue(logEvents.contains("END:MyWork:0"));
        assertTrue(logEvents.contains("START:MyWork:1"));
        assertTrue(logEvents.contains("END:MyWork:1"));
        assertTrue(logEvents.contains("FINISH:MyWork"));
    }

    @Test
    public void testWork_DeclarativeLogging() {
        Work.callable(WorkScheduler.IO, () -> "Hello")
            .log(LogLevel.INFO, "Pre-check")
            .asTerminalSingle()
            .blockingGet();

        assertTrue(logEvents.contains("INFO:RxOn:Step"));
    }

    @Test
    public void testWork_ErrorLogging() {
        try {
            Work.fail(new RuntimeException("Fail"))
                .tag("ErrorWork")
                .asTerminalSingle()
                .blockingGet();
        } catch (Exception ignored) {}

        assertTrue(logEvents.contains("PIPELINE_ERROR:ErrorWork"));
    }
}
