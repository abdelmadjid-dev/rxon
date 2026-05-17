package com.benaether.rxon.core;

import org.junit.Test;
import java.util.Arrays;
import static org.junit.Assert.*;

public class StackTraceCleanerTest {

    @Test
    public void testCleanNoise() {
        Throwable t = new RuntimeException("Error");
        StackTraceElement[] noise = new StackTraceElement[] {
                new StackTraceElement("io.reactivex.rxjava3.internal.operators.single.SingleFlatMap", "onSuccess", "SingleFlatMap.java", 77),
                new StackTraceElement("com.example.app.MyService", "doWork", "MyService.java", 10),
                new StackTraceElement("java.lang.Thread", "run", "Thread.java", 1012)
        };
        t.setStackTrace(noise);

        StackTraceCleaner.clean(t);

        StackTraceElement[] cleaned = t.getStackTrace();
        assertEquals("Should have exactly 1 frame left", 1, cleaned.length);
        assertEquals("com.example.app.MyService", cleaned[0].getClassName());
    }

    @Test
    public void testPreserveWorkEntryPoints() {
        Throwable t = new RuntimeException("Error");
        StackTraceElement[] frames = new StackTraceElement[] {
                new StackTraceElement("com.benaether.rxon.core.Work", "single", "Work.java", 94),
                new StackTraceElement("com.benaether.rxon.core.PipelineCompiler", "compile", "PipelineCompiler.java", 22)
        };
        t.setStackTrace(frames);

        StackTraceCleaner.clean(t);

        StackTraceElement[] cleaned = t.getStackTrace();
        
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement e : cleaned) sb.append(e.getClassName()).append("\n");
        String actual = sb.toString().trim();

        assertEquals("Should preserve only Work entry point. Actual: " + actual, 1, cleaned.length);
        assertEquals("com.benaether.rxon.core.Work", cleaned[0].getClassName());
    }

    @Test
    public void testRecursiveClean() {
        Throwable cause = new RuntimeException("Cause");
        cause.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.reactivex.rxjava3.core.Single", "subscribe", "Single.java", 100)
        });

        Throwable t = new RuntimeException("Top", cause);
        t.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.example.App", "main", "App.java", 1)
        });

        StackTraceCleaner.clean(t);

        assertEquals("Top exception should have its frame", 1, t.getStackTrace().length);
        assertEquals("Cause exception frames should be empty (all noise)", 0, t.getCause().getStackTrace().length);
    }
}
