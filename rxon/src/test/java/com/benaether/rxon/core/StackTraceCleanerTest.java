package com.benaether.rxon.core;

import org.junit.Test;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.*;

public class StackTraceCleanerTest {

    @Test
    public void testChainCollapsing() {
        TimeoutException realCause = new TimeoutException("Timed out");
        realCause.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.example.ExternalLib", "wait", "ExternalLib.java", 1)
        });

        Throwable assemblyWrapper2 = new DummyAssemblyException("assembled", realCause);
        assemblyWrapper2.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.smartprints.Manager", "init", "Manager.java", 61)
        });

        Throwable assemblyWrapper1 = new DummyAssemblyException("assembled", assemblyWrapper2);
        assemblyWrapper1.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.smartprints.Factory", "create", "Factory.java", 45)
        });

        RuntimeException top = new RuntimeException("Top Error", assemblyWrapper1);
        top.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.smartprints.App", "main", "App.java", 10)
        });

        Throwable cleaned = StackTraceCleaner.clean(top);
        assertNotNull(cleaned);
        assertEquals("Top cause should be the real cause", realCause, cleaned.getCause());
    }

    @Test
    public void testCleanAfterMapping() {
        RuntimeException real = new RuntimeException("real");
        real.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.example.Internal", "run", "I.java", 1)
        });

        Throwable dirty = new DummyAssemblyException("assembled", real);
        dirty.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("hu.akarnokd.rxjava3.debug.Assembly", "foo", "A.java", 1)
        });

        Throwable mapped = new RuntimeException("Mapped Error", dirty);
        mapped.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.example.App", "onMapper", "App.java", 1)
        });

        Throwable cleanedMapped = StackTraceCleaner.clean(mapped);
        assertNotNull(cleanedMapped);
        assertEquals("Mapped exception cause should be real cause", real, cleanedMapped.getCause());
    }

    @Test
    public void testPureAssemblyChain() {
        Throwable asm2 = new DummyAssemblyException("assembled", null);
        asm2.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.reactivex.rxjava3.core.Single", "subscribe", "Single.java", 100),
                new StackTraceElement("com.example.AppService", "doService", "AppService.java", 42)
        });

        Throwable asm1 = new DummyAssemblyException("assembled", asm2);
        asm1.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.reactivex.rxjava3.core.Single", "defer", "Single.java", 90)
        });

        RuntimeException top = new RuntimeException("Edfapay card scan timed out.", asm1);
        top.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("com.example.PaymentManager", "onTimeout", "PaymentManager.java", 15)
        });

        Throwable cleanedTop = StackTraceCleaner.clean(top);
        assertNotNull(cleanedTop);
        assertNull("Cause should be unlinked for pure assembly chain", cleanedTop.getCause());
    }

    static class DummyAssemblyException extends RuntimeException {
        public DummyAssemblyException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
