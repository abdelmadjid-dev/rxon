package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates that RxOn correctly switches between schedulers at each stage of the pipeline.
 */
public class ThreadSwitchingTest {

    private List<String> threadLogs;

    @Before
    public void setup() {
        threadLogs = Collections.synchronizedList(new ArrayList<>());
        
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .scheduler(WorkScheduler.IO, createNamedScheduler("T-IO"))
            .scheduler(WorkScheduler.COMPUTE, createNamedScheduler("T-COMPUTE"))
            .scheduler(WorkScheduler.DATA_READ, createNamedScheduler("T-DATA-READ"))
            .scheduler(WorkScheduler.DATA_WRITE, createNamedScheduler("T-DATA-WRITE"))
            .scheduler(WorkScheduler.MAIN, createNamedScheduler("T-MAIN"))
            .init();
    }

    private Scheduler createNamedScheduler(String name) {
        return Schedulers.from(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }));
    }

    private void recordThread() {
        threadLogs.add(Thread.currentThread().getName());
    }

    @SuppressWarnings("CheckResult")
    @Test
    public void testWork_SequentialThreadSwitching() {
        Work.callable(WorkScheduler.IO, () -> {
            recordThread();
            return "Start";
        })
        .thenFunction(WorkScheduler.COMPUTE, val -> {
            recordThread();
            return val + " processed";
        })
        .thenSingle(WorkScheduler.DATA_READ, val -> Single.fromCallable(() -> {
            recordThread();
            return val + " read";
        }))
        .thenAction(WorkScheduler.DATA_WRITE, this::recordThread)
        .thenCallable(WorkScheduler.MAIN, () -> {
            recordThread();
            return "Final Result";
        })
        .asTerminalSingle()
        .blockingGet();

        assertEquals("Should have recorded exactly 5 thread entries", 5, threadLogs.size());
        assertTrue("Stage 0 should be on T-IO", threadLogs.get(0).contains("T-IO"));
        assertTrue("Stage 1 should be on T-COMPUTE", threadLogs.get(1).contains("T-COMPUTE"));
        assertTrue("Stage 2 should be on T-DATA-READ", threadLogs.get(2).contains("T-DATA-READ"));
        assertTrue("Stage 3 should be on T-DATA-WRITE", threadLogs.get(3).contains("T-DATA-WRITE"));
        assertTrue("Stage 4 should be on T-MAIN", threadLogs.get(4).contains("T-MAIN"));
    }

    @SuppressWarnings("CheckResult")
    @Test
    public void testObserve_SequentialThreadSwitching() {
        List<Integer> inputs = List.of(1);
        
        Observe.iterable(WorkScheduler.IO, inputs)
            .thenFunction(WorkScheduler.COMPUTE, i -> {
                recordThread();
                return i * 10;
            })
            .thenFlowable(WorkScheduler.DATA_READ, i -> Flowable.fromCallable(() -> {
                recordThread();
                return i + 1;
            }))
            .thenSingle(WorkScheduler.DATA_WRITE, i -> Single.fromCallable(() -> {
                recordThread();
                return i + 5;
            }))
            .thenAction(WorkScheduler.MAIN, this::recordThread)
            .asFlowable()
            .blockingLast();

        assertEquals("Should have recorded 4 thread entries", 4, threadLogs.size());
        
        assertTrue("Stage 1 should be on T-COMPUTE", threadLogs.get(0).contains("T-COMPUTE"));
        assertTrue("Stage 2 should be on T-DATA-READ", threadLogs.get(1).contains("T-DATA-READ"));
        assertTrue("Stage 3 should be on T-DATA-WRITE", threadLogs.get(2).contains("T-DATA-WRITE"));
        assertTrue("Stage 4 should be on T-MAIN", threadLogs.get(3).contains("T-MAIN"));
    }

    @SuppressWarnings("CheckResult")
    @Test
    public void testObserve_SwitchingWithMultipleEmissions() {
        List<Integer> inputs = List.of(1, 2, 3);
        
        Observe.iterable(WorkScheduler.IO, inputs)
            .thenFunction(WorkScheduler.COMPUTE, i -> {
                recordThread();
                return i;
            })
            .thenAction(WorkScheduler.DATA_WRITE, this::recordThread)
            .asFlowable()
            .blockingLast();

        assertEquals(6, threadLogs.size());
        
        long computeCount = threadLogs.stream().filter(s -> s.contains("T-COMPUTE")).count();
        long writeCount = threadLogs.stream().filter(s -> s.contains("T-DATA-WRITE")).count();
        
        assertEquals("Each item should have passed through COMPUTE", 3, computeCount);
        assertEquals("Each item should have passed through DATA-WRITE", 3, writeCount);
    }
}
