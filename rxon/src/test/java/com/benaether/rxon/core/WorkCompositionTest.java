package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Single;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;

public class WorkCompositionTest {

    @Test
    public void testStaticThenComposition() {
        Work<Integer> part1 = Work.callable(WorkScheduler.IO, () -> 10);
        Work<Integer> part2 = part1.thenFunction(WorkScheduler.COMPUTE, val -> val * 2);
        
        Integer result = part2.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(20), result);
    }

    @Test
    public void testThenChainDynamicComposition() {
        Work<Integer> pipeline = Work.<String>callable(WorkScheduler.IO, () -> "input")
            .thenChain(val -> Work.callable(WorkScheduler.COMPUTE, val::length));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(5), result);
    }

    @Test
    public void testAsyncIoSingleIntegration() {
        Work<Integer> pipeline = Work.callable(WorkScheduler.IO, () -> 10)
            .thenSingle(WorkScheduler.IO, val -> Single.just(val + 5));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(15), result);
    }

    @Test
    public void testAsyncComputeSingleIntegration() {
        Work<Integer> pipeline = Work.callable(WorkScheduler.COMPUTE, () -> 100)
            .thenSingle(WorkScheduler.COMPUTE, val -> Single.just(val / 2));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(50), result);
    }

    @Test
    public void testThenWriteConsumerOverload() {
        AtomicInteger sideEffect = new AtomicInteger(0);
        Work<Integer> pipeline = Work.callable(WorkScheduler.IO, () -> 42)
            .thenConsumer(WorkScheduler.DATA_WRITE, sideEffect::set);
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(42), result);
        assertEquals(42, sideEffect.get());
    }

    @Test
    public void testUniversalAsyncIntegration() {
        Work<Integer> pipeline = Work.callable(WorkScheduler.DATA_READ, () -> 1)
            .thenSingle(WorkScheduler.DATA_READ, val -> Single.just(val + 1))
            .thenSingle(WorkScheduler.DATA_WRITE, val -> Single.just(val + 1))
            .thenSingle(WorkScheduler.IO, val -> Single.just(val + 1))
            .thenSingle(WorkScheduler.COMPUTE, val -> Single.just(val + 1))
            .thenSingle(WorkScheduler.MAIN, val -> Single.just(val + 1));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(6), result);
    }

    @Test
    public void testUniversalConsumerOverloads() {
        AtomicInteger count = new AtomicInteger(0);
        Work<Integer> pipeline = Work.callable(WorkScheduler.IO, () -> 1)
            .thenConsumer(WorkScheduler.DATA_READ, count::addAndGet)
            .thenConsumer(WorkScheduler.DATA_WRITE, count::addAndGet)
            .thenConsumer(WorkScheduler.COMPUTE, count::addAndGet)
            .thenConsumer(WorkScheduler.MAIN, count::addAndGet);
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(1), result);
        assertEquals(4, count.get());
    }
}
