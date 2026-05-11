package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Single;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;

public class WorkCompositionTest {

    @Test
    public void testStaticThenComposition() {
        Work<Integer, Object> part1 = Work.callable(WorkScheduler.IO, () -> 10);
        Work<Integer, Object> part2 = part1.thenFunction(WorkScheduler.COMPUTE, (ctx, val) -> val * 2);
        
        Integer result = part2.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(20), result);
    }

    @Test
    public void testThenChainDynamicComposition() {
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.IO, () -> "input")
            .thenChain(val -> Work.callable(WorkScheduler.COMPUTE, () -> val.length()));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(5), result);
    }

    @Test
    public void testAsyncIoSingleIntegration() {
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.IO, () -> 10)
            .thenSingle(WorkScheduler.IO, (ctx, val) -> Single.just(val + 5));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(15), result);
    }

    @Test
    public void testAsyncComputeSingleIntegration() {
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.COMPUTE, () -> 100)
            .thenSingle(WorkScheduler.COMPUTE, (ctx, val) -> Single.just(val / 2));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(50), result);
    }

    @Test
    public void testThenWriteConsumerOverload() {
        AtomicInteger sideEffect = new AtomicInteger(0);
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.IO, () -> 42)
            .thenConsumer(WorkScheduler.DATA_WRITE, (ctx, val) -> sideEffect.set(val));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(42), result);
        assertEquals(42, sideEffect.get());
    }

    @Test
    public void testUniversalAsyncIntegration() {
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.DATA_READ, () -> 1)
            .thenSingle(WorkScheduler.DATA_READ, (ctx, val) -> Single.just(val + 1))
            .thenSingle(WorkScheduler.DATA_WRITE, (ctx, val) -> Single.just(val + 1))
            .thenSingle(WorkScheduler.IO, (ctx, val) -> Single.just(val + 1))
            .thenSingle(WorkScheduler.COMPUTE, (ctx, val) -> Single.just(val + 1))
            .thenSingle(WorkScheduler.MAIN, (ctx, val) -> Single.just(val + 1));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(6), result);
    }

    @Test
    public void testUniversalConsumerOverloads() {
        AtomicInteger count = new AtomicInteger(0);
        Work<Integer, Object> pipeline = Work.callable(WorkScheduler.IO, () -> 1)
            .thenConsumer(WorkScheduler.DATA_READ, (ctx, val) -> count.addAndGet(val))
            .thenConsumer(WorkScheduler.DATA_WRITE, (ctx, val) -> count.addAndGet(val))
            .thenConsumer(WorkScheduler.COMPUTE, (ctx, val) -> count.addAndGet(val))
            .thenConsumer(WorkScheduler.MAIN, (ctx, val) -> count.addAndGet(val));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(1), result); // Pipeline value should remain unchanged
        assertEquals(4, count.get());
    }

    @Test
    public void testContextPropagationInComposition() {
        Work<Integer, Object> pipeline = Work.withContext(() -> (Object) "initial")
            .thenFunction(WorkScheduler.IO, (ctx, val) -> 10, (ctx, res) -> (Object) (ctx + "_updated"))
            .thenChain((ctx, val) -> {
                assertEquals("initial_updated", ctx);
                return Work.callable(WorkScheduler.COMPUTE, () -> ((Integer) val) * 2);
            });
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(20), result);
    }
}
