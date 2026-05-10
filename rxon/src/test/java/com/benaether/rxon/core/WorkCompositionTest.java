package com.benaether.rxon.core;

import io.reactivex.rxjava3.core.Single;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;

public class WorkCompositionTest {

    @Test
    public void testStaticThenComposition() {
        Work<Integer, Object> part1 = Work.io(() -> 10);
        Work<Integer, Object> part2 = part1.thenCompute(val -> val * 2);
        
        Integer result = part2.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(20), result);
    }

    @Test
    public void testThenChainDynamicComposition() {
        Work<Integer, Object> pipeline = Work.io(() -> "input")
            .thenChain(val -> Work.compute(() -> val.length()));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(5), result);
    }

    @Test
    public void testAsyncIoSingleIntegration() {
        Work<Integer, Object> pipeline = Work.io(() -> 10)
            .thenIoSingle(val -> Single.just(val + 5));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(15), result);
    }

    @Test
    public void testAsyncComputeSingleIntegration() {
        Work<Integer, Object> pipeline = Work.compute(() -> 100)
            .thenComputeSingle(val -> Single.just(val / 2));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(50), result);
    }

    @Test
    public void testThenWriteConsumerOverload() {
        AtomicInteger sideEffect = new AtomicInteger(0);
        Work<Integer, Object> pipeline = Work.io(() -> 42)
            .peekWrite(val -> sideEffect.set(val));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(42), result);
        assertEquals(42, sideEffect.get());
    }

    @Test
    public void testUniversalAsyncIntegration() {
        Work<Integer, Object> pipeline = Work.read(() -> 1)
            .thenReadSingle(val -> Single.just(val + 1))
            .thenWriteSingle(val -> Single.just(val + 1))
            .thenIoSingle(val -> Single.just(val + 1))
            .thenComputeSingle(val -> Single.just(val + 1))
            .thenMainSingle(val -> Single.just(val + 1));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(6), result);
    }

    @Test
    public void testUniversalConsumerOverloads() {
        AtomicInteger count = new AtomicInteger(0);
        Work<Integer, Object> pipeline = Work.io(() -> 1)
            .peekRead(val -> count.addAndGet(val))
            .peekWrite(val -> count.addAndGet(val))
            .peekCompute(val -> count.addAndGet(val))
            .peekMain(val -> count.addAndGet(val));
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(1), result); // Pipeline value should remain unchanged
        assertEquals(4, count.get());
    }

    @Test
    public void testContextPropagationInComposition() {
        Work<Integer, Object> pipeline = Work.withContext(() -> (Object) "initial")
            .thenIo((ctx, val) -> 10, (ctx, res) -> (Object) (ctx + "_updated"))
            .thenChain((ctx, val) -> {
                assertEquals("initial_updated", ctx);
                return Work.compute(() -> ((Integer) val) * 2);
            });
            
        Integer result = pipeline.asTerminalSingle().blockingGet();
        assertEquals(Integer.valueOf(20), result);
    }
}
