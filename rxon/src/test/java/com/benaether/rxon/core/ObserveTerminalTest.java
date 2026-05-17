package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Flowable;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ObserveTerminalTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testExecute() {
        AtomicInteger count = new AtomicInteger(0);
        Observe.flow(WorkScheduler.COMPUTE, Flowable.just(1, 2, 3))
            .thenConsumer(WorkScheduler.COMPUTE, v -> count.incrementAndGet())
            .execute();
            
        // Wait a bit for execution
        try { Thread.sleep(500); } catch (InterruptedException e) {}
        
        assertEquals(3, count.get());
    }

    @Test
    public void testExecuteOn() {
        AtomicReference<String> threadName = new AtomicReference<>();
        AtomicInteger count = new AtomicInteger(0);
        
        Observe.flow(WorkScheduler.IO, Flowable.just(1))
            .executeOn(WorkScheduler.COMPUTE, 
                v -> {
                    threadName.set(Thread.currentThread().getName());
                    count.incrementAndGet();
                },
                e -> {}
            );

        // Wait a bit for execution
        try { Thread.sleep(500); } catch (InterruptedException e) {}
        
        assertEquals(1, count.get());
        // Since we used COMPUTE, the thread name should likely contain RxCompute or similar 
        // (depending on SchedulerResolver implementation)
    }
}
