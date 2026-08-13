package com.benaether.rxon.core;
 
import static org.junit.Assert.assertEquals;

import com.benaether.rxon.schedulers.WorkScheduler;
import io.reactivex.rxjava3.core.Flowable;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ObserveParityTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testObserve_ChainingParity() {
        Observe.iterable(WorkScheduler.IO, Arrays.asList(1, 2, 3))
            .thenFunction(WorkScheduler.COMPUTE, val -> val * 10)
            .thenCallable(WorkScheduler.IO, () -> "Result")
            .asFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues("Result", "Result", "Result");
    }

    @Test
    public void testObserve_StreamingChaining() {
        Observe.iterable(WorkScheduler.IO, Arrays.asList(1, 2))
            .thenFlowable(WorkScheduler.COMPUTE, val -> Flowable.just(val, val + 1))
            .asFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues(1, 2, 2, 3);
    }

    @Test
    public void testObserve_ComplexPipeline() {
        AtomicInteger counter = new AtomicInteger(0);

        Observe.flow(WorkScheduler.IO, Flowable.just(10, 20))
            .thenConsumer(WorkScheduler.COMPUTE, counter::addAndGet)
            .thenSingle(WorkScheduler.IO, val -> io.reactivex.rxjava3.core.Single.just(val / 10))
            .thenFlowable(WorkScheduler.COMPUTE, val -> Flowable.range(0, val))
            .asFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues(0, 0, 1);

        assertEquals(30, counter.get());
    }
}
