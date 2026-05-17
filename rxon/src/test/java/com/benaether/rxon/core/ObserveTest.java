package com.benaether.rxon.core;

import static org.junit.Assert.assertEquals;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;

/**
 * Unit tests for {@link Observe} showcasing signal conditioning and work triggering.
 */
public class ObserveTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testObserve_TriggersWork() throws InterruptedException {
        AtomicInteger workCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);
        
        Work<Done> work = Work.callable(WorkScheduler.IO, () -> {
            workCounter.incrementAndGet();
            latch.countDown();
            return Done.INSTANCE;
        });

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, Flowable.just(1, 2, 3));
        Disposable d = observation.trigger(work);

        assertTrue("Timed out waiting for work triggers", latch.await(2, TimeUnit.SECONDS));
        assertEquals(3, workCounter.get());
        d.dispose();
    }

    @Test
    public void testObserve_DebouncesSignals() throws InterruptedException {
        AtomicInteger workCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        Work<Done> work = Work.callable(WorkScheduler.IO, () -> {
            workCounter.incrementAndGet();
            latch.countDown();
            return Done.INSTANCE;
        });

        // Emit 1, 2, 3 rapidly, then 4 after delay
        Flowable<Integer> source = Flowable.concat(
            Flowable.just(1, 2, 3),
            Flowable.just(4).delay(200, TimeUnit.MILLISECONDS)
        );

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, source)
            .debounce(WorkScheduler.COMPUTE, 100, TimeUnit.MILLISECONDS);
        
        Disposable d = observation.trigger(work);

        assertTrue("Timed out waiting for debounced triggers", latch.await(2, TimeUnit.SECONDS));

        // 1, 2, 3 are debounced to 3, then 4 is emitted
        assertEquals(2, workCounter.get());
        d.dispose();
    }

    @Test
    public void testObserve_ThrottlesSignals() throws InterruptedException {
        AtomicInteger workCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        Work<Done> work = Work.callable(WorkScheduler.IO, () -> {
            workCounter.incrementAndGet();
            latch.countDown();
            return Done.INSTANCE;
        });

        // Emit 1, 2, 3 rapidly, then 4 after delay
        Flowable<Integer> source = Flowable.concat(
            Flowable.just(1, 2, 3),
            Flowable.just(4).delay(200, TimeUnit.MILLISECONDS)
        );

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, source)
            .throttle(WorkScheduler.COMPUTE, 100, TimeUnit.MILLISECONDS);
        
        Disposable d = observation.trigger(work);

        assertTrue("Timed out waiting for throttled triggers", latch.await(2, TimeUnit.SECONDS));

        // 1 is taken, 2 and 3 are ignored, then 4 is taken
        assertEquals(2, workCounter.get());
        d.dispose();
    }

    @Test
    public void testObserve_DistinctSignals() throws InterruptedException {
        AtomicInteger workCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        Work<Done> work = Work.callable(WorkScheduler.IO, () -> {
            workCounter.incrementAndGet();
            latch.countDown();
            return Done.INSTANCE;
        });

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, Flowable.just(1, 1, 2, 2, 2))
            .distinct(WorkScheduler.COMPUTE);
        
        Disposable d = observation.trigger(work);

        assertTrue("Timed out waiting for distinct triggers", latch.await(2, TimeUnit.SECONDS));

        assertEquals(2, workCounter.get());
        d.dispose();
    }

    @Test
    public void testObserve_TriggerWithResults() throws InterruptedException {
        List<String> results = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        Work<String> work = Work.callable(WorkScheduler.IO, () -> "Data");

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, Flowable.just(1, 2, 3));
        Disposable d = observation.trigger(work, val -> {
            results.add(val);
            latch.countDown();
        }, err -> {});

        assertTrue("Timed out waiting for work results", latch.await(2, TimeUnit.SECONDS));
        assertEquals(3, results.size());
        assertEquals("Data", results.get(0));
        d.dispose();
    }

    @Test
    public void testObserve_PropagatesSourceErrors() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Throwable[] capturedError = new Throwable[1];

        Work<Done> work = Work.callable(WorkScheduler.IO, () -> Done.INSTANCE);

        Flowable<Integer> errorSource = Flowable.error(new RuntimeException("Source Fail"));

        Observe<Integer> observation = Observe.flow(WorkScheduler.IO, errorSource);
        Disposable d = observation.trigger(work, val -> {}, err -> {
            capturedError[0] = err;
            latch.countDown();
        });

        assertTrue("Timed out waiting for error propagation", latch.await(2, TimeUnit.SECONDS));
        assertEquals("Source Fail", capturedError[0].getMessage());
        d.dispose();
    }

    private void assertTrue(String message, boolean condition) {
        if (!condition) {
            org.junit.Assert.fail(message);
        }
    }
}
