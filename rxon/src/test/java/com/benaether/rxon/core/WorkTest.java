package com.benaether.rxon.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;

/**
 * Unit tests for {@link Work} showcasing pipeline usage and correctness.
 * These tests simulate various business logic scenarios using the semantic DSL.
 */
public class WorkTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    // ===========================================================================================
    // PIPELINE BASICS
    // ===========================================================================================

    @Test
    public void testSimplePipeline_EmitsTransformedValue() {
        Work.start(WorkScheduler.COMPUTE, () -> 10)
            .then(i -> Work.start(WorkScheduler.COMPUTE, () -> i * 2))
            .then(i -> Work.start(WorkScheduler.COMPUTE, () -> "Result: " + i))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Result: 20")
            .assertComplete();
    }

    @Test
    public void testPipelineFailure_PropagatesError() {
        Exception expectedError = new RuntimeException("Something went wrong");
        
        Work.start(WorkScheduler.COMPUTE, () -> 10)
            .then(i -> Work.fail(expectedError))
            .then(i -> Work.start(WorkScheduler.COMPUTE, () -> i + " ignored"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(expectedError)
            .assertNotComplete();
    }

    // ===========================================================================================
    // CONDITIONAL OPERATORS
    // ===========================================================================================

    @Test
    public void testThenIf_WhenConditionTrue_ExecutesSideEffectAndPreservesValue() {
        AtomicBoolean sideEffectRan = new AtomicBoolean(false);
        
        Work.start(WorkScheduler.COMPUTE, () -> "Input")
            .thenIf(s -> s.equals("Input"), s -> Work.start(WorkScheduler.COMPUTE, () -> {
                sideEffectRan.set(true);
                return Done.INSTANCE;
            }))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Input");
            
        assertTrue("Side effect should have executed", sideEffectRan.get());
    }

    @Test
    public void testThenIf_WhenConditionFalse_SkipsSideEffectAndPreservesValue() {
        AtomicBoolean sideEffectRan = new AtomicBoolean(false);
        
        Work.start(WorkScheduler.COMPUTE, () -> "Input")
            .thenIf(s -> s.equals("Other"), s -> Work.start(WorkScheduler.COMPUTE, () -> {
                sideEffectRan.set(true);
                return Done.INSTANCE;
            }))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Input");
            
        assertFalse("Side effect should NOT have executed", sideEffectRan.get());
    }

    @Test
    public void testThenOnlyIf_WhenConditionTrue_ContinuesWithMapping() {
        Work.start(WorkScheduler.COMPUTE, () -> 10)
            .thenOnlyIf(i -> i > 5, i -> Work.start(WorkScheduler.COMPUTE, () -> i * 10))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(100);
    }

    @Test
    public void testThenOnlyIf_WhenConditionFalse_FinishesWithCurrentValue() {
        AtomicBoolean downstreamRan = new AtomicBoolean(false);

        Work.start(WorkScheduler.COMPUTE, () -> 3)
            .thenOnlyIf(i -> i > 5, i -> Work.start(WorkScheduler.COMPUTE, () -> i * 10))
            .then(i -> Work.start(WorkScheduler.COMPUTE, () -> {
                downstreamRan.set(true);
                return i;
            }))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(3); // Chain finished early, 3 is the final value

        assertFalse("Downstream operations should have been skipped", downstreamRan.get());
    }

    // ===========================================================================================
    // ENTRY POINTS
    // ===========================================================================================

    @Test
    public void testStartFromRunnable_EmitsDone() {
        AtomicInteger counter = new AtomicInteger(0);
        
        Work.start(WorkScheduler.COMPUTE, (Runnable) counter::incrementAndGet)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(Done.INSTANCE);
            
        assertEquals(1, counter.get());
    }

    @Test
    public void testStartFromCompletable_EmitsDone() {
        Work.start(WorkScheduler.COMPUTE, Completable.complete())
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue(Done.INSTANCE);
    }

    @Test
    public void testStartFromSingle_EmitsValue() {
        Work.start(WorkScheduler.COMPUTE, Single.just("Success"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Success");
    }

    // ===========================================================================================
    // TERMINATION & FINISHING
    // ===========================================================================================

    @Test
    public void testFinish_ImmediatelyTerminatesSuccessfully() {
        Work.start(WorkScheduler.COMPUTE, () -> "Start")
            .then(s -> Work.finish("Early Return"))
            .then(s -> Work.start(WorkScheduler.COMPUTE, () -> s + " ignored"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValue("Early Return");
    }

    @Test
    public void testFail_ImmediatelyTerminatesWithError() {
        RuntimeException error = new RuntimeException("Fail Fast");
        
        Work.start(WorkScheduler.COMPUTE, () -> "Start")
            .then(s -> Work.fail(error))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(error);
    }

    // ===========================================================================================
    // EDGE CASES
    // ===========================================================================================

    @Test
    public void testFinishWithNull_ReturnsError() {
        Work.finish(null)
            .asTerminalSingle()
            .test()
            .assertError(NullPointerException.class);
    }

    @Test
    public void testStartFromCallableReturningNull_ReturnsError() {
        // RxJava Single.fromCallable does not allow null
        Work.start(WorkScheduler.COMPUTE, () -> null)
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(NullPointerException.class);
    }
}
