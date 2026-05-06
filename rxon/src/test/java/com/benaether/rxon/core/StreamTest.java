package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Unit tests for {@link Stream} showcasing pipeline usage and correctness.
 */
public class StreamTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void testSimpleStreamPipeline_EmitsTransformedValues() {
        Stream.start(WorkScheduler.COMPUTE, Flowable.just(1, 2, 3))
            .chainPublisher(i -> Flowable.just(i * 10))
            .asTerminalFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues(10, 20, 30)
            .assertComplete();
    }

    @Test
    public void testStreamToWorkPipeline_EmitsResults() {
        Stream.start(WorkScheduler.COMPUTE, Flowable.just("a", "b"))
            .chainPublisher(s -> Work.start(WorkScheduler.COMPUTE, () -> s.toUpperCase()).asSingle().toFlowable())
            .asTerminalFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues("A", "B")
            .assertComplete();
    }

    @Test
    public void testStreamToWorkPipeline_WithFinish_ContinuesStream() {
        // This test verifies that if a Work step finishes early, the Stream still recovers the value
        // and continues to the next emission.
        Stream.start(WorkScheduler.COMPUTE, Flowable.just(1, 2, 3))
            .chainPublisher(i -> {
                if (i == 2) return Work.finish(20).asSingle().toFlowable();
                return Work.start(WorkScheduler.COMPUTE, () -> i * 10).asSingle().toFlowable();
            })
            .asTerminalFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues(10, 20, 30)
            .assertComplete();
    }

    @Test
    public void testThenOnlyIf_WhenConditionFalse_FinishesEntireStream() {
        Stream.start(WorkScheduler.COMPUTE, Flowable.just(1, 2, 3))
            .chainOnlyIf(i -> i < 2, i -> Stream.start(WorkScheduler.COMPUTE, Flowable.just(i * 10)))
            .asTerminalFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertValues(10, 2) // 10 from mapping, 2 from early finish
            .assertComplete();
    }

    @Test
    public void testStreamFailure_PropagatesError() {
        Exception expectedError = new RuntimeException("Stream failure");
        
        Stream.start(WorkScheduler.COMPUTE, Flowable.error(expectedError))
            .asTerminalFlowable()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(expectedError);
    }
}
