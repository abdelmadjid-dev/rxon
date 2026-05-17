package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;
import io.reactivex.rxjava3.core.Completable;
import org.junit.Before;
import org.junit.Test;

public class BugReproductionTest {

    @Before
    public void setup() {
        RxOnConfig.builder()
            .debug(true)
            .logger(new com.benaether.rxon.rx.DefaultRxOnLogger())
            .errorMapper(t -> t)
            .init();
    }

    @Test
    public void reproduceClassCastException() {
        Work.just(WorkScheduler.COMPUTE, "Initial Value")
            .thenChain(val -> Work.completable(WorkScheduler.COMPUTE, Completable.complete()))
            .asTerminalSingle()
            .test()
            .awaitDone(2, java.util.concurrent.TimeUnit.SECONDS)
            .assertValue(Done.INSTANCE)
            .assertNoErrors();
    }
}
