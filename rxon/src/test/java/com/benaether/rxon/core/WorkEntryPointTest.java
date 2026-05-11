package com.benaether.rxon.core;

import com.benaether.rxon.schedulers.WorkScheduler;
import com.benaether.rxon.scopes.Done;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class WorkEntryPointTest {

    @Test
    public void testGenericEntryPoints() {
        Work.action(WorkScheduler.DATA_READ, () -> {})
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertResult(Done.INSTANCE);

        Work.callable(WorkScheduler.IO, () -> "hello")
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertResult("hello");

        Work.single(WorkScheduler.COMPUTE, Single.just("single"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertResult("single");

        Work.maybe(WorkScheduler.DATA_WRITE, Maybe.just("maybe"))
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertResult("maybe");

        Work.completable(WorkScheduler.MAIN, Completable.complete())
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertResult(Done.INSTANCE);
    }


    @Test
    public void testEmptyMaybeThrowsError() {
        Work.maybe(WorkScheduler.IO, Maybe.empty())
            .asTerminalSingle()
            .test()
            .awaitDone(2, TimeUnit.SECONDS)
            .assertError(java.util.NoSuchElementException.class);
    }
}
