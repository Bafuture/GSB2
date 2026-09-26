package com.example.saga.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaEngineTest {

    @Test
    void happyPathExecutesAllStepsInOrder() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        ConcurrentLinkedQueue<String> executed = new ConcurrentLinkedQueue<>();

        SagaDefinition saga = SagaBuilder.named("happy")
                .step("s1", ctx -> executed.add("s1"), ctx -> { })
                .step("s2", ctx -> executed.add("s2"), ctx -> { })
                .step("s3", ctx -> executed.add("s3"), ctx -> { })
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertIterableEquals(List.of("s1", "s2", "s3"), executed);
        assertTrue(result.timeline().contains("SAGA_COMPLETED"));
    }

    @Test
    void parallelGroupRunsAllBranches() throws Exception {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger ran = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("parallel")
                .step("s1", ctx -> { }, ctx -> { })
                .parallel("grp", g -> g
                        .step("p1", ctx -> {
                            entered.countDown();
                            ran.incrementAndGet();
                            release.await(2, TimeUnit.SECONDS);
                        }, ctx -> { })
                        .step("p2", ctx -> {
                            entered.countDown();
                            ran.incrementAndGet();
                            release.await(2, TimeUnit.SECONDS);
                        }, ctx -> { }))
                .build();

        java.util.concurrent.atomic.AtomicReference<SagaResult> ref = new java.util.concurrent.atomic.AtomicReference<>();
        Thread runner = new Thread(() -> ref.set(engine.execute(saga)));
        runner.start();
        // Both branches must be in flight at the same time.
        assertTrue(entered.await(5, TimeUnit.SECONDS), "parallel branches should overlap");
        release.countDown();
        runner.join(10_000);

        assertEquals(2, ran.get());
        assertEquals(SagaStatus.COMPLETED, ref.get().status());
    }

    @Test
    void failureCompensatesSucceededStepsInReverseOrder() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        ConcurrentLinkedQueue<String> compensated = new ConcurrentLinkedQueue<>();

        SagaDefinition saga = SagaBuilder.named("reverse")
                .step("s1", ctx -> { }, ctx -> compensated.add("c1"))
                .step("s2", ctx -> { }, ctx -> compensated.add("c2"))
                .step(SagaStep.of("s3", ctx -> {
                    throw new IllegalStateException("boom");
                }, ctx -> compensated.add("c3")).withRetry(CoreTestSupport.fastRetry(2)))
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertIterableEquals(List.of("c2", "c1"), compensated,
                "compensation must run in reverse order and skip the failed step");
        assertTrue(result.failReason().contains("s3"));
    }

    @Test
    void transientFailureIsRetriedWithBackoffUntilSuccess() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        AtomicInteger attempts = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("retry")
                .step(SagaStep.of("flaky", ctx -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("temporary");
                    }
                }, ctx -> { }).withRetry(CoreTestSupport.fastRetry(5)))
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(3, attempts.get());
        assertTrue(result.timeline().contains("STEP_RETRYING"));
    }

    @Test
    void exhaustedRetriesRecordFailReasonAndCompensate() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        AtomicInteger compensated = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("exhaust")
                .step("s1", ctx -> { }, ctx -> compensated.incrementAndGet())
                .step(SagaStep.of("alwaysFails", ctx -> {
                    throw new IllegalStateException("permanent");
                }, ctx -> { }).withRetry(CoreTestSupport.fastRetry(2)))
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(result.failReason().contains("alwaysFails"));
        assertTrue(result.failReason().contains("2 attempt"));
        assertEquals(1, compensated.get());
    }

    @Test
    void slowStepTimesOutAndIsRetriedThenCompensated() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource());
        AtomicInteger slowAttempts = new AtomicInteger();
        AtomicInteger compensated = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("timeout")
                .step("s1", ctx -> { }, ctx -> compensated.incrementAndGet())
                .step(SagaStep.of("slow", ctx -> {
                    slowAttempts.incrementAndGet();
                    Thread.sleep(10_000);
                }, ctx -> { })
                        .withTimeout(Duration.ofMillis(100))
                        .withRetry(CoreTestSupport.fastRetry(2)))
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(2, slowAttempts.get(), "each attempt should hit the timeout");
        assertTrue(result.failReason().contains("slow"));
        assertEquals(1, compensated.get());
        assertTrue(result.timeline().contains("timed out"));
    }

    @Test
    void faultInjectedBeforeStepPreventsExecutionAndCompensatesPriorSteps() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource(),
                FaultInjection.failBeforeStep(2));
        AtomicInteger step2Runs = new AtomicInteger();
        AtomicInteger compensated = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("fault-before")
                .step("s1", ctx -> { }, ctx -> compensated.incrementAndGet())
                .step(SagaStep.of("s2", ctx -> step2Runs.incrementAndGet(), ctx -> { })
                        .withRetry(CoreTestSupport.fastRetry(2)))
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(0, step2Runs.get(), "fault before step 2 means its action never ran");
        assertEquals(1, compensated.get());
    }

    @Test
    void faultInjectedAfterStepStillCompensatesTheExecutedAction() {
        SagaEngine engine = new SagaEngine(CoreTestSupport.inMemoryDataSource(),
                FaultInjection.failAfterStep(1));
        AtomicInteger step1Runs = new AtomicInteger();
        AtomicInteger step1Compensated = new AtomicInteger();

        SagaDefinition saga = SagaBuilder.named("fault-after")
                .step(SagaStep.of("s1", ctx -> step1Runs.incrementAndGet(),
                        ctx -> step1Compensated.incrementAndGet())
                        .withRetry(CoreTestSupport.fastRetry(2)))
                .step("s2", ctx -> { }, ctx -> { })
                .build();

        SagaResult result = engine.execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(step1Runs.get() >= 1, "the action ran before the injected fault");
        assertEquals(1, step1Compensated.get(),
                "a step whose action ran but whose outcome was lost must still be compensated (exactly once)");
    }
}
