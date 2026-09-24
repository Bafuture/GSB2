package com.example.saga.coordinator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaEngineTest {

    private final List<String> calls = new CopyOnWriteArrayList<>();

    private SagaAction record(String label) {
        return ctx -> calls.add(label);
    }

    @Test
    void happyPathRunsAllStepsInOrderAndCompletes() {
        SagaRepository repository = SagaRepository.inMemory("happy");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("happy")
                    .step("s1", record("f1"), record("c1"))
                    .step("s2", record("f2"), record("c2"))
                    .step("s3", record("f3"), record("c3"))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPLETED, result.status());
            assertEquals(List.of("f1", "f2", "f3"), calls);
            assertTrue(result.timeline().stream().anyMatch(e -> e.type().equals("SAGA_COMPLETED")));
        }
    }

    @Test
    void parallelGroupRunsAllStepsConcurrently() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        SagaAction blocking = ctx -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
        };
        SagaRepository repository = SagaRepository.inMemory("parallel");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("parallel")
                    .step("s1", record("f1"), record("c1"))
                    .parallel(p -> p
                            .step("s2", blocking, record("c2"))
                            .step("s3", blocking, record("c3")))
                    .build();

            AtomicInteger result = new AtomicInteger(-1);
            Thread runner = new Thread(() -> result.set(engine.execute(saga).status().ordinal()));
            runner.start();
            // both parallel steps must be inside their actions at the same time
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            release.countDown();
            runner.join(5000);
            assertEquals(SagaStatus.COMPLETED.ordinal(), result.get());
        }
    }

    @Test
    void forwardFailureCompensatesExecutedStepsInReverseOrder() {
        SagaRepository repository = SagaRepository.inMemory("compensate");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("compensate")
                    .step("s1", record("f1"), record("c1"))
                    .step("s2", record("f2"), record("c2"))
                    .step("s3", ctx -> {
                        calls.add("f3");
                        throw new IllegalStateException("boom");
                    }, record("c3"), Duration.ofSeconds(1), RetryPolicy.none())
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPENSATED, result.status());
            // failed step is compensated first (its outcome is unknown in general), then reverse order
            assertEquals(List.of("f1", "f2", "f3", "c3", "c2", "c1"), calls);
            assertTrue(result.failureReason().contains("boom"));
        }
    }

    @Test
    void retriesWithBackoffUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();
        SagaRepository repository = SagaRepository.inMemory("retry");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("retry")
                    .step("flaky", ctx -> {
                        if (attempts.incrementAndGet() < 3) {
                            throw new IllegalStateException("attempt " + attempts.get());
                        }
                    }, record("c1"), Duration.ofSeconds(1),
                            RetryPolicy.of(3, Duration.ofMillis(10), 2.0))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPLETED, result.status());
            assertEquals(3, attempts.get());
            long retries = result.timeline().stream().filter(e -> e.type().equals("STEP_RETRY")).count();
            assertEquals(2, retries);
        }
    }

    @Test
    void exhaustedRetriesFailTheStepAndRecordReason() {
        AtomicInteger attempts = new AtomicInteger();
        SagaRepository repository = SagaRepository.inMemory("exhausted");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("exhausted")
                    .step("alwaysFails", ctx -> {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("nope");
                    }, record("c1"), Duration.ofSeconds(1),
                            RetryPolicy.of(2, Duration.ofMillis(5), 1.0))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(2, attempts.get());
            assertEquals(SagaStatus.COMPENSATED, result.status());
            assertTrue(result.failureReason().contains("failed after 2 attempt(s)"), result.failureReason());
            // failure reason is persisted
            assertTrue(repository.findSaga(result.sagaId()).orElseThrow().failureReason().contains("nope"));
        }
    }

    @Test
    void stepTimingOutIsRetriedThenCompensated() {
        AtomicInteger attempts = new AtomicInteger();
        SagaRepository repository = SagaRepository.inMemory("timeout");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("timeout")
                    .step("slow", ctx -> {
                        attempts.incrementAndGet();
                        Thread.sleep(10_000);
                    }, record("c1"), Duration.ofMillis(100),
                            RetryPolicy.of(2, Duration.ofMillis(5), 1.0))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(2, attempts.get());
            assertEquals(SagaStatus.COMPENSATED, result.status());
            assertTrue(result.failureReason().contains("timed out"), result.failureReason());
        }
    }

    @Test
    void injectedFaultBeforeStepSkipsForwardAction() {
        SagaRepository repository = SagaRepository.inMemory("fault-before");
        try (SagaEngine engine = new SagaEngine(repository)) {
            engine.setFaultInjector(FaultInjector.none().failBeforeStep(1));
            SagaDefinition saga = Saga.define("fault-before")
                    .step("s1", record("f1"), record("c1"))
                    .step("s2", record("f2"), record("c2"))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPENSATED, result.status());
            // f2 never ran, but its compensation is still requested (null-rollback safe participants no-op it)
            assertEquals(List.of("f1", "c2", "c1"), calls);
        }
    }

    @Test
    void injectedFaultAfterStepLeavesOutcomeUnknownAndCompensates() {
        SagaRepository repository = SagaRepository.inMemory("fault-after");
        try (SagaEngine engine = new SagaEngine(repository)) {
            engine.setFaultInjector(FaultInjector.none().failAfterStep(1));
            SagaDefinition saga = Saga.define("fault-after")
                    .step("s1", record("f1"), record("c1"))
                    .step("s2", record("f2"), record("c2"))
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPENSATED, result.status());
            // f2 DID run; the coordinator could not see the result, so it compensates both steps
            assertEquals(List.of("f1", "f2", "c2", "c1"), calls);
        }
    }

    @Test
    void compensationExhaustingRetriesMarksSagaFailed() {
        SagaRepository repository = SagaRepository.inMemory("comp-fails");
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaDefinition saga = Saga.define("comp-fails")
                    .step("s1", record("f1"), ctx -> {
                        throw new IllegalStateException("compensation broken");
                    }, Duration.ofSeconds(1), RetryPolicy.of(2, Duration.ofMillis(5), 1.0))
                    .step("s2", ctx -> {
                        throw new IllegalStateException("trigger");
                    }, record("c2"), Duration.ofSeconds(1), RetryPolicy.none())
                    .build();

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.FAILED, result.status());
            assertTrue(result.failureReason().contains("compensation of step 's1'"), result.failureReason());
            assertTrue(result.timeline().stream().anyMatch(e -> e.type().equals("SAGA_FAILED")));
        }
    }
}
