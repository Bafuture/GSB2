package io.saga.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SagaOrchestratorTest {

    private static final Duration AWAIT = Duration.ofSeconds(10);

    @Test
    void happyPathCompletesAllStepsInOrder() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        List<String> executed = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("happy")
                .step("a", ctx -> executed.add("a"), ctx -> executed.add("~a"))
                .step("b", ctx -> executed.add("b"), ctx -> executed.add("~b"))
                .step("c", ctx -> executed.add("c"), ctx -> executed.add("~c"))
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(List.of("a", "b", "c"), executed);
        assertTrue(orchestrator.steps(id).stream().allMatch(s -> s.status() == StepStatus.SUCCEEDED));
        orchestrator.close();
    }

    @Test
    void failedStepTriggersCompensationInReverseOrder() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        List<String> compensated = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("reverse")
                .retry(RetryPolicy.noRetry())
                .step("a", ctx -> { }, ctx -> compensated.add("a"))
                .step("b", ctx -> { }, ctx -> compensated.add("b"))
                .step("c", ctx -> { throw new IllegalStateException("boom"); }, ctx -> compensated.add("c"))
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(List.of("b", "a"), compensated);
        assertTrue(result.failReason().contains("boom"));
        assertEquals(StepStatus.COMPENSATED, orchestrator.steps(id).get(0).status());
        assertEquals(StepStatus.FAILED, orchestrator.steps(id).get(2).status());
        orchestrator.close();
    }

    @Test
    void parallelGroupRunsAllBranchesAndCompensatesSucceededOnes() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        List<String> executed = new CopyOnWriteArrayList<>();
        List<String> compensated = new CopyOnWriteArrayList<>();
        CountDownLatch bothStarted = new CountDownLatch(2);

        SagaDefinition saga = Saga.define("parallel")
                .retry(RetryPolicy.noRetry())
                .step("first", ctx -> { }, ctx -> compensated.add("first"))
                .parallel("grp", p -> p
                        .step("slow-ok", ctx -> {
                            bothStarted.countDown();
                            await(bothStarted);
                            executed.add("slow-ok");
                        }, ctx -> compensated.add("slow-ok"))
                        .step("fast-bad", ctx -> {
                            bothStarted.countDown();
                            throw new IllegalStateException("branch failed");
                        }, ctx -> compensated.add("fast-bad")))
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(List.of("slow-ok"), executed);
        assertTrue(compensated.containsAll(List.of("slow-ok", "first")));
        assertTrue(result.failReason().contains("branch failed"));
        orchestrator.close();
    }

    @Test
    void failedStepIsRetriedWithBackoffAndEventuallySucceeds() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        AtomicInteger attempts = new AtomicInteger();

        SagaDefinition saga = Saga.define("flaky")
                .retry(RetryPolicy.exponential(3, Duration.ofMillis(20), 2.0))
                .step("flaky", ctx -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("transient");
                    }
                }, ctx -> { })
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(3, attempts.get());
        StepRecord step = orchestrator.steps(id).get(0);
        assertEquals(StepStatus.SUCCEEDED, step.status());
        assertEquals(3, step.attempts());
        long retries = orchestrator.timeline(id).stream().filter(e -> e.type().equals("STEP_RETRY")).count();
        assertEquals(2, retries);
        orchestrator.close();
    }

    @Test
    void sagaIsMarkedFailedWithReasonWhenRetriesAreExhausted() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        AtomicInteger attempts = new AtomicInteger();
        List<String> compensated = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("doomed")
                .retry(RetryPolicy.exponential(2, Duration.ofMillis(10), 2.0))
                .step("ok", ctx -> { }, ctx -> compensated.add("ok"))
                .step("always-bad", ctx -> { attempts.incrementAndGet(); throw new IllegalStateException("down"); },
                        ctx -> { })
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(2, attempts.get());
        assertTrue(result.status() == SagaStatus.COMPENSATED || result.status() == SagaStatus.FAILED);
        assertTrue(result.failReason().contains("attempts exhausted"), result.failReason());
        assertTrue(result.failReason().contains("down"), result.failReason());
        assertEquals(List.of("ok"), compensated);
        assertTrue(orchestrator.timeline(id).stream().anyMatch(e -> e.type().equals("STEP_FAILED")));
        orchestrator.close();
    }

    @Test
    void stepThatExceedsItsTimeoutFailsAndIsCompensated() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()));
        List<String> compensated = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("slow")
                .retry(RetryPolicy.exponential(2, Duration.ofMillis(10), 1.0))
                .stepTimeout(Duration.ofMillis(100))
                .step("ok", ctx -> { }, ctx -> compensated.add("ok"))
                .step("slow", ctx -> Thread.sleep(5_000), ctx -> { })
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertTrue(result.failReason().contains("timed out"), result.failReason());
        assertEquals(List.of("ok"), compensated);
        orchestrator.close();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
