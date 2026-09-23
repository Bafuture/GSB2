package io.saga.core;

import io.saga.core.engine.SagaOrchestrator;
import io.saga.core.engine.SagaResult;
import io.saga.core.fault.ConfigurableFaultInjector;
import io.saga.core.fault.SimulatedCrashException;
import io.saga.core.store.JdbcSagaStore;
import io.saga.core.store.SagaStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaOrchestratorTest {

    private static JdbcSagaStore newStore() {
        return JdbcSagaStore.inMemory("test-" + UUID.randomUUID());
    }

    private static SagaOrchestrator orchestrator(SagaStore store) {
        return SagaOrchestrator.builder(store)
                .defaultRetryPolicy(RetryPolicy.of(3, Duration.ofMillis(5), 2.0))
                .defaultStepTimeout(Duration.ofSeconds(2))
                .build();
    }

    @Test
    void happyPathCompletesAndPersistsEveryStep() {
        JdbcSagaStore store = newStore();
        List<String> executed = new CopyOnWriteArrayList<>();
        SagaDefinition saga = Saga.define("happy")
                .step("a", ctx -> executed.add("a"), ctx -> { })
                .step("b", ctx -> executed.add("b"), ctx -> { })
                .build();

        SagaResult result = orchestrator(store).execute(saga);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(List.of("a", "b"), executed);
        List<SagaStore.StepRecord> steps = store.loadSteps(result.sagaId());
        assertEquals(2, steps.size());
        assertTrue(steps.stream().allMatch(s -> s.status() == StepStatus.SUCCEEDED));
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.SAGA_STARTED));
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.SAGA_COMPLETED));
    }

    @Test
    void parallelGroupRunsAllStepsAndPersistsGroupId() throws Exception {
        JdbcSagaStore store = newStore();
        List<String> executed = new CopyOnWriteArrayList<>();
        SagaDefinition saga = Saga.define("with-group")
                .step("first", ctx -> executed.add("first"), ctx -> { })
                .parallel("grp", p -> p
                        .step("g1", ctx -> executed.add("g1"), ctx -> { })
                        .step("g2", ctx -> executed.add("g2"), ctx -> { }))
                .step("last", ctx -> executed.add("last"), ctx -> { })
                .build();

        SagaResult result = orchestrator(store).execute(saga);

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(4, executed.size());
        assertEquals("first", executed.get(0));
        assertEquals("last", executed.get(3));
        assertTrue(executed.containsAll(List.of("g1", "g2")));
        List<SagaStore.StepRecord> steps = store.loadSteps(result.sagaId());
        assertEquals("grp", steps.get(1).groupId());
        assertEquals("grp", steps.get(2).groupId());
        assertEquals(null, steps.get(0).groupId());
    }

    @Test
    void forwardFailureCompensatesSucceededStepsInReverseOrder() {
        JdbcSagaStore store = newStore();
        List<String> compensated = new CopyOnWriteArrayList<>();
        SagaDefinition saga = Saga.define("rollback-order")
                .step("s0", ctx -> { }, ctx -> compensated.add("c0"))
                .step("s1", ctx -> { }, ctx -> compensated.add("c1"))
                .step("s2", ctx -> { throw new IllegalStateException("boom"); },
                        ctx -> compensated.add("c2"))
                .build();

        SagaResult result = orchestrator(store).execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(List.of("c1", "c0"), compensated); // strictly reverse, s2 never ran
        assertTrue(result.failureReason().contains("s2"));
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.SAGA_COMPENSATED));
    }

    @Test
    void stepTimeoutRetriesWithBackoffThenRollsBack() {
        JdbcSagaStore store = newStore();
        AtomicInteger slowCalls = new AtomicInteger();
        AtomicInteger compensated = new AtomicInteger();
        SagaDefinition saga = Saga.define("timeout-saga")
                .step("fast", ctx -> { }, ctx -> compensated.incrementAndGet())
                .step(StepDefinition.of("slow", ctx -> {
                            slowCalls.incrementAndGet();
                            Thread.sleep(5_000); // way beyond the step timeout
                        }, ctx -> { })
                        .timeout(Duration.ofMillis(50))
                        .retry(RetryPolicy.of(3, Duration.ofMillis(10), 2.0))
                        .build())
                .build();

        long start = System.nanoTime();
        SagaResult result = orchestrator(store).execute(saga);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(3, slowCalls.get()); // bounded retries
        assertEquals(1, compensated.get()); // earlier step rolled back
        assertTrue(result.failureReason().contains("slow"));
        assertTrue(result.failureReason().contains("TimeoutException"));
        // backoff: 10ms + 20ms between the three attempts
        assertTrue(elapsedMillis >= 3 * 50 + 10 + 20, "elapsed=" + elapsedMillis);
        assertTrue(elapsedMillis < 5_000, "timed-out action must be abandoned, elapsed=" + elapsedMillis);
        assertEquals(3, store.loadSteps(result.sagaId()).get(1).attempts());
        assertEquals(3, result.timeline().stream().filter(e -> e.type() == EventType.STEP_TIMEOUT).count());
    }

    @Test
    void exhaustedCompensationRetriesMarkSagaFailedWithReason() {
        JdbcSagaStore store = newStore();
        SagaDefinition saga = Saga.define("compensation-fails")
                .step(StepDefinition.of("s0", ctx -> { }, ctx -> {
                            throw new IllegalStateException("compensation broken");
                        })
                        .retry(RetryPolicy.of(2, Duration.ofMillis(1), 1.0))
                        .build())
                .step(StepDefinition.of("s1", ctx -> {
                            throw new IllegalStateException("forward broken");
                        }, ctx -> { })
                        .retry(RetryPolicy.none())
                        .build())
                .build();

        SagaResult result = orchestrator(store).execute(saga);

        assertEquals(SagaStatus.FAILED, result.status());
        assertTrue(result.failureReason().contains("compensation incomplete"));
        assertEquals(StepStatus.COMPENSATION_FAILED, store.loadSteps(result.sagaId()).get(0).status());
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.SAGA_FAILED));
    }

    @Test
    void emptyRollbackProtectionSkipsCompensationForStepsNeverExecuted() {
        JdbcSagaStore store = newStore();
        AtomicInteger comp0 = new AtomicInteger();
        AtomicInteger comp1 = new AtomicInteger();
        AtomicInteger comp2 = new AtomicInteger();
        SagaDefinition saga = Saga.define("empty-rollback")
                .step("s0", ctx -> { }, ctx -> comp0.incrementAndGet())
                .step("s1", ctx -> { }, ctx -> comp1.incrementAndGet())
                .step("s2", ctx -> { }, ctx -> comp2.incrementAndGet())
                .build();
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector().failBeforeStep(1);

        SagaResult result = SagaOrchestrator.builder(store)
                .faultInjector(faults)
                .defaultRetryPolicy(RetryPolicy.none())
                .build()
                .execute(saga);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(1, comp0.get()); // only the step that actually ran is compensated
        assertEquals(0, comp1.get()); // empty rollback: forward never executed -> no compensation
        assertEquals(0, comp2.get());
        assertEquals(2, result.timeline().stream()
                .filter(e -> e.type() == EventType.COMPENSATION_SKIPPED_EMPTY_ROLLBACK).count());
    }

    @Test
    void danglingProtectionSkipsForwardArrivingAfterCompensation() {
        JdbcSagaStore store = newStore();
        AtomicInteger forward0 = new AtomicInteger();
        AtomicInteger forward1 = new AtomicInteger();
        SagaDefinition saga = Saga.define("dangling")
                .step("s0", ctx -> forward0.incrementAndGet(), ctx -> { })
                .step("s1", ctx -> forward1.incrementAndGet(), ctx -> { })
                .build();
        String sagaId = UUID.randomUUID().toString();
        store.createSaga(sagaId, saga.getName());
        saga.getFlatSteps().forEach(s -> store.insertStep(sagaId, s.getIndex(), s.getName(), s.getGroupId()));
        // anomaly: compensation of s0 was processed BEFORE its forward action arrives
        store.forceStepStatus(sagaId, 0, StepStatus.COMPENSATED);

        SagaResult result = orchestrator(store).resume(sagaId, saga, new SagaContext(sagaId));

        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(0, forward0.get()); // dangling forward rejected
        assertEquals(1, forward1.get());
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.FORWARD_SKIPPED_DANGLING));
    }

    @Test
    void compensationIsIdempotentAcrossRepeatedRollbackRequests() {
        JdbcSagaStore store = newStore();
        AtomicInteger comp0 = new AtomicInteger();
        SagaDefinition saga = Saga.define("idem")
                .step("s0", ctx -> { }, ctx -> comp0.incrementAndGet())
                .step(StepDefinition.of("s1", ctx -> {
                            throw new IllegalStateException("boom");
                        }, ctx -> { })
                        .retry(RetryPolicy.none())
                        .build())
                .build();
        SagaOrchestrator orchestrator = orchestrator(store);

        SagaResult first = orchestrator.execute(saga);
        assertEquals(SagaStatus.COMPENSATED, first.status());
        assertEquals(1, comp0.get());

        SagaResult second = orchestrator.rollback(first.sagaId()); // duplicate delivery
        assertEquals(SagaStatus.COMPENSATED, second.status());
        assertEquals(1, comp0.get()); // effect applied exactly once
        assertTrue(second.timeline().stream()
                .anyMatch(e -> e.type() == EventType.COMPENSATION_SKIPPED_DUPLICATE));
    }

    @Test
    void restartRecoversAndCompletesInterruptedSaga(@TempDir Path dir) {
        JdbcSagaStore store = JdbcSagaStore.file(dir, "recovery");
        AtomicInteger s0 = new AtomicInteger();
        AtomicInteger s1 = new AtomicInteger();
        AtomicInteger s2 = new AtomicInteger();
        SagaDefinition saga = Saga.define("recoverable")
                .step("s0", ctx -> s0.incrementAndGet(), ctx -> { })
                .step("s1", ctx -> s1.incrementAndGet(), ctx -> { })
                .step("s2", ctx -> s2.incrementAndGet(), ctx -> { })
                .build();
        SagaContext context = new SagaContext(UUID.randomUUID().toString());
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector().crashBeforeStep(2);

        SagaOrchestrator first = SagaOrchestrator.builder(store).faultInjector(faults).build();
        assertThrows(SimulatedCrashException.class, () -> first.execute(saga, context));
        first.close();
        assertEquals(1, s0.get());
        assertEquals(1, s1.get());
        assertEquals(0, s2.get());
        // saga is still RUNNING in the database
        assertEquals(SagaStatus.RUNNING, store.loadSaga(context.getSagaId()).orElseThrow().status());

        // simulated process restart: new orchestrator over the same H2 file
        SagaOrchestrator restarted = orchestrator(store);
        restarted.register(saga);
        assertEquals(1, restarted.recover());

        SagaResult result = restarted.result(context.getSagaId());
        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(1, s0.get()); // completed steps are not re-executed
        assertEquals(1, s1.get());
        assertEquals(1, s2.get()); // the interrupted step resumes
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.SAGA_RECOVERED));
    }

    @Test
    void restartResumesInterruptedCompensation(@TempDir Path dir) {
        JdbcSagaStore store = JdbcSagaStore.file(dir, "recovery-comp");
        AtomicInteger comp0 = new AtomicInteger();
        AtomicInteger comp1 = new AtomicInteger();
        SagaDefinition saga = Saga.define("recoverable-compensation")
                .step("s0", ctx -> { }, ctx -> comp0.incrementAndGet())
                .step("s1", ctx -> { }, ctx -> comp1.incrementAndGet())
                .step(StepDefinition.of("s2", ctx -> {
                            throw new IllegalStateException("boom");
                        }, ctx -> { })
                        .retry(RetryPolicy.none())
                        .build())
                .build();
        // pre-seed a saga that crashed halfway through compensation
        String sagaId = UUID.randomUUID().toString();
        store.createSaga(sagaId, saga.getName());
        saga.getFlatSteps().forEach(s -> store.insertStep(sagaId, s.getIndex(), s.getName(), s.getGroupId()));
        store.forceStepStatus(sagaId, 0, StepStatus.SUCCEEDED);
        store.forceStepStatus(sagaId, 1, StepStatus.SUCCEEDED);
        store.forceStepStatus(sagaId, 2, StepStatus.FAILED);
        store.forceStepStatus(sagaId, 1, StepStatus.COMPENSATED); // s1 compensated before the crash
        store.updateSagaStatus(sagaId, SagaStatus.COMPENSATING, "step 's2' failed");

        SagaOrchestrator restarted = orchestrator(store);
        restarted.register(saga);
        assertEquals(1, restarted.recover());

        SagaResult result = restarted.result(sagaId);
        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(1, comp0.get()); // only the remaining step is compensated
        assertEquals(0, comp1.get());
    }
}
