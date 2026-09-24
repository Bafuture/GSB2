package com.example.saga.coordinator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void crashedSagaResumesFromPersistedStateAfterRestart() {
        Path db = dir.resolve("recovery");
        List<String> calls = new CopyOnWriteArrayList<>();
        AtomicInteger step2Executions = new AtomicInteger();

        SagaDefinition definition = Saga.define("recoverable")
                .step("s1", ctx -> calls.add("f1"), ctx -> calls.add("c1"))
                .step("s2", ctx -> {
                    step2Executions.incrementAndGet();
                    calls.add("f2");
                }, ctx -> calls.add("c2"))
                .step("s3", ctx -> calls.add("f3"), ctx -> calls.add("c3"))
                .build();

        // first "process": crashes right after step s2 executed, before its state was persisted
        SagaEngine first = new SagaEngine(SagaRepository.at(db));
        first.setFaultInjector(FaultInjector.none().crashAfterStep(1));
        SagaCrashException crash = assertThrows(SagaCrashException.class,
                () -> first.execute(definition, "saga-1"));
        assertTrue(crash.getMessage().contains("after step 1"));
        first.close();

        // durable state: saga still RUNNING, s1 done, s2 not marked done
        SagaRepository repoCheck = SagaRepository.at(db);
        assertEquals(SagaStatus.RUNNING, repoCheck.findSaga("saga-1").orElseThrow().status());
        List<SagaRepository.StepRecord> steps = repoCheck.loadSteps("saga-1");
        assertEquals(StepStatus.SUCCEEDED, steps.get(0).status());
        assertEquals(StepStatus.RUNNING, steps.get(1).status());
        assertEquals(StepStatus.PENDING, steps.get(2).status());

        // second "process": recovers and drives the saga to completion
        try (SagaEngine second = new SagaEngine(repoCheck)) {
            second.register(definition);
            int recovered = second.recover();
            assertEquals(1, recovered);

            SagaResult result = second.result("saga-1");
            assertEquals(SagaStatus.COMPLETED, result.status());
            // s1 was not re-executed; s2 was (its outcome was lost in the crash); s3 ran once
            assertEquals(List.of("f1", "f2", "f2", "f3"), calls);
            assertEquals(2, step2Executions.get());
            assertTrue(result.timeline().stream().anyMatch(e -> e.type().equals("SAGA_RECOVERY")));
        }
    }

    @Test
    void sagaInterruptedWhileCompensatingResumesCompensation() {
        Path db = dir.resolve("recovery-comp");
        List<String> calls = new CopyOnWriteArrayList<>();
        AtomicInteger comp1Calls = new AtomicInteger();

        SagaDefinition definition = Saga.define("recover-comp")
                .step("s1", ctx -> calls.add("f1"), ctx -> {
                    comp1Calls.incrementAndGet();
                    if (comp1Calls.get() == 1) {
                        throw new SagaCrashException("crash during compensation");
                    }
                    calls.add("c1");
                }, Duration.ofSeconds(1), RetryPolicy.none())
                .step("s2", ctx -> {
                    throw new IllegalStateException("trigger compensation");
                }, ctx -> calls.add("c2"), Duration.ofSeconds(1), RetryPolicy.none())
                .build();

        SagaEngine first = new SagaEngine(SagaRepository.at(db));
        assertThrows(SagaCrashException.class, () -> first.execute(definition, "saga-2"));
        first.close();

        SagaRepository repoCheck = SagaRepository.at(db);
        assertEquals(SagaStatus.COMPENSATING, repoCheck.findSaga("saga-2").orElseThrow().status());

        try (SagaEngine second = new SagaEngine(repoCheck)) {
            second.register(definition);
            assertEquals(1, second.recover());

            SagaResult result = second.result("saga-2");
            assertEquals(SagaStatus.COMPENSATED, result.status());
            // c2 already compensated before the crash and is not repeated; c1 finishes on recovery
            assertEquals(List.of("f1", "c2", "c1"), calls);
        }
    }
}
