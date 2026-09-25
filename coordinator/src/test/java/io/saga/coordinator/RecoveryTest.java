package io.saga.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Restart-recovery test: the first "process" crashes after persisting step 1;
 * a brand-new orchestrator instance over the same H2 file resumes the saga
 * and drives it to completion without re-executing the finished step.
 */
class RecoveryTest {

    private static final Duration AWAIT = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    @Test
    void interruptedSagaResumesFromPersistedStateAfterRestart() throws Exception {
        List<String> executed = new CopyOnWriteArrayList<>();
        AtomicInteger debitCalls = new AtomicInteger();
        CountDownLatch crashed = new CountDownLatch(1);

        // --- first "process": crashes right after step "debit" is persisted ---
        FaultInjector faults = new FaultInjector().crashAfter("debit", crashed::countDown);
        SagaOrchestrator first = new SagaOrchestrator(new SagaRepository(TestDbs.fileBased(dir)), faults);
        SagaDefinition saga = definition(executed, debitCalls);
        String sagaId = first.submit(saga);

        assertTrue(crashed.await(5, TimeUnit.SECONDS), "crash should have been triggered");
        waitFor(() -> first.saga(sagaId).status() == SagaStatus.RUNNING
                && first.steps(sagaId).get(0).status() == StepStatus.SUCCEEDED);
        first.close(); // simulate process death; saga stays RUNNING in the database

        // --- second "process": new orchestrator over the same database file ---
        SagaOrchestrator second = new SagaOrchestrator(new SagaRepository(TestDbs.fileBased(dir)));
        second.register(definition(executed, debitCalls));
        int resumed = second.recover();

        assertEquals(1, resumed);
        SagaInstance result = second.awaitTerminal(sagaId, AWAIT);
        assertEquals(SagaStatus.COMPLETED, result.status());
        assertEquals(List.of("debit", "reserve", "points"), executed);
        assertEquals(1, debitCalls.get(), "persisted step must not be re-executed after recovery");
        assertTrue(second.timeline(sagaId).stream().anyMatch(e -> e.type().equals("SAGA_RESUMED")));
        second.close();
    }

    private static SagaDefinition definition(List<String> executed, AtomicInteger debitCalls) {
        return Saga.define("order")
                .step("debit", ctx -> {
                    debitCalls.incrementAndGet();
                    executed.add("debit");
                }, ctx -> { })
                .step("reserve", ctx -> executed.add("reserve"), ctx -> { })
                .step("points", ctx -> executed.add("points"), ctx -> { })
                .build();
    }

    private static void waitFor(Check check) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("condition not met within 5s");
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}
