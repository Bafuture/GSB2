package io.saga.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** End-to-end tests driven by faults injected before/after a given step. */
class FaultInjectionTest {

    private static final Duration AWAIT = Duration.ofSeconds(10);

    @Test
    void faultBeforeSecondStepCompensatesOnlyCompletedSteps() {
        FaultInjector faults = new FaultInjector().failBefore("reserve");
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()), faults);
        List<String> executed = new CopyOnWriteArrayList<>();
        List<String> compensated = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("order")
                .step("debit", ctx -> executed.add("debit"), ctx -> compensated.add("debit"))
                .step("reserve", ctx -> executed.add("reserve"), ctx -> compensated.add("reserve"))
                .step("points", ctx -> executed.add("points"), ctx -> compensated.add("points"))
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(List.of("debit"), executed);
        assertEquals(List.of("debit"), compensated);
        assertTrue(result.failReason().contains("injected fault before step 'reserve'"));
        orchestrator.close();
    }

    @Test
    void faultAfterSecondStepCompensatesItToo() {
        FaultInjector faults = new FaultInjector().failAfter("reserve");
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaRepository(TestDbs.inMemory()), faults);
        List<String> executed = new CopyOnWriteArrayList<>();
        List<String> compensated = new CopyOnWriteArrayList<>();

        SagaDefinition saga = Saga.define("order")
                .step("debit", ctx -> executed.add("debit"), ctx -> compensated.add("debit"))
                .step("reserve", ctx -> executed.add("reserve"), ctx -> compensated.add("reserve"))
                .step("points", ctx -> executed.add("points"), ctx -> compensated.add("points"))
                .build();

        String id = orchestrator.submit(saga);
        SagaInstance result = orchestrator.awaitTerminal(id, AWAIT);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        assertEquals(List.of("debit", "reserve"), executed);
        assertEquals(List.of("reserve", "debit"), compensated);
        assertEquals(StepStatus.COMPENSATED, orchestrator.steps(id).get(1).status());
        assertEquals(StepStatus.PENDING, orchestrator.steps(id).get(2).status());
        orchestrator.close();
    }
}
