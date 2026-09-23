package io.saga.participant;

import io.saga.core.EventType;
import io.saga.core.RetryPolicy;
import io.saga.core.Saga;
import io.saga.core.SagaContext;
import io.saga.core.SagaDefinition;
import io.saga.core.SagaStatus;
import io.saga.core.StepStatus;
import io.saga.core.engine.SagaOrchestrator;
import io.saga.core.engine.SagaResult;
import io.saga.core.fault.ConfigurableFaultInjector;
import io.saga.core.store.JdbcSagaStore;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end fault-injection scenarios across the three demo participants
 * (account / inventory / points), driven by the coordinator's fault injector.
 */
class FaultInjectionScenariosTest {

    private static final class Trio {
        final AccountParticipant account = new AccountParticipant(10_000);
        final InventoryParticipant inventory = new InventoryParticipant(10);
        final PointsParticipant points = new PointsParticipant(500);
        final JdbcSagaStore store = JdbcSagaStore.inMemory("scenario-" + UUID.randomUUID());
        final SagaDefinition saga = Saga.define("order")
                .step("debit", account.debit(2_000), account.refund(2_000))
                .step("stock", inventory.reserve(3), inventory.release(3))
                .step("points", points.add(100), points.revoke(100))
                .build();
        final SagaContext context = new SagaContext(UUID.randomUUID().toString());

        SagaOrchestrator orchestrator(ConfigurableFaultInjector faults) {
            SagaOrchestrator.Builder b = SagaOrchestrator.builder(store)
                    .defaultRetryPolicy(RetryPolicy.none());
            if (faults != null) {
                b.faultInjector(faults);
            }
            return b.build();
        }
    }

    @Test
    void emptyRollbackScenario() {
        Trio t = new Trio();
        // the forward delivery of 'stock' is lost; 'points' then fails before executing
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .dropForwardOfStep(t.saga.indexOf("stock"))
                .failBeforeStep(t.saga.indexOf("points"));

        SagaResult result = t.orchestrator(faults).execute(t.saga, t.context);

        assertEquals(SagaStatus.COMPENSATED, result.status());
        // the compensation for 'stock' was delivered although its forward never ran:
        // the participant treats it as a successful no-op
        assertEquals(10, t.inventory.getAvailableUnits());
        assertTrue(t.inventory.getOperationLog().stream().anyMatch(l -> l.contains("EMPTY_ROLLBACK_IGNORED")));
        // the debit that did happen is properly rolled back
        assertEquals(10_000, t.account.getBalanceCents());
        assertEquals(500, t.points.getPoints());
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.STEP_FORWARD_DROPPED));
    }

    @Test
    void danglingScenario() {
        Trio t = new Trio();
        String sagaId = t.context.getSagaId();
        t.store.createSaga(sagaId, t.saga.getName());
        t.saga.getFlatSteps().forEach(s -> t.store.insertStep(sagaId, s.getIndex(), s.getName(), s.getGroupId()));
        // anomaly: compensation of 'debit' was processed BEFORE its forward action
        t.store.forceStepStatus(sagaId, t.saga.indexOf("debit"), StepStatus.COMPENSATED);
        // ...and that early compensation was indeed delivered to the participant
        SagaContext.setCurrentStep("debit");
        try {
            t.account.refund(2_000).execute(new SagaContext(sagaId));
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            SagaContext.clearCurrentStep();
        }

        SagaResult result = t.orchestrator(null).resume(sagaId, t.saga, t.context);

        assertEquals(SagaStatus.COMPLETED, result.status());
        // engine-level guard: the belated forward action never reached the participant
        assertEquals(10_000, t.account.getBalanceCents());
        assertTrue(result.timeline().stream().anyMatch(e -> e.type() == EventType.FORWARD_SKIPPED_DANGLING));
        // participant-level guard: a direct belated delivery is rejected as well
        SagaContext.setCurrentStep("debit");
        try {
            t.account.debit(2_000).execute(new SagaContext(sagaId));
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            SagaContext.clearCurrentStep();
        }
        assertEquals(10_000, t.account.getBalanceCents());
        assertTrue(t.account.getOperationLog().stream().anyMatch(l -> l.contains("DANGLING_FORWARD_REJECTED")));
    }

    @Test
    void idempotentCompensationScenario() {
        Trio t = new Trio();
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .failBeforeStep(t.saga.indexOf("points"));
        SagaOrchestrator orchestrator = t.orchestrator(faults);

        SagaResult first = orchestrator.execute(t.saga, t.context);
        assertEquals(SagaStatus.COMPENSATED, first.status());
        assertEquals(10_000, t.account.getBalanceCents());
        assertEquals(10, t.inventory.getAvailableUnits());

        // duplicate rollback requests (e.g. retried by an upstream caller)
        orchestrator.rollback(t.context.getSagaId());
        SagaResult again = orchestrator.rollback(t.context.getSagaId());

        assertEquals(SagaStatus.COMPENSATED, again.status());
        assertEquals(10_000, t.account.getBalanceCents()); // refunds applied exactly once
        assertEquals(10, t.inventory.getAvailableUnits());
        assertTrue(t.account.getOperationLog().stream().noneMatch(l -> l.contains("EMPTY_ROLLBACK")));
    }
}
