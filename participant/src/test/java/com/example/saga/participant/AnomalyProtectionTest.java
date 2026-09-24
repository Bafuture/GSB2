package com.example.saga.participant;

import com.example.saga.coordinator.FaultInjector;
import com.example.saga.coordinator.RetryPolicy;
import com.example.saga.coordinator.Saga;
import com.example.saga.coordinator.SagaContext;
import com.example.saga.coordinator.SagaDefinition;
import com.example.saga.coordinator.SagaEngine;
import com.example.saga.coordinator.SagaRepository;
import com.example.saga.coordinator.SagaResult;
import com.example.saga.coordinator.SagaStatus;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end anomaly tests: the coordinator drives sagas with injected faults,
 * the participants protect themselves with barriers.
 */
class AnomalyProtectionTest {

    private final List<String> log = new CopyOnWriteArrayList<>();

    private Participant participant(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        Participant participant = new Participant(name, ds);
        participant.setLogger(log::add);
        return participant;
    }

    @Test
    void nullRollbackFaultInjectedBeforeStepExecution() {
        Participant account = participant("account");
        Participant inventory = participant("inventory");
        AtomicInteger releaseRuns = new AtomicInteger();

        SagaDefinition saga = Saga.define("null-rollback")
                .step("debit", account.forward(ctx -> { }), account.compensation(ctx -> { }))
                .step("reserve", inventory.forward(ctx -> { }),
                        inventory.compensation(ctx -> releaseRuns.incrementAndGet()))
                .build();

        SagaRepository repository = SagaRepository.inMemory("null-rollback");
        try (SagaEngine engine = new SagaEngine(repository)) {
            // fault: the reserve request never reaches the participant, but the
            // coordinator still asks the participant to compensate the step
            engine.setFaultInjector(FaultInjector.none().failBeforeStep(1));

            SagaResult result = engine.execute(saga);

            assertEquals(SagaStatus.COMPENSATED, result.status());
            assertEquals(0, releaseRuns.get(), "reserve never ran, so release must not run");
            assertTrue(log.stream().anyMatch(l -> l.contains("NULL ROLLBACK")), String.join("\n", log));
        }
    }

    @Test
    void suspensionDelayedForwardArrivingAfterCompensationIsRejected() throws Exception {
        Participant account = participant("account");
        Participant inventory = participant("inventory");
        AtomicInteger reserveRuns = new AtomicInteger();
        AtomicInteger releaseRuns = new AtomicInteger();

        // the reserve request is stuck "in the network" and reaches the participant late
        SagaDefinition saga = Saga.define("suspension")
                .step("debit", account.forward(ctx -> { }), account.compensation(ctx -> { }))
                .step("reserve", ctx -> Thread.sleep(10_000), // never reaches the participant in time
                        inventory.compensation(ctx -> releaseRuns.incrementAndGet()),
                        Duration.ofMillis(100), RetryPolicy.none())
                .build();

        SagaRepository repository = SagaRepository.inMemory("suspension");
        String sagaId;
        try (SagaEngine engine = new SagaEngine(repository)) {
            SagaResult result = engine.execute(saga);
            sagaId = result.sagaId();

            // the step timed out, the saga was compensated; the participant saw no forward
            assertEquals(SagaStatus.COMPENSATED, result.status());
            assertEquals(0, releaseRuns.get());
            assertTrue(log.stream().anyMatch(l -> l.contains("NULL ROLLBACK")), String.join("\n", log));
        }

        // the delayed forward request finally arrives at the participant
        SagaContext lateContext = new SagaContext(sagaId, "suspension");
        BarrierResult late = inventory.barrier().tryForward(sagaId, "reserve",
                ctx -> reserveRuns.incrementAndGet(), lateContext);

        assertEquals(BarrierResult.FORWARD_SUSPENDED, late);
        assertEquals(0, reserveRuns.get(), "suspended reserve must not execute after compensation");
    }

    @Test
    void compensationStaysIdempotentWhenCoordinatorRetriesAfterUnknownOutcome() {
        Participant account = participant("account");
        AtomicInteger creditRuns = new AtomicInteger();

        SagaDefinition saga = Saga.define("idempotent-comp")
                .step("debit", account.forward(ctx -> { }),
                        account.compensation(ctx -> creditRuns.incrementAndGet()))
                .step("trigger", ctx -> {
                    throw new IllegalStateException("fail to force compensation");
                }, ctx -> { }, Duration.ofSeconds(1), RetryPolicy.none())
                .build();

        SagaRepository repository = SagaRepository.inMemory("idempotent-comp");
        String sagaId;
        try (SagaEngine engine = new SagaEngine(repository)) {
            // outcome of debit is lost after it executed; the saga is compensated
            engine.setFaultInjector(FaultInjector.none().failAfterStep(0));
            SagaResult result = engine.execute(saga);
            sagaId = result.sagaId();
            assertEquals(SagaStatus.COMPENSATED, result.status());
        }
        assertEquals(1, creditRuns.get());

        // coordinator believes the compensation reply was lost and retries it
        SagaContext retryContext = new SagaContext(sagaId, "idempotent-comp");
        try {
            BarrierResult retry = account.barrier().tryCompensate(sagaId, "debit",
                    ctx -> creditRuns.incrementAndGet(), retryContext);
            assertEquals(BarrierResult.COMPENSATE_DUPLICATE, retry);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertEquals(1, creditRuns.get(), "compensation applied exactly once despite redelivery");
    }
}
