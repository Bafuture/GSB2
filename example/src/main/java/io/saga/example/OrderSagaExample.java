package io.saga.example;

import io.saga.core.EventType;
import io.saga.core.Saga;
import io.saga.core.SagaContext;
import io.saga.core.SagaDefinition;
import io.saga.core.SagaEvent;
import io.saga.core.StepStatus;
import io.saga.core.engine.SagaOrchestrator;
import io.saga.core.engine.SagaResult;
import io.saga.core.fault.ConfigurableFaultInjector;
import io.saga.core.fault.SimulatedCrashException;
import io.saga.core.store.JdbcSagaStore;
import io.saga.participant.AccountParticipant;
import io.saga.participant.InventoryParticipant;
import io.saga.participant.PointsParticipant;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end demo of an order saga spanning three participants:
 * account (debit), inventory (reserve) and points (add). Each scenario runs
 * against a fresh embedded H2 store and prints the final saga status plus the
 * persisted event timeline.
 */
public final class OrderSagaExample {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private OrderSagaExample() { }

    public static void main(String[] args) throws Exception {
        happyPath();
        failBeforeStep();
        failAfterStep();
        emptyRollback();
        dangling();
        crashAndRecover();
    }

    /** 1. No faults: every forward action runs, saga completes. */
    static void happyPath() {
        Fixture f = new Fixture();
        SagaResult result = f.newOrchestrator(null).execute(f.saga, f.context);
        print("happy path (no injection)", f, result);
    }

    /** 2. Injected failure BEFORE step 2 (add-points): steps 0-1 are compensated in reverse. */
    static void failBeforeStep() {
        Fixture f = new Fixture();
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .failBeforeStep(f.saga.indexOf("add-points"));
        SagaResult result = f.newOrchestrator(faults).execute(f.saga, f.context);
        print("fault injected BEFORE step 'add-points'", f, result);
    }

    /** 3. Injected failure AFTER step 1 (reserve-stock): its effect is uncertain, so it is compensated too. */
    static void failAfterStep() {
        Fixture f = new Fixture();
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .failAfterStep(f.saga.indexOf("reserve-stock"));
        SagaResult result = f.newOrchestrator(faults).execute(f.saga, f.context);
        print("fault injected AFTER step 'reserve-stock'", f, result);
    }

    /** 4. Empty rollback: the forward delivery of 'reserve-stock' is lost, then a later step fails. */
    static void emptyRollback() {
        Fixture f = new Fixture();
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .dropForwardOfStep(f.saga.indexOf("reserve-stock"))
                .failBeforeStep(f.saga.indexOf("add-points"));
        SagaResult result = f.newOrchestrator(faults).execute(f.saga, f.context);
        print("empty rollback (forward of 'reserve-stock' never delivered, compensation still requested)", f, result);
    }

    /** 5. Dangling: compensation of 'debit-account' was recorded BEFORE its forward action arrives. */
    static void dangling() {
        Fixture f = new Fixture();
        String sagaId = f.context.getSagaId();
        f.store.createSaga(sagaId, f.saga.getName());
        f.saga.getFlatSteps().forEach(s -> f.store.insertStep(sagaId, s.getIndex(), s.getName(), s.getGroupId()));
        int debit = f.saga.indexOf("debit-account");
        f.store.forceStepStatus(sagaId, debit, StepStatus.COMPENSATED);
        f.store.appendEvent(sagaId, EventType.COMPENSATION_SUCCEEDED,
                "debit-account (compensation arrived BEFORE the forward action)");
        SagaResult result = f.newOrchestrator(null).resume(sagaId, f.saga, f.context);
        print("dangling (compensation of 'debit-account' arrived before its forward action)", f, result);
    }

    /** 6. Crash before step 2, then a "restarted" orchestrator recovers from the same H2 file. */
    static void crashAndRecover() throws Exception {
        Path dir = Files.createTempDirectory("saga-demo-recovery");
        Fixture f = new Fixture(JdbcSagaStore.file(dir, "order-saga"));
        ConfigurableFaultInjector faults = new ConfigurableFaultInjector()
                .crashBeforeStep(f.saga.indexOf("add-points"));
        try {
            f.newOrchestrator(faults).execute(f.saga, f.context);
            throw new IllegalStateException("expected a simulated crash");
        } catch (SimulatedCrashException expected) {
            System.out.println("  >> process 'crashed' before step 'add-points'; saga left RUNNING in H2");
        }
        // process restart: brand-new orchestrator over the same database file
        try (SagaOrchestrator restarted = SagaOrchestrator.builder(f.store).build()) {
            restarted.register(f.saga);
            int resumed = restarted.recover();
            System.out.println("  >> restarted orchestrator recovered " + resumed + " saga(s)");
            SagaResult result = restarted.result(f.context.getSagaId());
            print("crash & recover (restart resumes the unfinished saga)", f, result);
        }
    }

    private static void print(String scenario, Fixture f, SagaResult result) {
        System.out.println();
        System.out.println("==================================================================");
        System.out.println("Scenario: " + scenario);
        System.out.println("==================================================================");
        System.out.println("Final status : " + result.status()
                + (result.failureReason() != null ? "  (" + result.failureReason() + ")" : ""));
        System.out.println("Participants : balance=" + f.account.getBalanceCents()
                + " cents, stock=" + f.inventory.getAvailableUnits()
                + " units, points=" + f.points.getPoints());
        System.out.println("Event timeline (persisted in H2):");
        List<SagaEvent> events = result.timeline().stream()
                .sorted(Comparator.comparingLong(SagaEvent::id))
                .toList();
        for (SagaEvent e : events) {
            System.out.printf("  %s  %-32s %s%n", TS.format(e.timestamp()), e.type(),
                    e.detail() == null ? "" : e.detail());
        }
        if (!f.account.getOperationLog().isEmpty()
                || !f.inventory.getOperationLog().isEmpty()
                || !f.points.getOperationLog().isEmpty()) {
            System.out.println("Participant operation logs:");
            f.account.getOperationLog().forEach(l -> System.out.println("  " + l));
            f.inventory.getOperationLog().forEach(l -> System.out.println("  " + l));
            f.points.getOperationLog().forEach(l -> System.out.println("  " + l));
        }
    }

    /** Fresh participants, store and saga definition per scenario. */
    private static final class Fixture {
        final AccountParticipant account = new AccountParticipant(100_00);
        final InventoryParticipant inventory = new InventoryParticipant(10);
        final PointsParticipant points = new PointsParticipant(500);
        final JdbcSagaStore store;
        final SagaDefinition saga;
        final SagaContext context = new SagaContext(UUID.randomUUID().toString());

        Fixture() {
            this(JdbcSagaStore.inMemory("demo-" + DB_SEQ.incrementAndGet()));
        }

        Fixture(JdbcSagaStore store) {
            this.store = store;
            this.saga = Saga.define("order-saga")
                    .step("debit-account", account.debit(80_00), account.refund(80_00))
                    .parallel("prepare", p -> p
                            .step("reserve-stock", inventory.reserve(3), inventory.release(3))
                            .step("add-points", points.add(120), points.revoke(120)))
                    .build();
        }

        SagaOrchestrator newOrchestrator(ConfigurableFaultInjector faults) {
            SagaOrchestrator.Builder b = SagaOrchestrator.builder(store);
            if (faults != null) {
                b.faultInjector(faults);
            }
            return b.build();
        }
    }
}
