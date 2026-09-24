package com.example.saga.example;

import com.example.saga.coordinator.FaultInjector;
import com.example.saga.coordinator.RetryPolicy;
import com.example.saga.coordinator.Saga;
import com.example.saga.coordinator.SagaContext;
import com.example.saga.coordinator.SagaCrashException;
import com.example.saga.coordinator.SagaDefinition;
import com.example.saga.coordinator.SagaEngine;
import com.example.saga.coordinator.SagaEvent;
import com.example.saga.coordinator.SagaRepository;
import com.example.saga.coordinator.SagaResult;
import com.example.saga.coordinator.SagaStatus;
import com.example.saga.participant.BarrierResult;
import com.example.saga.participant.Participant;
import org.h2.jdbcx.JdbcDataSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates a saga spanning three participants (account, inventory, points)
 * under several injected-fault scenarios, printing the final state and the
 * persisted event timeline of every transaction.
 */
public class OrderSagaDemo {

    private final List<String> participantLog = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws Exception {
        OrderSagaDemo demo = new OrderSagaDemo();
        demo.happyPath();
        demo.failBeforeStep();
        demo.failAfterStep();
        demo.nullRollbackAndSuspension();
        demo.crashAndRecover(Path.of("target", "example-recovery-db"));
    }

    SagaResult happyPath() {
        banner("Scenario 1: happy path");
        Participants p = participants("happy");
        SagaDefinition saga = orderSaga(p);
        try (SagaEngine engine = new SagaEngine(SagaRepository.inMemory("demo-happy"))) {
            SagaResult result = engine.execute(saga);
            printOutcome(engine, result);
            return result;
        }
    }

    SagaResult failBeforeStep() {
        banner("Scenario 2: fault injected BEFORE step 1 (reserve-inventory never runs)");
        Participants p = participants("before");
        SagaDefinition saga = orderSaga(p);
        try (SagaEngine engine = new SagaEngine(SagaRepository.inMemory("demo-before"))) {
            engine.setFaultInjector(FaultInjector.none().failBeforeStep(1));
            SagaResult result = engine.execute(saga);
            printOutcome(engine, result);
            return result;
        }
    }

    SagaResult failAfterStep() {
        banner("Scenario 3: fault injected AFTER step 0 (debit executed, outcome lost)");
        Participants p = participants("after");
        SagaDefinition saga = orderSaga(p);
        try (SagaEngine engine = new SagaEngine(SagaRepository.inMemory("demo-after"))) {
            engine.setFaultInjector(FaultInjector.none().failAfterStep(0));
            SagaResult result = engine.execute(saga);
            printOutcome(engine, result);
            return result;
        }
    }

    SagaResult nullRollbackAndSuspension() throws Exception {
        banner("Scenario 4: request lost in transit -> null rollback, late arrival -> suspension");
        Participants p = participants("anomaly");
        AtomicInteger reserveRuns = new AtomicInteger();

        SagaDefinition saga = Saga.define("place-order-anomaly")
                .step("debit-account",
                        p.account().forward(ctx -> log("account: debit 100")),
                        p.account().compensation(ctx -> log("account: refund 100")))
                .step("reserve-inventory",
                        ctx -> Thread.sleep(10_000), // request never reaches the participant in time
                        p.inventory().compensation(ctx -> log("inventory: release 1 item")),
                        Duration.ofMillis(150), RetryPolicy.none())
                .build();

        String sagaId;
        SagaResult result;
        try (SagaEngine engine = new SagaEngine(SagaRepository.inMemory("demo-anomaly"))) {
            result = engine.execute(saga, "anomaly-saga");
            sagaId = result.sagaId();
            printOutcome(engine, result);
        }

        System.out.println("-- the delayed reserve request finally arrives at the participant --");
        BarrierResult late = p.inventory().barrier().tryForward(sagaId, "reserve-inventory",
                ctx -> {
                    reserveRuns.incrementAndGet();
                    log("inventory: reserve 1 item");
                }, new SagaContext(sagaId, "place-order-anomaly"));
        System.out.println("late forward result: " + late + " (reserve executed " + reserveRuns.get() + " time(s))");
        System.out.println();
        return result;
    }

    SagaResult crashAndRecover(Path dbDir) {
        banner("Scenario 5: coordinator crashes after step 0, a new process recovers the saga");
        Participants p = participants("recovery");
        SagaDefinition saga = orderSaga(p);

        SagaEngine first = new SagaEngine(SagaRepository.at(dbDir));
        first.setFaultInjector(FaultInjector.none().crashAfterStep(0));
        String sagaId = "recovery-saga";
        try {
            first.execute(saga, sagaId);
        } catch (SagaCrashException crash) {
            System.out.println("*** process crashed: " + crash.getMessage() + " ***");
        } finally {
            first.close();
        }
        System.out.println("-- restarting coordinator against the same H2 database --");

        try (SagaEngine second = new SagaEngine(SagaRepository.at(dbDir))) {
            second.register(saga);
            int recovered = second.recover();
            System.out.println("recovered " + recovered + " unfinished saga(s)");
            SagaResult result = second.result(sagaId);
            printOutcome(second, result);
            return result;
        }
    }

    private SagaDefinition orderSaga(Participants p) {
        return Saga.define("place-order")
                .step("debit-account",
                        p.account().forward(ctx -> log("account: debit 100")),
                        p.account().compensation(ctx -> log("account: refund 100")))
                .parallel(group -> group
                        .step("reserve-inventory",
                                p.inventory().forward(ctx -> log("inventory: reserve 1 item")),
                                p.inventory().compensation(ctx -> log("inventory: release 1 item")))
                        .step("earn-points",
                                p.points().forward(ctx -> log("points: earn 10 points")),
                                p.points().compensation(ctx -> log("points: revoke 10 points"))))
                .build();
    }

    private Participants participants(String scenario) {
        Participant account = new Participant("account-" + scenario, freshDataSource("account"));
        Participant inventory = new Participant("inventory-" + scenario, freshDataSource("inventory"));
        Participant points = new Participant("points-" + scenario, freshDataSource("points"));
        for (Participant participant : List.of(account, inventory, points)) {
            participant.setLogger(message -> {
                participantLog.add(message);
                System.out.println("  barrier " + message);
            });
        }
        return new Participants(account, inventory, points);
    }

    private static JdbcDataSource freshDataSource(String name) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + name + "-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }

    private void printOutcome(SagaEngine engine, SagaResult result) {
        System.out.println("final status : " + result.status()
                + (result.status() == SagaStatus.COMPLETED ? "" : "  reason: " + result.failureReason()));
        System.out.println("event timeline:");
        for (SagaEvent event : engine.timeline(result.sagaId())) {
            System.out.println("  " + event);
        }
        System.out.println();
    }

    private void log(String message) {
        participantLog.add(message);
        System.out.println("  action  " + message);
    }

    List<String> participantLog() {
        return participantLog;
    }

    private static void banner(String title) {
        System.out.println();
        System.out.println("=== " + title + " ===");
    }

    private record Participants(Participant account, Participant inventory, Participant points) {
    }
}
