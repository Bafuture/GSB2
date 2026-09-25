package io.saga.example;

import io.saga.coordinator.FaultInjector;
import io.saga.coordinator.RetryPolicy;
import io.saga.coordinator.Saga;
import io.saga.coordinator.SagaDefinition;
import io.saga.coordinator.SagaEvent;
import io.saga.coordinator.SagaInstance;
import io.saga.coordinator.SagaOrchestrator;
import io.saga.coordinator.SagaRepository;
import io.saga.coordinator.SagaStatus;
import io.saga.participant.Barrier;
import io.saga.participant.BarrierStore;
import io.saga.participant.Participant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;

/**
 * End-to-end demo of a saga spanning three participants (account, inventory,
 * points). Runs one happy-path scenario plus the fault-injection scenarios and
 * prints the final state and the persisted event timeline of each.
 */
public final class OrderSagaDemo {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    private static final AtomicInteger DB_COUNTER = new AtomicInteger();

    public record ScenarioResult(String name, String finalStatus, String failReason, List<String> timeline) {
    }

    public static void main(String[] args) {
        for (ScenarioResult scenario : runScenarios()) {
            System.out.println("==============================================================");
            System.out.println("场景: " + scenario.name());
            System.out.println("最终状态: " + scenario.finalStatus()
                    + (scenario.failReason() == null ? "" : "  (原因: " + scenario.failReason() + ")"));
            System.out.println("事件时间线:");
            scenario.timeline().forEach(line -> System.out.println("  " + line));
            System.out.println();
        }
    }

    public static List<ScenarioResult> runScenarios() {
        List<ScenarioResult> results = new ArrayList<>();
        results.add(happyPath());
        results.add(faultBeforeStep());
        results.add(faultAfterStep());
        results.add(nullRollback());
        results.add(hanging());
        results.add(retryExhausted());
        return results;
    }

    /** 1. No faults: the whole chain commits. */
    static ScenarioResult happyPath() {
        Fixture fixture = new Fixture(new FaultInjector());
        String sagaId = fixture.orchestrator.submit(fixture.saga());
        SagaInstance result = fixture.orchestrator.awaitTerminal(sagaId, Duration.ofSeconds(10));
        ScenarioResult scenario = new ScenarioResult("正常流程（无故障）",
                result.status().name(), result.failReason(), fixture.timeline(sagaId));
        fixture.orchestrator.close();
        return scenario;
    }

    /** 2. Injected fault before step 2 executes. */
    static ScenarioResult faultBeforeStep() {
        Fixture fixture = new Fixture(new FaultInjector().failBefore("reserve"));
        String sagaId = fixture.orchestrator.submit(fixture.saga());
        SagaInstance result = fixture.orchestrator.awaitTerminal(sagaId, Duration.ofSeconds(10));
        ScenarioResult scenario = new ScenarioResult("故障注入：第 2 步（reserve）执行前失败",
                result.status().name(), result.failReason(), fixture.timeline(sagaId));
        fixture.orchestrator.close();
        return scenario;
    }

    /** 3. Injected fault after step 2 executed (its result was persisted). */
    static ScenarioResult faultAfterStep() {
        Fixture fixture = new Fixture(new FaultInjector().failAfter("reserve"));
        String sagaId = fixture.orchestrator.submit(fixture.saga());
        SagaInstance result = fixture.orchestrator.awaitTerminal(sagaId, Duration.ofSeconds(10));
        ScenarioResult scenario = new ScenarioResult("故障注入：第 2 步（reserve）执行后失败",
                result.status().name(), result.failReason(), fixture.timeline(sagaId));
        fixture.orchestrator.close();
        return scenario;
    }

    /** 4. Null rollback: the compensation arrives although the forward never ran. */
    static ScenarioResult nullRollback() {
        Fixture fixture = new Fixture(new FaultInjector());
        List<String> log = new ArrayList<>();
        String txId = "tx-null-rollback";
        try {
            log.add("网络故障注入：debit 的正向请求在传输中丢失");
            log.add("协调器随后发来 debit 的补偿请求（空回滚）");
            fixture.account.refund(txId, "debit", 100);
            log.add("屏障记录空回滚，补偿动作未执行，账户余额=" + fixture.account.balance());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        fixture.orchestrator.close();
        return new ScenarioResult("异常防护：空回滚（正向未执行，补偿先到）", "ABSORBED", null, log);
    }

    /** 5. Hanging: the compensation is delivered before the forward action. */
    static ScenarioResult hanging() {
        Fixture fixture = new Fixture(new FaultInjector());
        List<String> log = new ArrayList<>();
        String txId = "tx-hanging";
        try {
            log.add("网络故障注入：消息乱序，debit 的补偿请求先于正向请求到达");
            fixture.account.refund(txId, "debit", 100);
            log.add("补偿先到：屏障记录空回滚，余额=" + fixture.account.balance());
            fixture.account.debit(txId, "debit", 100);
            log.add("正向请求迟到：被屏障拒绝（悬挂防护），余额=" + fixture.account.balance());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        fixture.orchestrator.close();
        return new ScenarioResult("异常防护：悬挂（补偿先到、正向后到）", "REJECTED-LATE-FORWARD", null, log);
    }

    /** 6. Points service stays down: retries are exhausted and the saga fails with a reason. */
    static ScenarioResult retryExhausted() {
        Fixture fixture = new Fixture(new FaultInjector());
        fixture.points.setDown(true);
        String sagaId = fixture.orchestrator.submit(fixture.saga());
        SagaInstance result = fixture.orchestrator.awaitTerminal(sagaId, Duration.ofSeconds(10));
        ScenarioResult scenario = new ScenarioResult("超时与重试：points 服务持续不可用，重试耗尽",
                result.status().name(), result.failReason(), fixture.timeline(sagaId));
        fixture.orchestrator.close();
        return scenario;
    }

    /** Wires a fresh database, participants and orchestrator for one scenario. */
    private static final class Fixture {
        final AccountService account;
        final InventoryService inventory;
        final PointsService points;
        final SagaOrchestrator orchestrator;

        Fixture(FaultInjector faults) {
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:demo-" + DB_COUNTER.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
            ds.setUser("sa");
            Barrier barrier = new Barrier(new BarrierStore(ds));
            this.account = new AccountService(new Participant("account", barrier));
            this.inventory = new InventoryService(new Participant("inventory", barrier));
            this.points = new PointsService(new Participant("points", barrier));
            this.orchestrator = new SagaOrchestrator(new SagaRepository(ds), faults);
        }

        SagaDefinition saga() {
            return Saga.define("order-saga")
                    .retry(RetryPolicy.exponential(3, Duration.ofMillis(50), 2.0))
                    .stepTimeout(Duration.ofSeconds(1))
                    .step("debit",
                            ctx -> account.debit(ctx.sagaId(), "debit", 100),
                            ctx -> account.refund(ctx.sagaId(), "debit", 100))
                    .step("reserve",
                            ctx -> inventory.reserve(ctx.sagaId(), "reserve", 3),
                            ctx -> inventory.release(ctx.sagaId(), "reserve", 3))
                    .step("points",
                            ctx -> points.add(ctx.sagaId(), "points", 10),
                            ctx -> points.deduct(ctx.sagaId(), "points", 10))
                    .build();
        }

        List<String> timeline(String sagaId) {
            List<String> lines = new ArrayList<>();
            for (SagaEvent event : orchestrator.timeline(sagaId)) {
                lines.add(TIME.format(event.timestamp()) + "  " + event.type() + "  " + event.message());
            }
            return lines;
        }
    }

    private OrderSagaDemo() {
    }
}
