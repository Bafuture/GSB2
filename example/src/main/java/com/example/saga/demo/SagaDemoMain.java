package com.example.saga.demo;

import com.example.saga.core.FaultInjection;
import com.example.saga.core.FaultInjector;
import com.example.saga.core.SagaEngine;
import com.example.saga.core.SagaResult;
import com.example.saga.participant.BarrierResult;
import com.example.saga.participant.ParticipantBarrier;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.util.UUID;

/**
 * Runs the order saga under several fault-injection scenarios and prints the final
 * transaction state plus the persisted event timeline of each run.
 */
public final class SagaDemoMain {

    private SagaDemoMain() {
    }

    public static void main(String[] args) throws Exception {
        scenario("1) happy path - no faults", FaultInjector.NONE);
        scenario("2) fault injected BEFORE step 2 (inventory.reserve)", FaultInjection.failBeforeStep(2));
        scenario("3) fault injected AFTER step 2 (inventory.reserve)", FaultInjection.failAfterStep(2));
        nullRollbackScenario();
        suspensionScenario();
    }

    static SagaResult scenario(String title, FaultInjector injector) {
        DemoWorld world = new DemoWorld();
        SagaEngine engine = new SagaEngine(freshDataSource(), injector);
        SagaResult result = engine.execute(world.orderSaga());

        System.out.println("=".repeat(78));
        System.out.println(title);
        System.out.println("-".repeat(78));
        System.out.println("final state : " + result.status()
                + (result.failReason() != null ? "  (reason: " + result.failReason() + ")" : ""));
        System.out.println("participants: " + world.participantStates());
        System.out.println("event timeline:");
        System.out.print(result.timeline());
        return result;
    }

    /** (a) Null rollback: a compensation arrives for a step whose forward action never ran. */
    static BarrierResult nullRollbackScenario() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(freshDataSource());
        String sagaId = "demo-null-rollback";
        BarrierResult result = barrier.compensate(sagaId, "inventory.reserve",
                () -> System.out.println("    !! compensation logic ran (should not happen)"));

        System.out.println("=".repeat(78));
        System.out.println("4) injected anomaly: NULL ROLLBACK (compensate without try)");
        System.out.println("-".repeat(78));
        System.out.println("barrier outcome: " + result
                + "  -> compensation acknowledged without running any undo logic");
        return result;
    }

    /** (b) Suspension: the compensation arrives before the forward action; the late try is rejected. */
    static BarrierResult suspensionScenario() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(freshDataSource());
        String sagaId = "demo-suspension";
        barrier.compensate(sagaId, "points.add",
                () -> System.out.println("    !! compensation logic ran (should not happen)"));
        BarrierResult lateTry = barrier.tryExecute(sagaId, "points.add",
                () -> System.out.println("    !! forward action ran (should not happen)"));

        System.out.println("=".repeat(78));
        System.out.println("5) injected anomaly: SUSPENSION (compensate arrives before try)");
        System.out.println("-".repeat(78));
        System.out.println("barrier outcome: " + lateTry
                + "  -> the late forward action was rejected, branch stays rolled back");
        return lateTry;
    }

    private static DataSource freshDataSource() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:demo-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }
}
