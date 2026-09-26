package com.example.saga.participant;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Fault-injection test for the <b>null rollback</b> anomaly: a compensation request
 * is delivered for a branch whose forward action was never executed (e.g. the try
 * request was lost on the network). The compensation must be acknowledged without
 * running the compensation logic.
 */
class NullRollbackFaultInjectionTest {

    @Test
    void compensationWithoutPriorTryIsNullRollback() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(BarrierTestSupport.inMemoryDataSource());
        AtomicInteger compensationRuns = new AtomicInteger();

        // Inject the fault: the COMPENSATE message arrives although TRY never happened.
        BarrierResult result = barrier.compensate("saga-1", "account.debit", compensationRuns::incrementAndGet);

        assertEquals(BarrierResult.NULL_ROLLBACK, result);
        assertEquals(0, compensationRuns.get(), "compensation logic must not run for a null rollback");
    }

    @Test
    void nullRollbackIsRememberedAcrossBarrierInstances() throws Exception {
        // Same database, new barrier instance (simulates a process restart in between).
        javax.sql.DataSource ds = BarrierTestSupport.inMemoryDataSource();
        ParticipantBarrier first = new ParticipantBarrier(ds);
        first.compensate("saga-2", "inventory.reserve", () -> { });

        ParticipantBarrier second = new ParticipantBarrier(ds);
        BarrierResult again = second.compensate("saga-2", "inventory.reserve", () -> { });

        assertEquals(BarrierResult.DUPLICATE_SKIPPED, again,
                "a null rollback is recorded persistently and not repeated after restart");
    }
}
