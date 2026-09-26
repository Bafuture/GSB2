package com.example.saga.participant;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Fault-injection test for the <b>suspension</b> anomaly: the compensation request
 * arrives before the forward (try) request. When the late try finally arrives it must
 * be rejected, otherwise the branch would execute work that can never be compensated
 * (the saga already considers it rolled back).
 */
class SuspensionFaultInjectionTest {

    @Test
    void lateTryAfterCompensationIsRejected() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(BarrierTestSupport.inMemoryDataSource());
        AtomicInteger tryRuns = new AtomicInteger();

        // Inject the fault: COMPENSATE is delivered first (null rollback).
        BarrierResult compensate = barrier.compensate("saga-1", "points.add", () -> { });
        assertEquals(BarrierResult.NULL_ROLLBACK, compensate);

        // The delayed TRY arrives afterwards and must be rejected.
        BarrierResult lateTry = barrier.tryExecute("saga-1", "points.add", tryRuns::incrementAndGet);

        assertEquals(BarrierResult.SUSPENDED, lateTry);
        assertEquals(0, tryRuns.get(), "a suspended forward action must never execute");
    }

    @Test
    void normalTryBeforeCompensationIsNotSuspended() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(BarrierTestSupport.inMemoryDataSource());
        AtomicInteger tryRuns = new AtomicInteger();
        AtomicInteger compensationRuns = new AtomicInteger();

        assertEquals(BarrierResult.EXECUTED,
                barrier.tryExecute("saga-2", "points.add", tryRuns::incrementAndGet));
        assertEquals(BarrierResult.EXECUTED,
                barrier.compensate("saga-2", "points.add", compensationRuns::incrementAndGet));

        assertEquals(1, tryRuns.get());
        assertEquals(1, compensationRuns.get());
    }
}
