package io.saga.participant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Compensation must be idempotent: applying it N times has the same effect as applying it once. */
class BarrierIdempotencyTest {

    @Test
    void duplicateForwardRequestsExecuteTheActionOnlyOnce() throws Exception {
        Barrier barrier = BarrierTestSupport.newBarrier();
        AtomicInteger balance = new AtomicInteger(100);

        assertTrue(barrier.forward("tx-1", "debit", () -> balance.addAndGet(-30)));
        assertFalse(barrier.forward("tx-1", "debit", () -> balance.addAndGet(-30)));
        assertFalse(barrier.forward("tx-1", "debit", () -> balance.addAndGet(-30)));

        assertEquals(70, balance.get(), "duplicate forward delivery must not debit twice");
    }

    @Test
    void duplicateCompensationsExecuteOnlyOnce() throws Exception {
        Barrier barrier = BarrierTestSupport.newBarrier();
        AtomicInteger balance = new AtomicInteger(100);
        AtomicInteger compensationRuns = new AtomicInteger();

        barrier.forward("tx-2", "debit", () -> balance.addAndGet(-30));
        assertEquals(70, balance.get());

        ParticipantAction refund = () -> {
            compensationRuns.incrementAndGet();
            balance.addAndGet(30);
        };
        assertTrue(barrier.compensate("tx-2", "debit", refund));
        assertFalse(barrier.compensate("tx-2", "debit", refund));
        assertFalse(barrier.compensate("tx-2", "debit", refund));

        assertEquals(1, compensationRuns.get(), "compensation action must run exactly once");
        assertEquals(100, balance.get(), "repeated compensation must converge to the same state");
    }
}
