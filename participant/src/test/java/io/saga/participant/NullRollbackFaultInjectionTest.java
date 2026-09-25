package io.saga.participant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Fault injection: the forward request is dropped by the network, so the
 * compensation arrives for a branch that never executed (null rollback).
 * The barrier must absorb it, report success, and record state so a later
 * forward is rejected.
 */
class NullRollbackFaultInjectionTest {

    @Test
    void compensationWithoutForwardIsAbsorbedAsNullRollback() throws Exception {
        Barrier barrier = BarrierTestSupport.newBarrier();
        AtomicInteger balance = new AtomicInteger(100);
        FaultyTransport transport = new FaultyTransport(barrier);
        transport.dropForward("tx-null", "debit");

        transport.forward("tx-null", "debit", () -> balance.addAndGet(-30)); // dropped in transit
        boolean compensationExecuted =
                transport.compensate("tx-null", "debit", () -> balance.addAndGet(30));

        assertFalse(compensationExecuted, "null rollback must not run the compensation action");
        assertEquals(100, balance.get(), "balance must be untouched when the forward never ran");
    }

    @Test
    void nullRollbackBarrierBlocksTheLateForward() throws Exception {
        Barrier barrier = BarrierTestSupport.newBarrier();
        AtomicInteger balance = new AtomicInteger(100);
        FaultyTransport transport = new FaultyTransport(barrier);
        transport.dropForward("tx-null-2", "debit");

        transport.forward("tx-null-2", "debit", () -> balance.addAndGet(-30));
        transport.compensate("tx-null-2", "debit", () -> balance.addAndGet(30));
        transport.deliverDropped(); // the "lost" forward finally arrives

        assertEquals(100, balance.get(), "late forward after null rollback must be rejected");
    }
}
