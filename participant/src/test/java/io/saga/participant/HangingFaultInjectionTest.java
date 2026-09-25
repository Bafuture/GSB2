package io.saga.participant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Fault injection: message reordering delivers the compensation before the
 * forward action (hanging transaction). The barrier must reject the late
 * forward so the compensation is never undone.
 */
class HangingFaultInjectionTest {

    @Test
    void forwardArrivingAfterCompensationIsRejected() throws Exception {
        Barrier barrier = BarrierTestSupport.newBarrier();
        AtomicInteger balance = new AtomicInteger(100);
        FaultyTransport transport = new FaultyTransport(barrier);
        transport.delayForward("tx-hang", "debit");

        transport.forward("tx-hang", "debit", () -> balance.addAndGet(-30)); // parked, not delivered yet
        transport.compensate("tx-hang", "debit", () -> balance.addAndGet(30)); // arrives first: null rollback
        boolean forwardExecuted = transport.deliverDelayed(); // forward finally arrives

        assertFalse(forwardExecuted, "hanging forward must be rejected by the barrier");
        assertEquals(100, balance.get(), "late forward must not debit after compensation");
    }
}
