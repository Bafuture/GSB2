package com.example.saga.participant;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves that compensation guarded by the barrier is idempotent: delivering the same
 * compensation request any number of times applies its effect exactly once.
 */
class CompensationIdempotencyTest {

    @Test
    void duplicateCompensationIsAppliedExactlyOnce() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(BarrierTestSupport.inMemoryDataSource());
        AtomicInteger balance = new AtomicInteger(100);

        barrier.tryExecute("saga-1", "account.debit", () -> balance.addAndGet(-30));
        assertEquals(70, balance.get());

        // The same compensation request is delivered three times (e.g. coordinator retries).
        BarrierResult first = barrier.compensate("saga-1", "account.debit", () -> balance.addAndGet(30));
        BarrierResult second = barrier.compensate("saga-1", "account.debit", () -> balance.addAndGet(30));
        BarrierResult third = barrier.compensate("saga-1", "account.debit", () -> balance.addAndGet(30));

        assertEquals(BarrierResult.EXECUTED, first);
        assertEquals(BarrierResult.DUPLICATE_SKIPPED, second);
        assertEquals(BarrierResult.DUPLICATE_SKIPPED, third);
        assertEquals(100, balance.get(), "refund must be applied exactly once");
    }

    @Test
    void duplicateTryIsAppliedExactlyOnce() throws Exception {
        ParticipantBarrier barrier = new ParticipantBarrier(BarrierTestSupport.inMemoryDataSource());
        AtomicInteger balance = new AtomicInteger(100);

        assertEquals(BarrierResult.EXECUTED,
                barrier.tryExecute("saga-1", "account.debit", () -> balance.addAndGet(-30)));
        assertEquals(BarrierResult.DUPLICATE_SKIPPED,
                barrier.tryExecute("saga-1", "account.debit", () -> balance.addAndGet(-30)));

        assertEquals(70, balance.get(), "debit must be applied exactly once");
    }
}
