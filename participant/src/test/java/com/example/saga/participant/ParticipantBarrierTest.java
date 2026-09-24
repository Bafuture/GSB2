package com.example.saga.participant;

import com.example.saga.coordinator.SagaContext;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParticipantBarrierTest {

    private ParticipantBarrier barrier;
    private SagaContext ctx;

    @BeforeEach
    void setUp() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        barrier = new ParticipantBarrier(ds, "account");
        ctx = new SagaContext("saga-1", "test");
    }

    @Test
    void compensationIsIdempotentWhenRequestedTwice() throws Exception {
        AtomicInteger forwardRuns = new AtomicInteger();
        AtomicInteger compensationRuns = new AtomicInteger();

        assertEquals(BarrierResult.FORWARD_EXECUTED,
                barrier.tryForward("saga-1", "debit", c -> forwardRuns.incrementAndGet(), ctx));
        assertEquals(BarrierResult.COMPENSATED,
                barrier.tryCompensate("saga-1", "debit", c -> compensationRuns.incrementAndGet(), ctx));
        // same compensation delivered again (e.g. coordinator retry after a timeout)
        assertEquals(BarrierResult.COMPENSATE_DUPLICATE,
                barrier.tryCompensate("saga-1", "debit", c -> compensationRuns.incrementAndGet(), ctx));

        assertEquals(1, forwardRuns.get());
        assertEquals(1, compensationRuns.get(), "compensation must be applied exactly once");
    }

    @Test
    void duplicateForwardIsNotReExecuted() throws Exception {
        AtomicInteger forwardRuns = new AtomicInteger();

        assertEquals(BarrierResult.FORWARD_EXECUTED,
                barrier.tryForward("saga-1", "debit", c -> forwardRuns.incrementAndGet(), ctx));
        assertEquals(BarrierResult.FORWARD_DUPLICATE,
                barrier.tryForward("saga-1", "debit", c -> forwardRuns.incrementAndGet(), ctx));

        assertEquals(1, forwardRuns.get());
    }

    @Test
    void nullRollbackCompensateWithoutForwardIsANoOp() throws Exception {
        AtomicInteger compensationRuns = new AtomicInteger();

        // compensation arrives although the forward action never executed
        BarrierResult result = barrier.tryCompensate("saga-1", "debit",
                c -> compensationRuns.incrementAndGet(), ctx);

        assertEquals(BarrierResult.COMPENSATE_NULL_ROLLBACK, result);
        assertEquals(0, compensationRuns.get(), "nothing was done, so nothing may be undone");
    }

    @Test
    void suspensionForwardArrivingAfterCompensationIsRejected() throws Exception {
        AtomicInteger forwardRuns = new AtomicInteger();
        AtomicInteger compensationRuns = new AtomicInteger();

        // the compensation request wins the race and is recorded as a null rollback
        assertEquals(BarrierResult.COMPENSATE_NULL_ROLLBACK,
                barrier.tryCompensate("saga-1", "debit", c -> compensationRuns.incrementAndGet(), ctx));
        // the delayed forward request finally arrives and must be rejected
        BarrierResult late = barrier.tryForward("saga-1", "debit",
                c -> forwardRuns.incrementAndGet(), ctx);

        assertEquals(BarrierResult.FORWARD_SUSPENDED, late);
        assertEquals(0, forwardRuns.get(), "suspended forward must not execute");
        assertEquals(0, compensationRuns.get());
    }

    @Test
    void failedForwardReleasesBarrierSoRetryCanReExecute() throws Exception {
        AtomicInteger forwardRuns = new AtomicInteger();

        try {
            barrier.tryForward("saga-1", "debit", c -> {
                forwardRuns.incrementAndGet();
                throw new IllegalStateException("boom");
            }, ctx);
        } catch (IllegalStateException expected) {
            // first attempt failed, barrier rolled back
        }
        BarrierResult retry = barrier.tryForward("saga-1", "debit",
                c -> forwardRuns.incrementAndGet(), ctx);

        assertEquals(BarrierResult.FORWARD_EXECUTED, retry);
        assertEquals(2, forwardRuns.get());
    }
}
