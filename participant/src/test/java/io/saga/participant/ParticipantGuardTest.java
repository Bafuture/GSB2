package io.saga.participant;

import io.saga.core.SagaContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit-level proof of the participant-side anomaly guards:
 * idempotent compensation, empty rollback and dangling rejection.
 */
class ParticipantGuardTest {

    private AccountParticipant account;
    private SagaContext context;

    @BeforeEach
    void setUp() {
        account = new AccountParticipant(1_000);
        context = new SagaContext("saga-1");
        SagaContext.setCurrentStep("debit");
    }

    @AfterEach
    void tearDown() {
        SagaContext.clearCurrentStep();
    }

    @Test
    void compensationAppliedMultipleTimesHasSameResultAsOnce() throws Exception {
        account.debit(400).execute(context);
        assertEquals(600, account.getBalanceCents());

        account.refund(400).execute(context);
        account.refund(400).execute(context); // duplicate delivery
        account.refund(400).execute(context);

        assertEquals(1_000, account.getBalanceCents()); // effect applied exactly once
        assertTrue(account.getOperationLog().stream().anyMatch(l -> l.contains("DUPLICATE_COMPENSATION_IGNORED")));
    }

    @Test
    void emptyRollbackIsASuccessfulNoOp() throws Exception {
        // compensation arrives although the forward action never executed
        account.refund(400).execute(context);

        assertEquals(1_000, account.getBalanceCents()); // nothing to undo
        assertTrue(account.getOperationLog().stream().anyMatch(l -> l.contains("EMPTY_ROLLBACK_IGNORED")));
    }

    @Test
    void danglingForwardArrivingAfterCompensationIsRejected() throws Exception {
        // compensation first (empty rollback), then the belated forward action
        account.refund(400).execute(context);
        account.debit(400).execute(context);

        assertEquals(1_000, account.getBalanceCents()); // forward must not apply
        assertTrue(account.getOperationLog().stream().anyMatch(l -> l.contains("DANGLING_FORWARD_REJECTED")));
    }

    @Test
    void duplicateForwardDeliveryIsIgnored() throws Exception {
        account.debit(400).execute(context);
        account.debit(400).execute(context);

        assertEquals(600, account.getBalanceCents());
        assertTrue(account.getOperationLog().stream().anyMatch(l -> l.contains("DUPLICATE_FORWARD_IGNORED")));
    }
}
