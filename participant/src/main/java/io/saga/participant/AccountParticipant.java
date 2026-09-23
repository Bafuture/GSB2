package io.saga.participant;

import io.saga.core.SagaAction;

/** Abstract account service: debit / refund, no real business logic. */
public class AccountParticipant extends Participant {

    private long balanceCents;

    public AccountParticipant(long initialBalanceCents) {
        super("account");
        this.balanceCents = initialBalanceCents;
    }

    public SagaAction debit(long cents) {
        return ctx -> forward(ctx.idempotencyKey(), "debit " + cents, () -> balanceCents -= cents);
    }

    public SagaAction refund(long cents) {
        return ctx -> compensate(ctx.idempotencyKey(), "refund " + cents, () -> balanceCents += cents);
    }

    public synchronized long getBalanceCents() {
        return balanceCents;
    }
}
