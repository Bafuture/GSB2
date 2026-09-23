package io.saga.participant;

import io.saga.core.SagaAction;

/** Abstract inventory service: reserve / release stock, no real business logic. */
public class InventoryParticipant extends Participant {

    private int availableUnits;

    public InventoryParticipant(int initialUnits) {
        super("inventory");
        this.availableUnits = initialUnits;
    }

    public SagaAction reserve(int units) {
        return ctx -> forward(ctx.idempotencyKey(), "reserve " + units, () -> availableUnits -= units);
    }

    public SagaAction release(int units) {
        return ctx -> compensate(ctx.idempotencyKey(), "release " + units, () -> availableUnits += units);
    }

    public synchronized int getAvailableUnits() {
        return availableUnits;
    }
}
