package io.saga.participant;

import io.saga.core.SagaAction;

/** Abstract loyalty-points service: add / revoke points, no real business logic. */
public class PointsParticipant extends Participant {

    private long points;

    public PointsParticipant(long initialPoints) {
        super("points");
        this.points = initialPoints;
    }

    public SagaAction add(long amount) {
        return ctx -> forward(ctx.idempotencyKey(), "add " + amount, () -> points += amount);
    }

    public SagaAction revoke(long amount) {
        return ctx -> compensate(ctx.idempotencyKey(), "revoke " + amount, () -> points -= amount);
    }

    public synchronized long getPoints() {
        return points;
    }
}
