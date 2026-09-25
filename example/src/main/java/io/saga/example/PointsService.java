package io.saga.example;

import io.saga.participant.Participant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Demo participant: loyalty points. Can be switched "down" to demo retry exhaustion. */
public final class PointsService {

    private final Participant participant;
    private final AtomicInteger points = new AtomicInteger(0);
    private final AtomicBoolean down = new AtomicBoolean(false);

    public PointsService(Participant participant) {
        this.participant = participant;
    }

    public void setDown(boolean value) {
        down.set(value);
    }

    public void add(String txId, String stepId, int amount) throws Exception {
        participant.forward(txId, stepId, () -> {
            if (down.get()) {
                throw new IllegalStateException("points service unavailable");
            }
            points.addAndGet(amount);
            System.out.printf("    [points]    add    %4d -> points=%d%n", amount, points.get());
        });
    }

    public void deduct(String txId, String stepId, int amount) throws Exception {
        participant.compensate(txId, stepId, () -> {
            points.addAndGet(-amount);
            System.out.printf("    [points]    deduct %4d -> points=%d%n", amount, points.get());
        });
    }

    public int points() {
        return points.get();
    }
}
