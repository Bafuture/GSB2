package io.saga.participant;

/**
 * A saga participant. Wraps business actions with barrier protection so the
 * coordinator may safely retry or reorder forward/compensation requests.
 */
public final class Participant {

    private final String name;
    private final Barrier barrier;

    public Participant(String name, Barrier barrier) {
        this.name = name;
        this.barrier = barrier;
    }

    public String name() {
        return name;
    }

    /** Executes the forward action under barrier protection. */
    public boolean forward(String txId, String stepId, ParticipantAction action) throws Exception {
        return barrier.forward(txId, stepId, action);
    }

    /** Executes the compensation under barrier protection. */
    public boolean compensate(String txId, String stepId, ParticipantAction action) throws Exception {
        return barrier.compensate(txId, stepId, action);
    }
}
