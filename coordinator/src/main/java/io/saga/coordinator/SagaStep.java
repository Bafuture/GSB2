package io.saga.coordinator;

import java.util.Objects;

/** A single saga step: a forward action plus its idempotent compensation. */
public final class SagaStep implements Phase {
    private final String name;
    private final SagaAction action;
    private final SagaAction compensation;

    public SagaStep(String name, SagaAction action, SagaAction compensation) {
        this.name = Objects.requireNonNull(name, "name");
        this.action = Objects.requireNonNull(action, "action");
        this.compensation = Objects.requireNonNull(compensation, "compensation");
    }

    @Override
    public String name() {
        return name;
    }

    public SagaAction action() {
        return action;
    }

    public SagaAction compensation() {
        return compensation;
    }
}
