package io.saga.coordinator;

import java.util.List;
import java.util.Objects;

/** A group of steps executed concurrently; all must succeed for the saga to proceed. */
public final class ParallelGroup implements Phase {
    private final String name;
    private final List<SagaStep> steps;

    public ParallelGroup(String name, List<SagaStep> steps) {
        this.name = Objects.requireNonNull(name, "name");
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("parallel group must contain at least one step");
        }
        this.steps = List.copyOf(steps);
    }

    @Override
    public String name() {
        return name;
    }

    public List<SagaStep> steps() {
        return steps;
    }
}
