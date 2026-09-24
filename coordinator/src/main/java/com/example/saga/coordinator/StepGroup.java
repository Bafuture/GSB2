package com.example.saga.coordinator;

import java.util.List;

public record StepGroup(List<SagaStep> steps, boolean parallel) {
    public StepGroup {
        steps = List.copyOf(steps);
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("a step group must contain at least one step");
        }
    }

    static StepGroup serial(SagaStep step) {
        return new StepGroup(List.of(step), false);
    }

    static StepGroup parallel(List<SagaStep> steps) {
        return new StepGroup(steps, true);
    }
}
