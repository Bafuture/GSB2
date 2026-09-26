package com.example.saga.core;

import java.util.List;

/**
 * A stage of a saga definition. A stage is either a single step executed serially,
 * or a named group of steps executed in parallel (the stage succeeds only if all
 * of its steps succeed).
 */
public final class SagaStage {

    private final String name;
    private final List<SagaStep> steps;
    private final boolean parallel;

    private SagaStage(String name, List<SagaStep> steps, boolean parallel) {
        this.name = name;
        this.steps = List.copyOf(steps);
        this.parallel = parallel;
    }

    public static SagaStage serial(SagaStep step) {
        return new SagaStage(step.getName(), List.of(step), false);
    }

    public static SagaStage parallel(String name, List<SagaStep> steps) {
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("a parallel group needs at least one step");
        }
        return new SagaStage(name, steps, true);
    }

    public String getName() {
        return name;
    }

    public List<SagaStep> getSteps() {
        return steps;
    }

    public boolean isParallel() {
        return parallel;
    }
}
