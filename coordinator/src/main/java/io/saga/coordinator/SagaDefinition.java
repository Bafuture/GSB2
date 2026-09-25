package io.saga.coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable description of a saga: an ordered list of phases (steps / parallel groups). */
public final class SagaDefinition {
    private final String name;
    private final List<Phase> phases;
    private final RetryPolicy retryPolicy;
    private final Duration stepTimeout;

    SagaDefinition(String name, List<Phase> phases, RetryPolicy retryPolicy, Duration stepTimeout) {
        this.name = Objects.requireNonNull(name, "name");
        if (phases.isEmpty()) {
            throw new IllegalArgumentException("saga must contain at least one step");
        }
        this.phases = List.copyOf(phases);
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.stepTimeout = Objects.requireNonNull(stepTimeout, "stepTimeout");
    }

    public String name() {
        return name;
    }

    public List<Phase> phases() {
        return phases;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public Duration stepTimeout() {
        return stepTimeout;
    }

    /** All steps in execution order (parallel group members keep declaration order). */
    public List<SagaStep> allSteps() {
        List<SagaStep> all = new ArrayList<>();
        for (Phase phase : phases) {
            if (phase instanceof SagaStep step) {
                all.add(step);
            } else {
                all.addAll(((ParallelGroup) phase).steps());
            }
        }
        return all;
    }

    public Optional<SagaStep> findStep(String stepName) {
        return allSteps().stream().filter(s -> s.name().equals(stepName)).findFirst();
    }
}
