package com.example.saga.coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class Saga {

    private Saga() {
    }

    public static Builder define(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<StepGroup> groups = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder step(String stepName, SagaAction forward, SagaAction compensation) {
            groups.add(StepGroup.serial(new SagaStep(stepName, forward, compensation)));
            return this;
        }

        public Builder step(String stepName, SagaAction forward, SagaAction compensation,
                            Duration timeout, RetryPolicy retryPolicy) {
            groups.add(StepGroup.serial(new SagaStep(stepName, forward, compensation, timeout, retryPolicy)));
            return this;
        }

        /** Adds a group of steps that are executed concurrently; the saga continues once all of them succeed. */
        public Builder parallel(Consumer<ParallelBuilder> groupSpec) {
            ParallelBuilder pb = new ParallelBuilder();
            groupSpec.accept(pb);
            groups.add(StepGroup.parallel(pb.steps));
            return this;
        }

        public SagaDefinition build() {
            if (groups.isEmpty()) {
                throw new IllegalStateException("saga '" + name + "' has no steps");
            }
            return new SagaDefinition(name, groups);
        }
    }

    public static final class ParallelBuilder {
        private final List<SagaStep> steps = new ArrayList<>();

        public ParallelBuilder step(String stepName, SagaAction forward, SagaAction compensation) {
            steps.add(new SagaStep(stepName, forward, compensation));
            return this;
        }

        public ParallelBuilder step(String stepName, SagaAction forward, SagaAction compensation,
                                    Duration timeout, RetryPolicy retryPolicy) {
            steps.add(new SagaStep(stepName, forward, compensation, timeout, retryPolicy));
            return this;
        }
    }
}
