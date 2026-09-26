package com.example.saga.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fluent DSL for defining a saga:
 *
 * <pre>{@code
 * SagaDefinition saga = SagaBuilder.named("order")
 *     .step("account.debit", this::debit, this::refund)
 *     .parallel("fulfil", group -> group
 *         .step("inventory.reserve", this::reserve, this::release)
 *         .step("points.add", this::addPoints, this::deductPoints))
 *     .step("notify", this::notifyUser, this::noop)
 *     .build();
 * }</pre>
 */
public final class SagaBuilder {

    private final String name;
    private final List<SagaStage> stages = new ArrayList<>();

    private SagaBuilder(String name) {
        this.name = name;
    }

    public static SagaBuilder named(String name) {
        return new SagaBuilder(name);
    }

    /** Appends a serial step. */
    public SagaBuilder step(String name, SagaAction action, SagaCompensation compensation) {
        return step(new SagaStep(name, action, compensation));
    }

    /** Appends a serial step. */
    public SagaBuilder step(SagaStep step) {
        stages.add(SagaStage.serial(step));
        return this;
    }

    /** Appends a group of steps executed in parallel. */
    public SagaBuilder parallel(String groupName, Consumer<ParallelGroupBuilder> group) {
        ParallelGroupBuilder builder = new ParallelGroupBuilder();
        group.accept(builder);
        stages.add(SagaStage.parallel(groupName, builder.steps));
        return this;
    }

    public SagaDefinition build() {
        if (stages.isEmpty()) {
            throw new IllegalStateException("a saga needs at least one step");
        }
        return new SagaDefinition(name, stages);
    }

    /** Builder for the steps of one parallel group. */
    public static final class ParallelGroupBuilder {
        private final List<SagaStep> steps = new ArrayList<>();

        public ParallelGroupBuilder step(String name, SagaAction action, SagaCompensation compensation) {
            return step(new SagaStep(name, action, compensation));
        }

        public ParallelGroupBuilder step(SagaStep step) {
            steps.add(step);
            return this;
        }
    }
}
