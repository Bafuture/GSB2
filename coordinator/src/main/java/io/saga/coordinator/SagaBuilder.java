package io.saga.coordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fluent DSL for a saga definition, e.g.
 * <pre>
 * Saga.define("order")
 *     .step("debit",  this::debit,  this::refund)
 *     .parallel("extras", p -&gt; p
 *         .step("points", this::addPoints, this::revertPoints)
 *         .step("coupon", this::issueCoupon, this::voidCoupon))
 *     .build();
 * </pre>
 */
public final class SagaBuilder {
    private final String name;
    private final List<Phase> phases = new ArrayList<>();
    private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;
    private Duration stepTimeout = Duration.ofSeconds(5);

    SagaBuilder(String name) {
        this.name = name;
    }

    public SagaBuilder step(String name, SagaAction action, SagaAction compensation) {
        phases.add(new SagaStep(name, action, compensation));
        return this;
    }

    public SagaBuilder parallel(String groupName, Consumer<ParallelBuilder> configurer) {
        ParallelBuilder builder = new ParallelBuilder();
        configurer.accept(builder);
        phases.add(new ParallelGroup(groupName, builder.steps));
        return this;
    }

    public SagaBuilder retry(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    public SagaBuilder stepTimeout(Duration timeout) {
        this.stepTimeout = timeout;
        return this;
    }

    public SagaDefinition build() {
        return new SagaDefinition(name, phases, retryPolicy, stepTimeout);
    }

    public static final class ParallelBuilder {
        private final List<SagaStep> steps = new ArrayList<>();

        public ParallelBuilder step(String name, SagaAction action, SagaAction compensation) {
            steps.add(new SagaStep(name, action, compensation));
            return this;
        }
    }
}
