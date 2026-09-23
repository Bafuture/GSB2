package io.saga.core;

import java.time.Duration;
import java.util.Objects;

/** Immutable definition of one saga step: a forward action plus its compensation. */
public final class StepDefinition {

    private final String name;
    private final SagaAction forward;
    private final SagaAction compensation;
    private final Duration timeout;      // null -> orchestrator default
    private final RetryPolicy retryPolicy; // null -> orchestrator default
    private final String groupId;        // null -> serial step
    private int index = -1;              // global position, assigned at build time

    private StepDefinition(Builder b) {
        this.name = Objects.requireNonNull(b.name, "step name");
        this.forward = Objects.requireNonNull(b.forward, "forward action of step " + b.name);
        this.compensation = Objects.requireNonNull(b.compensation, "compensation of step " + b.name);
        this.timeout = b.timeout;
        this.retryPolicy = b.retryPolicy;
        this.groupId = b.groupId;
    }

    public static Builder of(String name, SagaAction forward, SagaAction compensation) {
        return new Builder(name, forward, compensation);
    }

    public String getName() { return name; }
    public SagaAction getForward() { return forward; }
    public SagaAction getCompensation() { return compensation; }
    public Duration getTimeout() { return timeout; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public String getGroupId() { return groupId; }
    public int getIndex() { return index; }

    void assignIndex(int index) { this.index = index; }
    public StepDefinition inGroup(String groupId) {
        Builder b = new Builder(name, forward, compensation);
        b.timeout = timeout;
        b.retryPolicy = retryPolicy;
        b.groupId = groupId;
        return b.build();
    }

    public static final class Builder {
        private final String name;
        private final SagaAction forward;
        private final SagaAction compensation;
        private Duration timeout;
        private RetryPolicy retryPolicy;
        private String groupId;

        private Builder(String name, SagaAction forward, SagaAction compensation) {
            this.name = name;
            this.forward = forward;
            this.compensation = compensation;
        }

        public Builder timeout(Duration timeout) { this.timeout = timeout; return this; }
        public Builder retry(RetryPolicy retryPolicy) { this.retryPolicy = retryPolicy; return this; }
        public StepDefinition build() { return new StepDefinition(this); }
    }
}
